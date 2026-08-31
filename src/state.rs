//! Connection state manager.
//!
//! Tracks connection lifecycle states and coordinates state transitions across components.
//! Provides health status computation and state change notifications via broadcast channels.

use chrono::Utc;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::models::{ConnectionId, ConnectionState, StateChange};

/// Overall health status of the proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    /// Charger connected, upstream established, MQTT connected. HTTP 200.
    Healthy,
    /// Listening, but no charger connected. HTTP 200.
    ///
    /// This is the normal state whenever no vehicle is plugged in and is
    /// explicitly *not* a fault. An earlier revision reported it as
    /// `Unhealthy`/503, which combined with a restart-on-failed-healthcheck
    /// supervisor would have restarted the proxy forever whenever nobody was
    /// charging.
    Idle,
    /// Forwarding impaired but recoverable: upstream reconnecting inside its
    /// window, or MQTT down. HTTP 200.
    Degraded,
    /// Not listening, or a charger is connected and upstream has failed past
    /// its reconnection window. HTTP 503.
    Unhealthy,
}

/// Message counters for forwarded and dropped messages per direction.
#[derive(Debug, Clone, Default)]
pub struct ConnectionMetrics {
    pub charger_to_central_forwarded: u64,
    pub charger_to_central_dropped: u64,
    pub central_to_charger_forwarded: u64,
    pub central_to_charger_dropped: u64,
}

/// Manages connection states across upstream, downstream, and MQTT connections.
///
/// Broadcasts state change events to subscribers and computes overall health status.
pub struct ConnectionStateManager {
    upstream_state: ConnectionState,
    downstream_state: ConnectionState,
    mqtt_state: ConnectionState,
    /// Whether the charger-facing listener is bound and accepting.
    ///
    /// Distinguishes "idle, waiting for a charger" from "cannot serve at all",
    /// which the connection states alone cannot express.
    listener_bound: bool,
    state_tx: broadcast::Sender<StateChange>,
    metrics: ConnectionMetrics,
}

impl ConnectionStateManager {
    /// Create a new `ConnectionStateManager`.
    ///
    /// All connections start in the `Disconnected` state. The broadcast channel
    /// is created with the specified capacity for state change notifications.
    pub fn new(broadcast_capacity: usize) -> Self {
        let (state_tx, _) = broadcast::channel(broadcast_capacity);
        Self {
            upstream_state: ConnectionState::Disconnected,
            downstream_state: ConnectionState::Disconnected,
            mqtt_state: ConnectionState::Disconnected,
            listener_bound: false,
            state_tx,
            metrics: ConnectionMetrics::default(),
        }
    }

    /// Update a connection's state and notify subscribers.
    ///
    /// If the new state differs from the current state, a `StateChange` event
    /// is broadcast to all active subscribers. If the state hasn't changed,
    /// no event is emitted.
    pub fn transition(&mut self, conn: ConnectionId, new_state: ConnectionState) {
        let previous = match conn {
            ConnectionId::Upstream => &mut self.upstream_state,
            ConnectionId::Downstream => &mut self.downstream_state,
            ConnectionId::Mqtt => &mut self.mqtt_state,
        };

        let old_state = *previous;
        if old_state == new_state {
            return;
        }

        *previous = new_state;

        let event = StateChange {
            connection: conn,
            previous: old_state,
            current: new_state,
            timestamp: Utc::now(),
        };

        // Ignore send errors — they occur when there are no active receivers.
        let _ = self.state_tx.send(event);
    }

    /// Mark the charger-facing listener as bound or unbound.
    pub fn set_listener_bound(&mut self, bound: bool) {
        self.listener_bound = bound;
    }

    /// Whether the charger-facing listener is bound.
    pub fn listener_bound(&self) -> bool {
        self.listener_bound
    }

    /// Compute the overall proxy health status.
    ///
    /// Health describes the proxy's ability to serve, not whether a charger
    /// happens to be plugged in. Rules, in order:
    ///
    /// 1. Listener not bound → `Unhealthy` (503) — we cannot serve at all
    /// 2. No charger connected → `Idle` (200) — the normal resting state
    /// 3. Charger connected, upstream up, MQTT up → `Healthy` (200)
    /// 4. Charger connected, upstream up, MQTT down → `Degraded` (200) — MQTT
    ///    loss costs Home Assistant visibility but never costs charging
    /// 5. Charger connected, upstream reconnecting → `Degraded` (200)
    /// 6. Charger connected, upstream down → `Unhealthy` (503)
    pub fn health_status(&self) -> HealthStatus {
        if !self.listener_bound {
            return HealthStatus::Unhealthy;
        }

        if self.downstream_state != ConnectionState::Connected {
            return HealthStatus::Idle;
        }

        match self.upstream_state {
            ConnectionState::Connected => {
                if self.mqtt_state == ConnectionState::Connected {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Degraded
                }
            }
            ConnectionState::Connecting | ConnectionState::Reconnecting => HealthStatus::Degraded,
            ConnectionState::Disconnected => HealthStatus::Unhealthy,
        }
    }

    /// Subscribe to state change notifications.
    ///
    /// Returns a receiver that will receive all future `StateChange` events.
    pub fn subscribe(&self) -> broadcast::Receiver<StateChange> {
        self.state_tx.subscribe()
    }

    /// Get a reference to the current connection metrics.
    pub fn metrics(&self) -> &ConnectionMetrics {
        &self.metrics
    }

    /// Get a mutable reference to the connection metrics.
    pub fn metrics_mut(&mut self) -> &mut ConnectionMetrics {
        &mut self.metrics
    }

    /// Record a successfully forwarded message.
    pub fn record_forwarded(&mut self, direction: crate::models::Direction) {
        match direction {
            crate::models::Direction::ChargerToCentral => {
                self.metrics.charger_to_central_forwarded += 1
            }
            crate::models::Direction::CentralToCharger => {
                self.metrics.central_to_charger_forwarded += 1
            }
        }
    }

    /// Record a dropped message.
    pub fn record_dropped(&mut self, direction: crate::models::Direction, count: u64) {
        match direction {
            crate::models::Direction::ChargerToCentral => {
                self.metrics.charger_to_central_dropped += count
            }
            crate::models::Direction::CentralToCharger => {
                self.metrics.central_to_charger_dropped += count
            }
        }
    }

    /// Get the current upstream connection state.
    pub fn upstream_state(&self) -> ConnectionState {
        self.upstream_state
    }

    /// Get the current downstream connection state.
    pub fn downstream_state(&self) -> ConnectionState {
        self.downstream_state
    }

    /// Get the current MQTT connection state.
    pub fn mqtt_state(&self) -> ConnectionState {
        self.mqtt_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager() -> ConnectionStateManager {
        ConnectionStateManager::new(16)
    }

    // --- Health status tests covering all key combinations ---

    /// Health describes the proxy's ability to serve, not whether a charger
    /// happens to be plugged in. These cases pin that distinction down.

    #[test]
    fn test_listener_unbound_is_unhealthy() {
        let mgr = make_manager();
        assert!(!mgr.listener_bound());
        assert_eq!(mgr.health_status(), HealthStatus::Unhealthy);
    }

    /// The regression this revision exists to prevent.
    ///
    /// "Listening, no charger connected" is the normal resting state whenever
    /// no vehicle is plugged in. Reporting it as unhealthy — as the previous
    /// implementation did — makes any restart-on-failed-healthcheck supervisor
    /// restart the proxy forever whenever nobody is charging.
    #[test]
    fn test_listening_with_no_charger_is_idle_not_unhealthy() {
        let mut mgr = make_manager();
        mgr.set_listener_bound(true);
        assert_eq!(mgr.health_status(), HealthStatus::Idle);
        assert_ne!(mgr.health_status(), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_idle_regardless_of_upstream_and_mqtt_when_no_charger() {
        for upstream in [
            ConnectionState::Disconnected,
            ConnectionState::Connecting,
            ConnectionState::Connected,
            ConnectionState::Reconnecting,
        ] {
            for mqtt in [ConnectionState::Disconnected, ConnectionState::Connected] {
                let mut mgr = make_manager();
                mgr.set_listener_bound(true);
                mgr.transition(ConnectionId::Upstream, upstream);
                mgr.transition(ConnectionId::Mqtt, mqtt);
                assert_eq!(
                    mgr.health_status(),
                    HealthStatus::Idle,
                    "no charger connected must be idle (upstream={:?}, mqtt={:?})",
                    upstream,
                    mqtt
                );
            }
        }
    }

    #[test]
    fn test_charger_and_upstream_connected_with_mqtt_is_healthy() {
        let mut mgr = make_manager();
        mgr.set_listener_bound(true);
        mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
        assert_eq!(mgr.health_status(), HealthStatus::Healthy);
    }

    /// MQTT loss costs Home Assistant visibility, never charging. Degraded,
    /// and deliberately still HTTP 200.
    #[test]
    fn test_mqtt_not_connected_is_degraded() {
        for mqtt in [
            ConnectionState::Disconnected,
            ConnectionState::Connecting,
            ConnectionState::Reconnecting,
        ] {
            let mut mgr = make_manager();
            mgr.set_listener_bound(true);
            mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
            mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
            mgr.transition(ConnectionId::Mqtt, mqtt);
            assert_eq!(
                mgr.health_status(),
                HealthStatus::Degraded,
                "mqtt={:?} should be degraded",
                mqtt
            );
        }
    }

    #[test]
    fn test_upstream_reconnecting_with_charger_is_degraded() {
        for upstream in [ConnectionState::Connecting, ConnectionState::Reconnecting] {
            let mut mgr = make_manager();
            mgr.set_listener_bound(true);
            mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
            mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
            mgr.transition(ConnectionId::Upstream, upstream);
            assert_eq!(mgr.health_status(), HealthStatus::Degraded);
        }
    }

    /// A charger that is connected but cannot reach Mobi.e is the one runtime
    /// state that genuinely warrants 503: charging and billing are broken.
    #[test]
    fn test_charger_connected_upstream_down_is_unhealthy() {
        let mut mgr = make_manager();
        mgr.set_listener_bound(true);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Upstream, ConnectionState::Disconnected);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
        assert_eq!(mgr.health_status(), HealthStatus::Unhealthy);
    }

    /// Exhaustive: while listening, the ONLY route to 503 is a connected
    /// charger with a dead upstream.
    #[test]
    fn test_no_unexpected_unhealthy_while_listening() {
        let states = [
            ConnectionState::Disconnected,
            ConnectionState::Connecting,
            ConnectionState::Connected,
            ConnectionState::Reconnecting,
        ];
        for up in states {
            for down in states {
                for mqtt in states {
                    let mut mgr = make_manager();
                    mgr.set_listener_bound(true);
                    mgr.transition(ConnectionId::Upstream, up);
                    mgr.transition(ConnectionId::Downstream, down);
                    mgr.transition(ConnectionId::Mqtt, mqtt);

                    let expected_unhealthy =
                        down == ConnectionState::Connected && up == ConnectionState::Disconnected;
                    assert_eq!(
                        mgr.health_status() == HealthStatus::Unhealthy,
                        expected_unhealthy,
                        "unexpected health for up={:?} down={:?} mqtt={:?} -> {:?}",
                        up,
                        down,
                        mqtt,
                        mgr.health_status()
                    );
                }
            }
        }
    }

    // --- Transition and broadcast tests ---

    #[test]
    fn test_transition_updates_state() {
        let mut mgr = make_manager();
        assert_eq!(mgr.upstream_state(), ConnectionState::Disconnected);

        mgr.transition(ConnectionId::Upstream, ConnectionState::Connecting);
        assert_eq!(mgr.upstream_state(), ConnectionState::Connecting);

        mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
        assert_eq!(mgr.upstream_state(), ConnectionState::Connected);
    }

    #[test]
    fn test_transition_no_op_when_same_state() {
        let mut mgr = make_manager();
        let mut rx = mgr.subscribe();

        // Transition to same state (Disconnected → Disconnected)
        mgr.transition(ConnectionId::Upstream, ConnectionState::Disconnected);

        // No event should be broadcast
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_transition_broadcasts_event() {
        let mut mgr = make_manager();
        let mut rx = mgr.subscribe();

        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);

        let event = rx.try_recv().expect("should receive state change event");
        assert_eq!(event.connection, ConnectionId::Downstream);
        assert_eq!(event.previous, ConnectionState::Disconnected);
        assert_eq!(event.current, ConnectionState::Connected);
    }

    #[test]
    fn test_multiple_subscribers_receive_events() {
        let mut mgr = make_manager();
        let mut rx1 = mgr.subscribe();
        let mut rx2 = mgr.subscribe();

        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connecting);

        let event1 = rx1.try_recv().expect("subscriber 1 should receive event");
        let event2 = rx2.try_recv().expect("subscriber 2 should receive event");

        assert_eq!(event1.connection, ConnectionId::Mqtt);
        assert_eq!(event2.connection, ConnectionId::Mqtt);
    }

    // --- Metrics tests ---

    #[test]
    fn test_metrics_default_zero() {
        let mgr = make_manager();
        let metrics = mgr.metrics();
        assert_eq!(metrics.charger_to_central_forwarded, 0);
        assert_eq!(metrics.charger_to_central_dropped, 0);
        assert_eq!(metrics.central_to_charger_forwarded, 0);
        assert_eq!(metrics.central_to_charger_dropped, 0);
    }

    #[test]
    fn test_metrics_can_be_incremented() {
        let mut mgr = make_manager();
        mgr.metrics_mut().charger_to_central_forwarded += 5;
        mgr.metrics_mut().central_to_charger_dropped += 2;

        assert_eq!(mgr.metrics().charger_to_central_forwarded, 5);
        assert_eq!(mgr.metrics().central_to_charger_dropped, 2);
    }

    // --- State accessor tests ---

    #[test]
    fn test_state_accessors_reflect_transitions() {
        let mut mgr = make_manager();

        mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Reconnecting);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connecting);

        assert_eq!(mgr.upstream_state(), ConnectionState::Connected);
        assert_eq!(mgr.downstream_state(), ConnectionState::Reconnecting);
        assert_eq!(mgr.mqtt_state(), ConnectionState::Connecting);
    }
}
