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
    Healthy,
    Degraded,
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

    /// Compute the overall proxy health status.
    ///
    /// Health rules (evaluated in order):
    /// 1. If downstream is disconnected → Unhealthy
    /// 2. If upstream AND downstream are connected, MQTT is disconnected → Degraded
    /// 3. If upstream AND downstream are connected → Healthy
    /// 4. Otherwise → Unhealthy
    pub fn health_status(&self) -> HealthStatus {
        if self.downstream_state == ConnectionState::Disconnected {
            return HealthStatus::Unhealthy;
        }

        let upstream_connected = self.upstream_state == ConnectionState::Connected;
        let downstream_connected = self.downstream_state == ConnectionState::Connected;

        if upstream_connected && downstream_connected {
            if self.mqtt_state == ConnectionState::Disconnected {
                return HealthStatus::Degraded;
            }
            return HealthStatus::Healthy;
        }

        HealthStatus::Unhealthy
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

    #[test]
    fn test_health_all_disconnected_is_unhealthy() {
        let mgr = make_manager();
        assert_eq!(mgr.health_status(), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_downstream_disconnected_always_unhealthy() {
        let mut mgr = make_manager();
        // Upstream connected, MQTT connected, but downstream disconnected
        mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
        assert_eq!(mgr.health_status(), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_upstream_and_downstream_connected_mqtt_connected_is_healthy() {
        let mut mgr = make_manager();
        mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
        assert_eq!(mgr.health_status(), HealthStatus::Healthy);
    }

    #[test]
    fn test_health_upstream_and_downstream_connected_mqtt_disconnected_is_degraded() {
        let mut mgr = make_manager();
        mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
        // MQTT stays Disconnected (default)
        assert_eq!(mgr.health_status(), HealthStatus::Degraded);
    }

    #[test]
    fn test_health_upstream_and_downstream_connected_mqtt_reconnecting_is_healthy() {
        let mut mgr = make_manager();
        mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Reconnecting);
        assert_eq!(mgr.health_status(), HealthStatus::Healthy);
    }

    #[test]
    fn test_health_upstream_and_downstream_connected_mqtt_connecting_is_healthy() {
        let mut mgr = make_manager();
        mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connecting);
        assert_eq!(mgr.health_status(), HealthStatus::Healthy);
    }

    #[test]
    fn test_health_upstream_disconnected_downstream_connected_is_unhealthy() {
        let mut mgr = make_manager();
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
        // Upstream stays Disconnected (default)
        assert_eq!(mgr.health_status(), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_upstream_reconnecting_downstream_connected_is_unhealthy() {
        let mut mgr = make_manager();
        mgr.transition(ConnectionId::Upstream, ConnectionState::Reconnecting);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
        assert_eq!(mgr.health_status(), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_upstream_connecting_downstream_connected_is_unhealthy() {
        let mut mgr = make_manager();
        mgr.transition(ConnectionId::Upstream, ConnectionState::Connecting);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
        assert_eq!(mgr.health_status(), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_downstream_reconnecting_is_unhealthy() {
        let mut mgr = make_manager();
        mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Reconnecting);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
        // Downstream is not Disconnected, but it's also not Connected.
        // The health rules: downstream disconnected → unhealthy (not this case);
        // upstream AND downstream connected → healthy/degraded (downstream not connected here).
        // Else → unhealthy.
        assert_eq!(mgr.health_status(), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_downstream_connecting_is_unhealthy() {
        let mut mgr = make_manager();
        mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connecting);
        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
        assert_eq!(mgr.health_status(), HealthStatus::Unhealthy);
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
