//! Property-based tests for health status computation.
//!
//! **Property 9: Health status computation is correct for all state combinations**
//!
//! Health describes the proxy's ability to serve, not whether a charger happens
//! to be plugged in:
//!
//! - listener not bound → `Unhealthy` (503)
//! - no charger connected → `Idle` (200)
//! - charger + upstream connected + MQTT connected → `Healthy` (200)
//! - charger + upstream connected + MQTT down → `Degraded` (200)
//! - charger + upstream connecting/reconnecting → `Degraded` (200)
//! - charger + upstream disconnected → `Unhealthy` (503)
//!
//! **Validates: Requirements 10.4, 10.5, 10.6, 10.7, 10.8**
//!
//! The previous version of this file encoded the earlier rule under which
//! "downstream disconnected" was `Unhealthy`/503. Combined with a
//! restart-on-failed-healthcheck supervisor, that made an idle proxy — the
//! normal state when no vehicle is plugged in — restart forever. The
//! `prop_listening_and_idle_is_never_unhealthy` case below exists specifically
//! to stop that regression coming back.

use proptest::prelude::*;

use ocpp_proxy::models::{ConnectionId, ConnectionState};
use ocpp_proxy::state::{ConnectionStateManager, HealthStatus};

const ALL_STATES: [ConnectionState; 4] = [
    ConnectionState::Disconnected,
    ConnectionState::Connecting,
    ConnectionState::Connected,
    ConnectionState::Reconnecting,
];

fn arb_state() -> impl Strategy<Value = ConnectionState> {
    prop::sample::select(ALL_STATES.as_slice())
}

/// Build a manager in a given state. Exercises the production transition path
/// rather than writing fields directly.
fn manager(
    listener_bound: bool,
    upstream: ConnectionState,
    downstream: ConnectionState,
    mqtt: ConnectionState,
) -> ConnectionStateManager {
    let mut mgr = ConnectionStateManager::new(16);
    mgr.set_listener_bound(listener_bound);
    mgr.transition(ConnectionId::Upstream, upstream);
    mgr.transition(ConnectionId::Downstream, downstream);
    mgr.transition(ConnectionId::Mqtt, mqtt);
    mgr
}

/// The specification, restated independently of the implementation.
fn expected_status(
    listener_bound: bool,
    upstream: ConnectionState,
    downstream: ConnectionState,
    mqtt: ConnectionState,
) -> HealthStatus {
    if !listener_bound {
        return HealthStatus::Unhealthy;
    }
    if downstream != ConnectionState::Connected {
        return HealthStatus::Idle;
    }
    match upstream {
        ConnectionState::Connected => {
            if mqtt == ConnectionState::Connected {
                HealthStatus::Healthy
            } else {
                HealthStatus::Degraded
            }
        }
        ConnectionState::Connecting | ConnectionState::Reconnecting => HealthStatus::Degraded,
        ConnectionState::Disconnected => HealthStatus::Unhealthy,
    }
}

/// HTTP status is derived from health status; only `Unhealthy` is 503.
fn expected_http(status: HealthStatus) -> u16 {
    match status {
        HealthStatus::Healthy | HealthStatus::Idle | HealthStatus::Degraded => 200,
        HealthStatus::Unhealthy => 503,
    }
}

#[test]
fn exhaustive_health_status_all_128_combinations() {
    for listener_bound in [false, true] {
        for upstream in ALL_STATES {
            for downstream in ALL_STATES {
                for mqtt in ALL_STATES {
                    let mgr = manager(listener_bound, upstream, downstream, mqtt);
                    assert_eq!(
                        mgr.health_status(),
                        expected_status(listener_bound, upstream, downstream, mqtt),
                        "bound={} upstream={:?} downstream={:?} mqtt={:?}",
                        listener_bound,
                        upstream,
                        downstream,
                        mqtt
                    );
                }
            }
        }
    }
}

/// The regression guard the design calls for by name: while the listener is
/// bound and no charger is connected, the proxy is never unhealthy — whatever
/// upstream and MQTT are doing.
#[test]
fn listening_and_idle_is_never_unhealthy() {
    for upstream in ALL_STATES {
        for mqtt in ALL_STATES {
            for downstream in [
                ConnectionState::Disconnected,
                ConnectionState::Connecting,
                ConnectionState::Reconnecting,
            ] {
                let mgr = manager(true, upstream, downstream, mqtt);
                assert_eq!(
                    mgr.health_status(),
                    HealthStatus::Idle,
                    "no charger connected must be idle, not a fault \
                     (upstream={:?} downstream={:?} mqtt={:?})",
                    upstream,
                    downstream,
                    mqtt
                );
                assert_eq!(expected_http(mgr.health_status()), 200);
            }
        }
    }
}

proptest! {
    #[test]
    fn prop_health_status_matches_specification(
        listener_bound in proptest::bool::ANY,
        upstream in arb_state(),
        downstream in arb_state(),
        mqtt in arb_state(),
    ) {
        let mgr = manager(listener_bound, upstream, downstream, mqtt);
        prop_assert_eq!(
            mgr.health_status(),
            expected_status(listener_bound, upstream, downstream, mqtt)
        );
    }

    /// While serving, 503 has exactly one cause: a connected charger whose
    /// upstream is down. Anything else returning 503 would get the proxy
    /// restarted for a condition it can recover from on its own.
    #[test]
    fn prop_only_dead_upstream_with_a_charger_is_unhealthy(
        upstream in arb_state(),
        downstream in arb_state(),
        mqtt in arb_state(),
    ) {
        let mgr = manager(true, upstream, downstream, mqtt);
        let is_unhealthy = mgr.health_status() == HealthStatus::Unhealthy;
        let should_be = downstream == ConnectionState::Connected
            && upstream == ConnectionState::Disconnected;
        prop_assert_eq!(
            is_unhealthy,
            should_be,
            "upstream={:?} downstream={:?} mqtt={:?} -> {:?}",
            upstream, downstream, mqtt, mgr.health_status()
        );
    }

    /// An unbound listener is unhealthy no matter what else is true: the proxy
    /// cannot serve at all.
    #[test]
    fn prop_unbound_listener_is_always_unhealthy(
        upstream in arb_state(),
        downstream in arb_state(),
        mqtt in arb_state(),
    ) {
        let mgr = manager(false, upstream, downstream, mqtt);
        prop_assert_eq!(mgr.health_status(), HealthStatus::Unhealthy);
    }

    /// MQTT never affects the HTTP status while OCPP forwarding works. Losing
    /// Home Assistant visibility must not look like a failing proxy.
    #[test]
    fn prop_mqtt_never_causes_503_while_forwarding_works(
        mqtt in arb_state(),
    ) {
        let mgr = manager(true, ConnectionState::Connected, ConnectionState::Connected, mqtt);
        prop_assert_eq!(expected_http(mgr.health_status()), 200);
    }

    /// Both connected: MQTT alone decides healthy vs degraded.
    #[test]
    fn prop_both_connected_health_depends_on_mqtt(mqtt in arb_state()) {
        let mgr = manager(true, ConnectionState::Connected, ConnectionState::Connected, mqtt);
        let expected = if mqtt == ConnectionState::Connected {
            HealthStatus::Healthy
        } else {
            HealthStatus::Degraded
        };
        prop_assert_eq!(mgr.health_status(), expected);
    }
}
