//! Property-based tests for health status computation.
//!
//! **Property 9: Health status computation is correct for all state combinations**
//!
//! For any combination of upstream, downstream, and MQTT connection states:
//! - IF downstream is Disconnected → Unhealthy
//! - ELSE IF upstream AND downstream are both Connected AND MQTT is Disconnected → Degraded
//! - ELSE IF upstream AND downstream are both Connected → Healthy
//! - ELSE → Unhealthy
//!
//! **Validates: Requirements 10.3, 10.4, 10.5, 10.6**

use proptest::prelude::*;

use ocpp_proxy::models::{ConnectionId, ConnectionState};
use ocpp_proxy::state::{ConnectionStateManager, HealthStatus};

/// All possible connection states.
const ALL_STATES: [ConnectionState; 4] = [
    ConnectionState::Disconnected,
    ConnectionState::Connecting,
    ConnectionState::Connected,
    ConnectionState::Reconnecting,
];

/// Compute the expected health status according to the specification rules.
///
/// Rules (evaluated in order):
/// 1. If downstream is Disconnected → Unhealthy
/// 2. If upstream AND downstream are both Connected, MQTT is Disconnected → Degraded
/// 3. If upstream AND downstream are both Connected → Healthy
/// 4. Otherwise → Unhealthy
fn expected_health(
    upstream: ConnectionState,
    downstream: ConnectionState,
    mqtt: ConnectionState,
) -> HealthStatus {
    if downstream == ConnectionState::Disconnected {
        return HealthStatus::Unhealthy;
    }

    let upstream_connected = upstream == ConnectionState::Connected;
    let downstream_connected = downstream == ConnectionState::Connected;

    if upstream_connected && downstream_connected {
        if mqtt == ConnectionState::Disconnected {
            return HealthStatus::Degraded;
        }
        return HealthStatus::Healthy;
    }

    HealthStatus::Unhealthy
}

/// Helper: create a ConnectionStateManager with the given states applied.
fn make_manager_with_states(
    upstream: ConnectionState,
    downstream: ConnectionState,
    mqtt: ConnectionState,
) -> ConnectionStateManager {
    let mut mgr = ConnectionStateManager::new(16);

    // Transition each connection to the desired state.
    // Since all start at Disconnected, we only need to transition if the target differs.
    if upstream != ConnectionState::Disconnected {
        mgr.transition(ConnectionId::Upstream, upstream);
    }
    if downstream != ConnectionState::Disconnected {
        mgr.transition(ConnectionId::Downstream, downstream);
    }
    if mqtt != ConnectionState::Disconnected {
        mgr.transition(ConnectionId::Mqtt, mqtt);
    }

    mgr
}

/// Exhaustive test: verify health status for ALL 64 combinations of
/// (upstream, downstream, mqtt) × 4 states each.
///
/// **Validates: Requirements 10.3, 10.4, 10.5, 10.6**
#[test]
fn exhaustive_health_status_all_64_combinations() {
    let mut tested = 0;

    for &upstream in &ALL_STATES {
        for &downstream in &ALL_STATES {
            for &mqtt in &ALL_STATES {
                let mgr = make_manager_with_states(upstream, downstream, mqtt);
                let actual = mgr.health_status();
                let expected = expected_health(upstream, downstream, mqtt);

                assert_eq!(
                    actual, expected,
                    "Health status mismatch for upstream={:?}, downstream={:?}, mqtt={:?}: \
                     got {:?}, expected {:?}",
                    upstream, downstream, mqtt, actual, expected
                );

                tested += 1;
            }
        }
    }

    assert_eq!(tested, 64, "Expected to test all 64 combinations");
}

/// Proptest strategy to generate a random ConnectionState.
fn arb_connection_state() -> impl Strategy<Value = ConnectionState> {
    prop_oneof![
        Just(ConnectionState::Disconnected),
        Just(ConnectionState::Connecting),
        Just(ConnectionState::Connected),
        Just(ConnectionState::Reconnecting),
    ]
}

proptest! {
    /// Property 9: Health status computation is correct for all state combinations.
    ///
    /// Randomly generates (upstream, downstream, mqtt) state triples and verifies
    /// the computed health status matches the specification rules.
    ///
    /// **Validates: Requirements 10.3, 10.4, 10.5, 10.6**
    #[test]
    fn prop_health_status_matches_specification(
        upstream in arb_connection_state(),
        downstream in arb_connection_state(),
        mqtt in arb_connection_state(),
    ) {
        let mgr = make_manager_with_states(upstream, downstream, mqtt);
        let actual = mgr.health_status();
        let expected = expected_health(upstream, downstream, mqtt);

        prop_assert_eq!(
            actual, expected,
            "Health status mismatch for upstream={:?}, downstream={:?}, mqtt={:?}",
            upstream, downstream, mqtt
        );
    }

    /// Property 9 (supplementary): Downstream disconnected always yields Unhealthy,
    /// regardless of upstream and MQTT states.
    ///
    /// **Validates: Requirements 10.3**
    #[test]
    fn prop_downstream_disconnected_always_unhealthy(
        upstream in arb_connection_state(),
        mqtt in arb_connection_state(),
    ) {
        let mgr = make_manager_with_states(upstream, ConnectionState::Disconnected, mqtt);
        let actual = mgr.health_status();

        prop_assert_eq!(
            actual,
            HealthStatus::Unhealthy,
            "Expected Unhealthy when downstream is Disconnected, \
             got {:?} with upstream={:?}, mqtt={:?}",
            actual, upstream, mqtt
        );
    }

    /// Property 9 (supplementary): When upstream and downstream are both Connected,
    /// health is Degraded if MQTT is Disconnected, Healthy otherwise.
    ///
    /// **Validates: Requirements 10.4, 10.5**
    #[test]
    fn prop_both_connected_health_depends_on_mqtt(
        mqtt in arb_connection_state(),
    ) {
        let mgr = make_manager_with_states(
            ConnectionState::Connected,
            ConnectionState::Connected,
            mqtt,
        );
        let actual = mgr.health_status();

        let expected = if mqtt == ConnectionState::Disconnected {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        prop_assert_eq!(
            actual, expected,
            "With upstream+downstream Connected and mqtt={:?}: got {:?}, expected {:?}",
            mqtt, actual, expected
        );
    }
}
