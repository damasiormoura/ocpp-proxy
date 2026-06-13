//! Property-based tests for connection status message accuracy.
//!
//! **Property 8: Connection status message reflects actual states**
//!
//! For any combination of upstream connection state and downstream connection state,
//! the published status JSON on `ocpp/{Charge_Point_ID}/status` SHALL accurately
//! represent both connection states.
//!
//! **Validates: Requirements 5.6**

use proptest::prelude::*;

use ocpp_proxy::models::ConnectionState;
use ocpp_proxy::mqtt::{connection_state_str, StatusPayload};

/// All possible connection states (4 states: Connected, Disconnected, Reconnecting, Connecting).
const ALL_STATES: [ConnectionState; 4] = [
    ConnectionState::Connected,
    ConnectionState::Disconnected,
    ConnectionState::Reconnecting,
    ConnectionState::Connecting,
];

/// Exhaustive test: verify status payload for ALL 16 combinations of
/// (upstream, downstream) × 4 states each.
///
/// For each combination:
/// 1. Create a `StatusPayload` with the mapped state strings using `connection_state_str()`
/// 2. Serialize to JSON
/// 3. Deserialize and verify the "upstream" and "downstream" fields match the input states
///
/// **Validates: Requirements 5.6**
#[test]
fn exhaustive_status_message_all_16_combinations() {
    let mut tested = 0;

    for &upstream in &ALL_STATES {
        for &downstream in &ALL_STATES {
            let upstream_str = connection_state_str(upstream).to_string();
            let downstream_str = connection_state_str(downstream).to_string();

            // 1. Create StatusPayload with mapped state strings
            let payload = StatusPayload {
                upstream: upstream_str.clone(),
                downstream: downstream_str.clone(),
            };

            // 2. Serialize to JSON
            let json = serde_json::to_string(&payload).expect("StatusPayload should serialize");

            // 3. Deserialize and verify fields match
            let deserialized: StatusPayload =
                serde_json::from_str(&json).expect("StatusPayload should deserialize");

            assert_eq!(
                deserialized.upstream, upstream_str,
                "Upstream mismatch for state {:?}: got '{}', expected '{}'",
                upstream, deserialized.upstream, upstream_str
            );
            assert_eq!(
                deserialized.downstream, downstream_str,
                "Downstream mismatch for state {:?}: got '{}', expected '{}'",
                downstream, deserialized.downstream, downstream_str
            );

            // Also verify via serde_json::Value that the JSON has exactly the expected keys
            let value: serde_json::Value =
                serde_json::from_str(&json).expect("Should parse as Value");
            assert_eq!(
                value.get("upstream").and_then(|v| v.as_str()),
                Some(upstream_str.as_str()),
                "JSON 'upstream' field mismatch for state {:?}",
                upstream
            );
            assert_eq!(
                value.get("downstream").and_then(|v| v.as_str()),
                Some(downstream_str.as_str()),
                "JSON 'downstream' field mismatch for state {:?}",
                downstream
            );

            tested += 1;
        }
    }

    assert_eq!(tested, 16, "Expected to test all 16 combinations");
}

/// Verify that `connection_state_str` maps each state to the correct string
/// as specified in requirement 5.6.
///
/// **Validates: Requirements 5.6**
#[test]
fn connection_state_str_maps_correctly() {
    assert_eq!(connection_state_str(ConnectionState::Connected), "connected");
    assert_eq!(connection_state_str(ConnectionState::Disconnected), "disconnected");
    assert_eq!(connection_state_str(ConnectionState::Reconnecting), "reconnecting");
    assert_eq!(connection_state_str(ConnectionState::Connecting), "connecting");
}

/// Verify that the serialized status payload contains exactly two fields:
/// "upstream" and "downstream", and no extraneous fields.
///
/// **Validates: Requirements 5.6**
#[test]
fn status_payload_contains_only_required_fields() {
    let payload = StatusPayload {
        upstream: "connected".to_string(),
        downstream: "disconnected".to_string(),
    };

    let json = serde_json::to_string(&payload).expect("Should serialize");
    let value: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

    let obj = value.as_object().expect("Should be a JSON object");
    assert_eq!(obj.len(), 2, "Status payload should have exactly 2 fields");
    assert!(obj.contains_key("upstream"), "Missing 'upstream' field");
    assert!(obj.contains_key("downstream"), "Missing 'downstream' field");
}

/// Proptest strategy to generate a random ConnectionState.
fn arb_connection_state() -> impl Strategy<Value = ConnectionState> {
    prop_oneof![
        Just(ConnectionState::Connected),
        Just(ConnectionState::Disconnected),
        Just(ConnectionState::Reconnecting),
        Just(ConnectionState::Connecting),
    ]
}

proptest! {
    /// Property 8: Connection status message reflects actual states.
    ///
    /// Randomly generates (upstream, downstream) state pairs and verifies that
    /// the serialized/deserialized status JSON accurately represents both states.
    ///
    /// **Validates: Requirements 5.6**
    #[test]
    fn prop_status_message_reflects_actual_states(
        upstream in arb_connection_state(),
        downstream in arb_connection_state(),
    ) {
        let upstream_str = connection_state_str(upstream).to_string();
        let downstream_str = connection_state_str(downstream).to_string();

        // Create payload from connection states
        let payload = StatusPayload {
            upstream: upstream_str.clone(),
            downstream: downstream_str.clone(),
        };

        // Serialize to JSON
        let json = serde_json::to_string(&payload)
            .expect("StatusPayload serialization should not fail");

        // Deserialize back
        let deserialized: StatusPayload = serde_json::from_str(&json)
            .expect("StatusPayload deserialization should not fail");

        // Verify roundtrip preserves both fields accurately
        prop_assert_eq!(
            &deserialized.upstream, &upstream_str,
            "Upstream state mismatch after roundtrip: {:?} → '{}' → '{}'",
            upstream, upstream_str, deserialized.upstream
        );
        prop_assert_eq!(
            &deserialized.downstream, &downstream_str,
            "Downstream state mismatch after roundtrip: {:?} → '{}' → '{}'",
            downstream, downstream_str, deserialized.downstream
        );

        // Verify JSON structure via Value
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let obj = value.as_object().unwrap();

        // Must have exactly 2 fields
        prop_assert_eq!(obj.len(), 2, "Status payload must have exactly 2 fields");

        // Fields must be strings matching the state
        prop_assert_eq!(
            obj.get("upstream").and_then(|v| v.as_str()),
            Some(upstream_str.as_str()),
            "JSON upstream field mismatch"
        );
        prop_assert_eq!(
            obj.get("downstream").and_then(|v| v.as_str()),
            Some(downstream_str.as_str()),
            "JSON downstream field mismatch"
        );
    }

    /// Property 8 (supplementary): State string values are always one of the
    /// expected set {"connected", "disconnected", "reconnecting", "connecting"}.
    ///
    /// **Validates: Requirements 5.6**
    #[test]
    fn prop_state_strings_are_valid_values(
        state in arb_connection_state(),
    ) {
        let state_str = connection_state_str(state);
        let valid_values = ["connected", "disconnected", "reconnecting", "connecting"];

        prop_assert!(
            valid_values.contains(&state_str),
            "connection_state_str({:?}) returned '{}' which is not in the valid set",
            state, state_str
        );
    }
}
