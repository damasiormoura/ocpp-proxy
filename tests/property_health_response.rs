//! Property-based tests for health response structure.
//!
//! **Property 16: Health response contains all required fields**
//!
//! For any health check request, the response JSON SHALL contain:
//! - `status` (healthy/degraded/unhealthy)
//! - `upstream` (connection state)
//! - `downstream` (connection state)
//! - `mqtt` (connection state)
//! - `uptime_seconds` (non-negative integer)
//! - `messages` (object with four non-negative integer counter fields)
//!
//! **Validates: Requirements 10.2**

use proptest::prelude::*;

use ocpp_proxy::health::{HealthResponse, MessageCounters};
use ocpp_proxy::models::ConnectionState;
use ocpp_proxy::state::HealthStatus;

/// Valid string representations of HealthStatus when serialized.
const VALID_STATUSES: [&str; 3] = ["healthy", "degraded", "unhealthy"];

/// Valid string representations of ConnectionState when serialized.
const VALID_CONNECTION_STATES: [&str; 4] = [
    "disconnected",
    "connecting",
    "connected",
    "reconnecting",
];

/// Proptest strategy to generate a random HealthStatus.
fn arb_health_status() -> impl Strategy<Value = HealthStatus> {
    prop_oneof![
        Just(HealthStatus::Healthy),
        Just(HealthStatus::Degraded),
        Just(HealthStatus::Unhealthy),
    ]
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

/// Proptest strategy to generate random MessageCounters.
fn arb_message_counters() -> impl Strategy<Value = MessageCounters> {
    (0u64..1_000_000, 0u64..1_000_000, 0u64..1_000_000, 0u64..1_000_000).prop_map(
        |(ctc_fwd, ctc_drop, ctch_fwd, ctch_drop)| MessageCounters {
            charger_to_central_forwarded: ctc_fwd,
            charger_to_central_dropped: ctc_drop,
            central_to_charger_forwarded: ctch_fwd,
            central_to_charger_dropped: ctch_drop,
        },
    )
}

/// Proptest strategy to generate a random HealthResponse.
fn arb_health_response() -> impl Strategy<Value = HealthResponse> {
    (
        arb_health_status(),
        arb_connection_state(),
        arb_connection_state(),
        arb_connection_state(),
        0u64..1_000_000,
        arb_message_counters(),
    )
        .prop_map(
            |(status, upstream, downstream, mqtt, uptime_seconds, messages)| HealthResponse {
                status,
                upstream,
                downstream,
                mqtt,
                uptime_seconds,
                messages,
            },
        )
}

proptest! {
    /// Property 16: Health response contains all required fields with correct types.
    ///
    /// Constructs a HealthResponse with random state combinations and counter values,
    /// serializes to JSON, then verifies all required fields are present with correct types.
    ///
    /// **Validates: Requirements 10.2**
    #[test]
    fn prop_health_response_has_all_required_fields(response in arb_health_response()) {
        let json_value = serde_json::to_value(&response)
            .expect("HealthResponse should serialize to JSON");

        // 1. JSON has "status" field (string, one of "healthy"/"degraded"/"unhealthy")
        let status = json_value.get("status")
            .expect("JSON must have 'status' field");
        let status_str = status.as_str()
            .expect("'status' field must be a string");
        prop_assert!(
            VALID_STATUSES.contains(&status_str),
            "status '{}' is not one of {:?}",
            status_str, VALID_STATUSES
        );

        // 2. JSON has "upstream" field (string, one of connection states)
        let upstream = json_value.get("upstream")
            .expect("JSON must have 'upstream' field");
        let upstream_str = upstream.as_str()
            .expect("'upstream' field must be a string");
        prop_assert!(
            VALID_CONNECTION_STATES.contains(&upstream_str),
            "upstream '{}' is not one of {:?}",
            upstream_str, VALID_CONNECTION_STATES
        );

        // 3. JSON has "downstream" field (string, one of connection states)
        let downstream = json_value.get("downstream")
            .expect("JSON must have 'downstream' field");
        let downstream_str = downstream.as_str()
            .expect("'downstream' field must be a string");
        prop_assert!(
            VALID_CONNECTION_STATES.contains(&downstream_str),
            "downstream '{}' is not one of {:?}",
            downstream_str, VALID_CONNECTION_STATES
        );

        // 4. JSON has "mqtt" field (string, one of connection states)
        let mqtt = json_value.get("mqtt")
            .expect("JSON must have 'mqtt' field");
        let mqtt_str = mqtt.as_str()
            .expect("'mqtt' field must be a string");
        prop_assert!(
            VALID_CONNECTION_STATES.contains(&mqtt_str),
            "mqtt '{}' is not one of {:?}",
            mqtt_str, VALID_CONNECTION_STATES
        );

        // 5. JSON has "uptime_seconds" field (non-negative integer)
        let uptime = json_value.get("uptime_seconds")
            .expect("JSON must have 'uptime_seconds' field");
        prop_assert!(
            uptime.is_u64(),
            "'uptime_seconds' must be a non-negative integer, got {:?}",
            uptime
        );

        // 6. JSON has "messages" object with 4 non-negative integer fields
        let messages = json_value.get("messages")
            .expect("JSON must have 'messages' field");
        prop_assert!(
            messages.is_object(),
            "'messages' must be an object, got {:?}",
            messages
        );

        let msg_obj = messages.as_object().unwrap();

        // Verify all 4 counter fields exist and are non-negative integers
        let counter_fields = [
            "charger_to_central_forwarded",
            "charger_to_central_dropped",
            "central_to_charger_forwarded",
            "central_to_charger_dropped",
        ];

        for field in &counter_fields {
            let value = msg_obj.get(*field)
                .unwrap_or_else(|| panic!("'messages' must have '{}' field", field));
            prop_assert!(
                value.is_u64(),
                "'messages.{}' must be a non-negative integer, got {:?}",
                field, value
            );
        }
    }

    /// Property 16 (supplementary): Serialized status value matches the input HealthStatus.
    ///
    /// **Validates: Requirements 10.2**
    #[test]
    fn prop_health_response_status_matches_input(response in arb_health_response()) {
        let json_value = serde_json::to_value(&response)
            .expect("HealthResponse should serialize to JSON");

        let expected_status = match response.status {
            HealthStatus::Healthy => "healthy",
            HealthStatus::Degraded => "degraded",
            HealthStatus::Unhealthy => "unhealthy",
        };

        let actual_status = json_value["status"].as_str().unwrap();
        prop_assert_eq!(actual_status, expected_status);
    }

    /// Property 16 (supplementary): Message counter values in JSON match the input values.
    ///
    /// **Validates: Requirements 10.2**
    #[test]
    fn prop_health_response_counters_match_input(response in arb_health_response()) {
        let json_value = serde_json::to_value(&response)
            .expect("HealthResponse should serialize to JSON");

        let messages = &json_value["messages"];

        prop_assert_eq!(
            messages["charger_to_central_forwarded"].as_u64().unwrap(),
            response.messages.charger_to_central_forwarded
        );
        prop_assert_eq!(
            messages["charger_to_central_dropped"].as_u64().unwrap(),
            response.messages.charger_to_central_dropped
        );
        prop_assert_eq!(
            messages["central_to_charger_forwarded"].as_u64().unwrap(),
            response.messages.central_to_charger_forwarded
        );
        prop_assert_eq!(
            messages["central_to_charger_dropped"].as_u64().unwrap(),
            response.messages.central_to_charger_dropped
        );
    }
}
