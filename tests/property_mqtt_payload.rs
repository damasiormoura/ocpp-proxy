//! Property-based tests for MQTT payload structure.
//!
//! **Property 6: MQTT payload contains all required fields with correct types**
//!
//! For any OCPP message forwarded by the proxy, the published MQTT JSON payload
//! SHALL contain: a `timestamp` field that is a valid ISO 8601 string, a
//! `message_type` field that is one of "Call", "CallResult", or "CallError",
//! and a `payload` field containing the full original OCPP message JSON array.
//!
//! **Validates: Requirements 5.3**

use chrono::{DateTime, Utc};
use ocpp_proxy::models::OcppMessageType;
use ocpp_proxy::mqtt::{message_type_str, MqttPayload};
use proptest::prelude::*;
use serde_json::Value;

// --- Strategies for generating valid OCPP messages ---

/// Generate a random unique ID string.
fn arb_unique_id() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-zA-Z0-9][a-zA-Z0-9_\\-]{0,35}")
        .unwrap()
        .prop_filter("non-empty unique_id", |s| !s.is_empty())
}

/// Generate a random OCPP action name (PascalCase-like).
fn arb_action() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Z][a-zA-Z0-9]{2,30}").unwrap()
}

/// Generate a random JSON value for payload fields.
fn arb_json_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        prop::string::string_regex("[a-zA-Z0-9 _\\-]{0,20}")
            .unwrap()
            .prop_map(|s| Value::String(s)),
        (0i64..=9999i64).prop_map(|n| Value::Number(n.into())),
        prop::bool::ANY.prop_map(|b| Value::Bool(b)),
        Just(Value::Null),
    ]
}

/// Generate a random JSON object.
fn arb_json_object() -> impl Strategy<Value = Value> {
    prop::collection::vec(
        (
            prop::string::string_regex("[a-zA-Z][a-zA-Z0-9]{0,10}").unwrap(),
            arb_json_value(),
        ),
        0..5,
    )
    .prop_map(|pairs| {
        let mut map = serde_json::Map::new();
        for (key, val) in pairs {
            map.insert(key, val);
        }
        Value::Object(map)
    })
}

/// Generate a valid OCPP Call message as a JSON array value.
/// Format: [2, UniqueId, Action, Payload]
fn arb_call_message() -> impl Strategy<Value = (Value, OcppMessageType)> {
    (arb_unique_id(), arb_action(), arb_json_object()).prop_map(|(uid, action, payload)| {
        let arr = Value::Array(vec![
            Value::Number(2.into()),
            Value::String(uid),
            Value::String(action.clone()),
            payload,
        ]);
        let msg_type = OcppMessageType::Call { action };
        (arr, msg_type)
    })
}

/// Generate a valid OCPP CallResult message as a JSON array value.
/// Format: [3, UniqueId, Payload]
fn arb_call_result_message() -> impl Strategy<Value = (Value, OcppMessageType)> {
    (arb_unique_id(), arb_json_object()).prop_map(|(uid, payload)| {
        let arr = Value::Array(vec![
            Value::Number(3.into()),
            Value::String(uid),
            payload,
        ]);
        (arr, OcppMessageType::CallResult)
    })
}

/// Generate a valid OCPP CallError message as a JSON array value.
/// Format: [4, UniqueId, ErrorCode, ErrorDescription, ErrorDetails]
fn arb_call_error_message() -> impl Strategy<Value = (Value, OcppMessageType)> {
    (
        arb_unique_id(),
        prop::string::string_regex("[A-Z][a-zA-Z]{3,20}").unwrap(),
        prop::string::string_regex("[a-zA-Z0-9 ]{0,30}").unwrap(),
        arb_json_object(),
    )
        .prop_map(|(uid, error_code, description, details)| {
            let arr = Value::Array(vec![
                Value::Number(4.into()),
                Value::String(uid),
                Value::String(error_code),
                Value::String(description),
                details,
            ]);
            (arr, OcppMessageType::CallError)
        })
}

/// Generate any valid OCPP message (Call, CallResult, or CallError).
fn arb_ocpp_message() -> impl Strategy<Value = (Value, OcppMessageType)> {
    prop_oneof![
        arb_call_message(),
        arb_call_result_message(),
        arb_call_error_message(),
    ]
}

/// Generate a valid ISO 8601 timestamp string using chrono.
fn arb_timestamp() -> impl Strategy<Value = String> {
    // Generate a timestamp within a reasonable range
    (0i64..=2_000_000_000i64).prop_map(|secs| {
        let dt = DateTime::from_timestamp(secs, 0).unwrap_or_else(|| Utc::now());
        dt.to_rfc3339()
    })
}

// --- Property Tests ---

proptest! {
    /// **Property 6: MQTT payload JSON has exactly 3 fields: timestamp, message_type, payload**
    ///
    /// Validates: Requirements 5.3
    ///
    /// For any OCPP message, the serialized MqttPayload must contain exactly
    /// the 3 required fields: timestamp, message_type, and payload.
    #[test]
    fn mqtt_payload_has_exactly_three_fields(
        (ocpp_message, msg_type) in arb_ocpp_message(),
        timestamp in arb_timestamp(),
    ) {
        let mqtt_payload = MqttPayload {
            timestamp: timestamp.clone(),
            message_type: message_type_str(&msg_type).to_string(),
            payload: ocpp_message,
        };

        let serialized = serde_json::to_string(&mqtt_payload).unwrap();
        let parsed: Value = serde_json::from_str(&serialized).unwrap();

        let obj = parsed.as_object().unwrap();
        prop_assert_eq!(
            obj.len(),
            3,
            "MqttPayload JSON must have exactly 3 fields, got {}: {:?}",
            obj.len(),
            obj.keys().collect::<Vec<_>>()
        );
        prop_assert!(obj.contains_key("timestamp"), "Missing 'timestamp' field");
        prop_assert!(obj.contains_key("message_type"), "Missing 'message_type' field");
        prop_assert!(obj.contains_key("payload"), "Missing 'payload' field");
    }

    /// **Property 6: timestamp field is a valid ISO 8601 string**
    ///
    /// Validates: Requirements 5.3
    ///
    /// For any MqttPayload, the timestamp field must be parseable as a valid
    /// ISO 8601 date-time by chrono.
    #[test]
    fn mqtt_payload_timestamp_is_valid_iso8601(
        (ocpp_message, msg_type) in arb_ocpp_message(),
        timestamp in arb_timestamp(),
    ) {
        let mqtt_payload = MqttPayload {
            timestamp: timestamp.clone(),
            message_type: message_type_str(&msg_type).to_string(),
            payload: ocpp_message,
        };

        let serialized = serde_json::to_string(&mqtt_payload).unwrap();
        let parsed: Value = serde_json::from_str(&serialized).unwrap();

        let ts_value = parsed.get("timestamp").unwrap().as_str().unwrap();

        // Must parse as valid ISO 8601 / RFC 3339
        let parse_result = DateTime::parse_from_rfc3339(ts_value);
        prop_assert!(
            parse_result.is_ok(),
            "timestamp '{}' is not a valid ISO 8601 string: {:?}",
            ts_value,
            parse_result.err()
        );
    }

    /// **Property 6: message_type field is one of "Call", "CallResult", "CallError"**
    ///
    /// Validates: Requirements 5.3
    ///
    /// For any MqttPayload, the message_type field must be one of the three
    /// valid OCPP message type strings.
    #[test]
    fn mqtt_payload_message_type_is_valid(
        (ocpp_message, msg_type) in arb_ocpp_message(),
        timestamp in arb_timestamp(),
    ) {
        let mqtt_payload = MqttPayload {
            timestamp: timestamp.clone(),
            message_type: message_type_str(&msg_type).to_string(),
            payload: ocpp_message,
        };

        let serialized = serde_json::to_string(&mqtt_payload).unwrap();
        let parsed: Value = serde_json::from_str(&serialized).unwrap();

        let msg_type_value = parsed.get("message_type").unwrap().as_str().unwrap();

        let valid_types = ["Call", "CallResult", "CallError"];
        prop_assert!(
            valid_types.contains(&msg_type_value),
            "message_type '{}' is not one of {:?}",
            msg_type_value,
            valid_types
        );
    }

    /// **Property 6: payload field is a JSON array (the full original OCPP message)**
    ///
    /// Validates: Requirements 5.3
    ///
    /// For any MqttPayload constructed from an OCPP message, the payload field
    /// must be a JSON array representing the full original OCPP message.
    #[test]
    fn mqtt_payload_contains_json_array(
        (ocpp_message, msg_type) in arb_ocpp_message(),
        timestamp in arb_timestamp(),
    ) {
        let mqtt_payload = MqttPayload {
            timestamp: timestamp.clone(),
            message_type: message_type_str(&msg_type).to_string(),
            payload: ocpp_message.clone(),
        };

        let serialized = serde_json::to_string(&mqtt_payload).unwrap();
        let parsed: Value = serde_json::from_str(&serialized).unwrap();

        let payload_value = parsed.get("payload").unwrap();
        prop_assert!(
            payload_value.is_array(),
            "payload must be a JSON array, got: {:?}",
            payload_value
        );

        // The payload must match the original OCPP message
        prop_assert_eq!(
            payload_value,
            &ocpp_message,
            "payload must equal the original OCPP message"
        );
    }

    /// **Property 6: message_type corresponds correctly to the OCPP message type ID**
    ///
    /// Validates: Requirements 5.3
    ///
    /// For each OCPP message type, the message_type field in the serialized payload
    /// must match the type ID in the OCPP message array:
    /// - Type 2 → "Call"
    /// - Type 3 → "CallResult"
    /// - Type 4 → "CallError"
    #[test]
    fn mqtt_payload_message_type_matches_ocpp_type_id(
        (ocpp_message, msg_type) in arb_ocpp_message(),
        timestamp in arb_timestamp(),
    ) {
        let mqtt_payload = MqttPayload {
            timestamp: timestamp.clone(),
            message_type: message_type_str(&msg_type).to_string(),
            payload: ocpp_message.clone(),
        };

        let serialized = serde_json::to_string(&mqtt_payload).unwrap();
        let parsed: Value = serde_json::from_str(&serialized).unwrap();

        let msg_type_str_val = parsed.get("message_type").unwrap().as_str().unwrap();
        let payload_arr = parsed.get("payload").unwrap().as_array().unwrap();
        let type_id = payload_arr[0].as_u64().unwrap();

        let expected_type = match type_id {
            2 => "Call",
            3 => "CallResult",
            4 => "CallError",
            _ => panic!("Unexpected type ID: {}", type_id),
        };

        prop_assert_eq!(
            msg_type_str_val,
            expected_type,
            "message_type '{}' does not match OCPP type ID {} (expected '{}')",
            msg_type_str_val,
            type_id,
            expected_type
        );
    }
}
