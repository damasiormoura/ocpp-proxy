//! Property-based tests for OCPP frame parsing.
//!
//! **Property 1: Message forwarding preserves payload byte-for-byte**
//!
//! For any valid OCPP JSON message (Call, CallResult, or CallError), the parsed
//! OcppFrame's `raw` field must be byte-for-byte identical to the input string.
//! The parser must not modify, reformat, or re-serialize the JSON.
//!
//! **Validates: Requirements 3.1, 3.2, 3.3**

use ocpp_proxy::models::{OcppFrame, OcppMessageType};
use proptest::prelude::*;
use serde_json::{json, Value};

// --- Strategies for generating valid JSON values ---

/// Generate a random unique ID string (alphanumeric with hyphens/underscores).
fn arb_unique_id() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-zA-Z0-9][a-zA-Z0-9_\\-]{0,35}")
        .unwrap()
        .prop_filter("non-empty unique_id", |s| !s.is_empty())
}

/// Generate a random OCPP action name (PascalCase-like).
fn arb_action() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Z][a-zA-Z0-9]{2,30}").unwrap()
}

/// Generate random whitespace to insert between JSON structural elements.
fn arb_whitespace() -> impl Strategy<Value = String> {
    prop::string::string_regex("[ \t\n\r]{0,4}").unwrap()
}

/// Generate a random string value that may include unicode.
fn arb_string_value() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            // ASCII alphanumeric
            prop::char::range('a', 'z').prop_map(|c| c.to_string()),
            prop::char::range('A', 'Z').prop_map(|c| c.to_string()),
            prop::char::range('0', '9').prop_map(|c| c.to_string()),
            // Common punctuation (safe in JSON strings)
            Just(" ".to_string()),
            Just("_".to_string()),
            Just("-".to_string()),
            Just(".".to_string()),
            Just("!".to_string()),
            // Unicode characters
            Just("\u{00e9}".to_string()),  // é
            Just("\u{00f1}".to_string()),  // ñ
            Just("\u{4e16}".to_string()),  // 世
            Just("\u{754c}".to_string()),  // 界
            Just("\u{2603}".to_string()),  // ☃
        ],
        0..15,
    )
    .prop_map(|chars| chars.join(""))
}

/// Generate a random serde_json::Value for use in payloads.
fn arb_json_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        arb_string_value().prop_map(|s| json!(s)),
        (0i64..=9999i64).prop_map(|n| json!(n)),
        prop::bool::ANY.prop_map(|b| json!(b)),
        Just(json!(null)),
    ]
}

/// Generate a random JSON object as serde_json::Value (with possible nesting).
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

/// Generate a nested JSON object (object values can themselves be objects).
fn arb_nested_json_object() -> impl Strategy<Value = Value> {
    prop::collection::vec(
        (
            prop::string::string_regex("[a-zA-Z][a-zA-Z0-9]{0,10}").unwrap(),
            prop_oneof![
                arb_json_value(),
                arb_json_object(),
            ],
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

/// Serialize a JSON value and inject varied whitespace around structural characters.
/// This produces valid JSON with non-standard formatting.
fn inject_whitespace(json_str: &str, ws_before: &str, ws_after: &str) -> String {
    let mut result = String::with_capacity(json_str.len() * 2);
    let mut in_string = false;
    let mut escape_next = false;

    for ch in json_str.chars() {
        if escape_next {
            result.push(ch);
            escape_next = false;
            continue;
        }
        if ch == '\\' && in_string {
            result.push(ch);
            escape_next = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            result.push(ch);
            continue;
        }
        if !in_string {
            match ch {
                '{' | '[' => {
                    result.push(ch);
                    result.push_str(ws_after);
                }
                '}' | ']' => {
                    result.push_str(ws_before);
                    result.push(ch);
                }
                ',' => {
                    result.push_str(ws_before);
                    result.push(ch);
                    result.push_str(ws_after);
                }
                ':' => {
                    result.push_str(ws_before);
                    result.push(ch);
                    result.push_str(ws_after);
                }
                _ => {
                    result.push(ch);
                }
            }
        } else {
            result.push(ch);
        }
    }
    result
}

/// Generate a valid OCPP Call frame string: [2, "UniqueId", "Action", {payload}]
/// with varied whitespace between elements.
fn arb_call_frame() -> impl Strategy<Value = String> {
    (
        arb_unique_id(),
        arb_action(),
        arb_nested_json_object(),
        arb_whitespace(),
        arb_whitespace(),
    )
        .prop_map(|(uid, action, payload, ws1, ws2)| {
            let payload_str = serde_json::to_string(&payload).unwrap();
            let payload_formatted = inject_whitespace(&payload_str, &ws1, &ws2);
            format!(
                "[{}2{},{}\"{}\"{},{}\"{}\"{},{}{}{}]",
                ws1, ws2, ws1, uid, ws2, ws1, action, ws2, ws1, payload_formatted, ws2
            )
        })
}

/// Generate a valid OCPP CallResult frame string: [3, "UniqueId", {payload}]
/// with varied whitespace.
fn arb_call_result_frame() -> impl Strategy<Value = String> {
    (
        arb_unique_id(),
        arb_nested_json_object(),
        arb_whitespace(),
        arb_whitespace(),
    )
        .prop_map(|(uid, payload, ws1, ws2)| {
            let payload_str = serde_json::to_string(&payload).unwrap();
            let payload_formatted = inject_whitespace(&payload_str, &ws1, &ws2);
            format!(
                "[{}3{},{}\"{}\"{},{}{}{}]",
                ws1, ws2, ws1, uid, ws2, ws1, payload_formatted, ws2
            )
        })
}

/// Generate a valid OCPP CallError frame string:
/// [4, "UniqueId", "ErrorCode", "ErrorDescription", {details}]
/// with varied whitespace.
fn arb_call_error_frame() -> impl Strategy<Value = String> {
    (
        arb_unique_id(),
        prop::string::string_regex("[A-Z][a-zA-Z]{3,20}").unwrap(),
        arb_string_value(),
        arb_nested_json_object(),
        arb_whitespace(),
        arb_whitespace(),
    )
        .prop_map(|(uid, error_code, description, details, ws1, ws2)| {
            let details_str = serde_json::to_string(&details).unwrap();
            let details_formatted = inject_whitespace(&details_str, &ws1, &ws2);
            // Escape the description for safe JSON embedding
            let desc_escaped = description
                .replace('\\', "\\\\")
                .replace('"', "\\\"");
            format!(
                "[{}4{},{}\"{}\"{},{}\"{}\"{},{}\"{}\"{},{}{}{}]",
                ws1, ws2, ws1, uid, ws2, ws1, error_code, ws2, ws1, desc_escaped, ws2, ws1,
                details_formatted, ws2
            )
        })
}

/// Generate any valid OCPP frame (Call, CallResult, or CallError).
fn arb_ocpp_frame() -> impl Strategy<Value = String> {
    prop_oneof![
        arb_call_frame(),
        arb_call_result_frame(),
        arb_call_error_frame(),
    ]
}

// --- Property Tests ---

proptest! {
    /// **Property 1: Message forwarding preserves payload byte-for-byte**
    ///
    /// Validates: Requirements 3.1, 3.2, 3.3
    ///
    /// For any valid OCPP frame, parsing it and reading `frame.raw` must return
    /// the exact same bytes as the original input string.
    #[test]
    fn raw_field_preserves_input_byte_for_byte(input in arb_ocpp_frame()) {
        let frame = OcppFrame::parse(&input)
            .unwrap_or_else(|e| panic!(
                "Generated OCPP frame should parse successfully.\nInput: {:?}\nError: {:?}",
                input, e
            ));

        // The key invariant: raw must be byte-for-byte identical to input
        prop_assert_eq!(
            frame.raw.as_bytes(),
            input.as_bytes(),
            "frame.raw must be byte-for-byte identical to the input string"
        );
    }

    /// **Property 1 (supplemental): Call message type and unique_id are correctly extracted**
    ///
    /// Validates: Requirements 3.1, 3.2, 3.3
    #[test]
    fn call_frame_extracts_correct_message_type(
        uid in arb_unique_id(),
        action in arb_action(),
        payload in arb_nested_json_object(),
    ) {
        let payload_str = serde_json::to_string(&payload).unwrap();
        let input = format!("[2, \"{}\", \"{}\", {}]", uid, action, payload_str);
        let frame = OcppFrame::parse(&input)
            .unwrap_or_else(|e| panic!(
                "Call frame should parse successfully.\nInput: {:?}\nError: {:?}",
                input, e
            ));

        prop_assert_eq!(&frame.raw, &input);
        prop_assert_eq!(&frame.unique_id, &uid);
        match &frame.message_type {
            OcppMessageType::Call { action: parsed_action } => {
                prop_assert_eq!(parsed_action, &action);
            }
            other => {
                prop_assert!(false, "Expected Call, got {:?}", other);
            }
        }
    }

    /// **Property 1 (supplemental): CallResult message type is correctly identified**
    ///
    /// Validates: Requirements 3.1, 3.2, 3.3
    #[test]
    fn call_result_frame_extracts_correct_type(
        uid in arb_unique_id(),
        payload in arb_nested_json_object(),
    ) {
        let payload_str = serde_json::to_string(&payload).unwrap();
        let input = format!("[3, \"{}\", {}]", uid, payload_str);
        let frame = OcppFrame::parse(&input)
            .unwrap_or_else(|e| panic!(
                "CallResult frame should parse successfully.\nInput: {:?}\nError: {:?}",
                input, e
            ));

        prop_assert_eq!(&frame.raw, &input);
        prop_assert_eq!(&frame.unique_id, &uid);
        prop_assert_eq!(frame.message_type, OcppMessageType::CallResult);
    }

    /// **Property 1 (supplemental): CallError message type is correctly identified**
    ///
    /// Validates: Requirements 3.1, 3.2, 3.3
    #[test]
    fn call_error_frame_extracts_correct_type(
        uid in arb_unique_id(),
        error_code in prop::string::string_regex("[A-Z][a-zA-Z]{3,20}").unwrap(),
        description in arb_string_value(),
        details in arb_nested_json_object(),
    ) {
        let details_str = serde_json::to_string(&details).unwrap();
        let desc_escaped = description.replace('\\', "\\\\").replace('"', "\\\"");
        let input = format!(
            "[4, \"{}\", \"{}\", \"{}\", {}]",
            uid, error_code, desc_escaped, details_str
        );
        let frame = OcppFrame::parse(&input)
            .unwrap_or_else(|e| panic!(
                "CallError frame should parse successfully.\nInput: {:?}\nError: {:?}",
                input, e
            ));

        prop_assert_eq!(&frame.raw, &input);
        prop_assert_eq!(&frame.unique_id, &uid);
        prop_assert_eq!(frame.message_type, OcppMessageType::CallError);
    }

    /// **Property 1 (supplemental): Varied whitespace is preserved exactly**
    ///
    /// Validates: Requirements 3.1, 3.2, 3.3
    ///
    /// The parser must not normalize or collapse whitespace in the raw JSON.
    #[test]
    fn whitespace_is_preserved_in_raw(input in arb_ocpp_frame()) {
        let frame = OcppFrame::parse(&input)
            .unwrap_or_else(|e| panic!(
                "Generated OCPP frame should parse successfully.\nInput: {:?}\nError: {:?}",
                input, e
            ));

        // Verify exact string equality (which implies whitespace preservation)
        prop_assert_eq!(
            &frame.raw,
            &input,
            "Whitespace in JSON must be preserved exactly"
        );
    }
}
