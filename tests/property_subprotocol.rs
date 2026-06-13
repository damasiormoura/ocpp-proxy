//! Property-based tests for subprotocol rejection.
//!
//! **Property 14: Non-ocpp1.6 subprotocols are rejected**
//!
//! For any WebSocket subprotocol string that is not exactly `ocpp1.6`,
//! the proxy SHALL reject the connection.
//!
//! **Validates: Requirements 1.6**

use proptest::prelude::*;

use ocpp_proxy::downstream::validate_subprotocol;

/// Strategy to generate random strings that are NOT exactly "ocpp1.6".
///
/// Covers:
/// - Random alphanumeric strings
/// - Strings similar but not exact: case variations, extra chars, different versions
/// - Empty strings
/// - Strings with whitespace
fn non_ocpp16_string() -> impl Strategy<Value = String> {
    prop_oneof![
        // Random alphanumeric strings (filtered to exclude exact match)
        "[a-zA-Z0-9._\\- ]{0,30}".prop_filter("must not be exactly ocpp1.6", |s| s != "ocpp1.6"),
        // Case variations
        Just("OCPP1.6".to_string()),
        Just("Ocpp1.6".to_string()),
        Just("OCPP1.6J".to_string()),
        Just("ocPP1.6".to_string()),
        // Similar but not exact
        Just("ocpp1.6j".to_string()),
        Just("ocpp1.6.1".to_string()),
        Just("ocpp16".to_string()),
        Just("ocpp 1.6".to_string()),
        Just("ocpp1.6 ".to_string()),
        Just(" ocpp1.6".to_string()),
        Just(" ocpp1.6 ".to_string()),
        // Different versions
        Just("ocpp2.0".to_string()),
        Just("ocpp1.5".to_string()),
        Just("ocpp2.1".to_string()),
        Just("ocpp1.6.2".to_string()),
        // Empty and whitespace
        Just("".to_string()),
        Just(" ".to_string()),
        Just("  ".to_string()),
        // Partial matches
        Just("ocpp".to_string()),
        Just("ocpp1".to_string()),
        Just("ocpp1.".to_string()),
        Just("1.6".to_string()),
        Just("ocp1.6".to_string()),
        // Completely unrelated
        Just("websocket".to_string()),
        Just("mqtt".to_string()),
        Just("http".to_string()),
    ]
}

proptest! {
    /// Property 14: Non-ocpp1.6 subprotocols are rejected.
    ///
    /// For any string that is not exactly "ocpp1.6", validate_subprotocol
    /// returns false when that string is the only protocol offered.
    ///
    /// **Validates: Requirements 1.6**
    #[test]
    fn prop_non_ocpp16_subprotocols_are_rejected(
        protocol in non_ocpp16_string()
    ) {
        let protocols = vec![protocol.clone()];
        let result = validate_subprotocol(&protocols);

        prop_assert!(
            !result,
            "Expected validate_subprotocol to reject {:?}, but it returned true",
            protocol
        );
    }

    /// Property 14 (multiple protocols): Non-ocpp1.6 subprotocols are rejected
    /// even when multiple non-matching protocols are offered.
    ///
    /// Generates a list of 1-5 non-ocpp1.6 strings and verifies all are rejected together.
    ///
    /// **Validates: Requirements 1.6**
    #[test]
    fn prop_multiple_non_ocpp16_subprotocols_are_rejected(
        protocols in prop::collection::vec(non_ocpp16_string(), 1..=5)
    ) {
        let result = validate_subprotocol(&protocols);

        prop_assert!(
            !result,
            "Expected validate_subprotocol to reject {:?}, but it returned true",
            protocols
        );
    }

    /// Soundness check: "ocpp1.6" is always accepted.
    ///
    /// For any list of protocols that includes exactly "ocpp1.6",
    /// validate_subprotocol must return true.
    ///
    /// **Validates: Requirements 1.6**
    #[test]
    fn prop_ocpp16_always_accepted(
        extra_protocols in prop::collection::vec("[a-zA-Z0-9.]{1,20}", 0..=4),
        position in 0usize..=4usize,
    ) {
        let mut protocols: Vec<String> = extra_protocols
            .into_iter()
            .filter(|p| p != "ocpp1.6")
            .collect();

        // Insert "ocpp1.6" at a bounded position
        let insert_pos = position.min(protocols.len());
        protocols.insert(insert_pos, "ocpp1.6".to_string());

        let result = validate_subprotocol(&protocols);

        prop_assert!(
            result,
            "Expected validate_subprotocol to accept list containing 'ocpp1.6': {:?}",
            protocols
        );
    }
}
