//! Property-based tests for MQTT topic construction.
//!
//! **Property 5: MQTT topic construction follows the format specification**
//!
//! For any valid Charge_Point_ID, for any direction (charger or central_system),
//! and for any OCPP action name, the constructed MQTT topic SHALL equal
//! `ocpp/{Charge_Point_ID}/{direction}/{action}` with no additional path segments,
//! leading/trailing slashes, or character transformations.
//!
//! **Validates: Requirements 5.2**

use proptest::prelude::*;

use ocpp_proxy::mqtt::{availability_topic, message_topic, status_topic};

/// Strategy to generate valid Charge Point IDs.
///
/// Valid IDs are alphanumeric with hyphens and underscores, 1-30 characters.
fn valid_charge_point_id() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_\\-]{1,30}"
}

/// Strategy to generate valid MQTT direction segments.
///
/// Per the spec, direction is either "charger" or "central_system".
fn valid_direction() -> impl Strategy<Value = String> {
    prop_oneof![Just("charger".to_string()), Just("central_system".to_string()),]
}

/// Strategy to generate valid OCPP action names.
///
/// Action names follow PascalCase convention, 3-30 characters.
fn valid_action_name() -> impl Strategy<Value = String> {
    "[A-Z][a-zA-Z]{2,29}"
}

proptest! {
    /// Property 5: message_topic produces the exact expected format.
    ///
    /// The topic must equal `ocpp/{charge_point_id}/{direction}/{action}` exactly.
    ///
    /// **Validates: Requirements 5.2**
    #[test]
    fn prop_message_topic_matches_format(
        charge_point_id in valid_charge_point_id(),
        direction in valid_direction(),
        action in valid_action_name(),
    ) {
        let topic = message_topic(&charge_point_id, &direction, &action);
        let expected = format!("ocpp/{}/{}/{}", charge_point_id, direction, action);

        prop_assert_eq!(
            &topic,
            &expected,
            "message_topic did not match expected format"
        );
    }

    /// Property 5: message_topic contains exactly 4 segments separated by '/'.
    ///
    /// The topic format `ocpp/{id}/{direction}/{action}` has exactly 4 segments.
    ///
    /// **Validates: Requirements 5.2**
    #[test]
    fn prop_message_topic_has_exactly_four_segments(
        charge_point_id in valid_charge_point_id(),
        direction in valid_direction(),
        action in valid_action_name(),
    ) {
        let topic = message_topic(&charge_point_id, &direction, &action);
        let segments: Vec<&str> = topic.split('/').collect();

        prop_assert_eq!(
            segments.len(),
            4,
            "Expected exactly 4 topic segments, got {}: {:?}",
            segments.len(),
            segments
        );
        prop_assert_eq!(segments[0], "ocpp");
        prop_assert_eq!(segments[1], charge_point_id.as_str());
        prop_assert_eq!(segments[2], direction.as_str());
        prop_assert_eq!(segments[3], action.as_str());
    }

    /// Property 5: message_topic contains no double slashes.
    ///
    /// **Validates: Requirements 5.2**
    #[test]
    fn prop_message_topic_no_double_slashes(
        charge_point_id in valid_charge_point_id(),
        direction in valid_direction(),
        action in valid_action_name(),
    ) {
        let topic = message_topic(&charge_point_id, &direction, &action);

        prop_assert!(
            !topic.contains("//"),
            "Topic '{}' contains double slashes",
            topic
        );
    }

    /// Property 5: message_topic has no leading or trailing slashes.
    ///
    /// **Validates: Requirements 5.2**
    #[test]
    fn prop_message_topic_no_leading_trailing_slashes(
        charge_point_id in valid_charge_point_id(),
        direction in valid_direction(),
        action in valid_action_name(),
    ) {
        let topic = message_topic(&charge_point_id, &direction, &action);

        prop_assert!(
            !topic.starts_with('/'),
            "Topic '{}' has a leading slash",
            topic
        );
        prop_assert!(
            !topic.ends_with('/'),
            "Topic '{}' has a trailing slash",
            topic
        );
    }

    /// Property 5: availability_topic follows the format `ocpp/{id}/availability`.
    ///
    /// **Validates: Requirements 5.2**
    #[test]
    fn prop_availability_topic_matches_format(
        charge_point_id in valid_charge_point_id(),
    ) {
        let topic = availability_topic(&charge_point_id);
        let expected = format!("ocpp/{}/availability", charge_point_id);

        prop_assert_eq!(
            &topic,
            &expected,
            "availability_topic did not match expected format"
        );
    }

    /// Property 5: availability_topic contains exactly 3 segments.
    ///
    /// **Validates: Requirements 5.2**
    #[test]
    fn prop_availability_topic_has_three_segments(
        charge_point_id in valid_charge_point_id(),
    ) {
        let topic = availability_topic(&charge_point_id);
        let segments: Vec<&str> = topic.split('/').collect();

        prop_assert_eq!(
            segments.len(),
            3,
            "Expected exactly 3 segments, got {}: {:?}",
            segments.len(),
            segments
        );
        prop_assert_eq!(segments[0], "ocpp");
        prop_assert_eq!(segments[1], charge_point_id.as_str());
        prop_assert_eq!(segments[2], "availability");
    }

    /// Property 5: status_topic follows the format `ocpp/{id}/status`.
    ///
    /// **Validates: Requirements 5.2**
    #[test]
    fn prop_status_topic_matches_format(
        charge_point_id in valid_charge_point_id(),
    ) {
        let topic = status_topic(&charge_point_id);
        let expected = format!("ocpp/{}/status", charge_point_id);

        prop_assert_eq!(
            &topic,
            &expected,
            "status_topic did not match expected format"
        );
    }

    /// Property 5: status_topic contains exactly 3 segments.
    ///
    /// **Validates: Requirements 5.2**
    #[test]
    fn prop_status_topic_has_three_segments(
        charge_point_id in valid_charge_point_id(),
    ) {
        let topic = status_topic(&charge_point_id);
        let segments: Vec<&str> = topic.split('/').collect();

        prop_assert_eq!(
            segments.len(),
            3,
            "Expected exactly 3 segments, got {}: {:?}",
            segments.len(),
            segments
        );
        prop_assert_eq!(segments[0], "ocpp");
        prop_assert_eq!(segments[1], charge_point_id.as_str());
        prop_assert_eq!(segments[2], "status");
    }

    /// Property 5: No topic helper produces double slashes or leading/trailing slashes.
    ///
    /// **Validates: Requirements 5.2**
    #[test]
    fn prop_all_topics_no_double_or_boundary_slashes(
        charge_point_id in valid_charge_point_id(),
        direction in valid_direction(),
        action in valid_action_name(),
    ) {
        let topics = vec![
            message_topic(&charge_point_id, &direction, &action),
            availability_topic(&charge_point_id),
            status_topic(&charge_point_id),
        ];

        for topic in &topics {
            prop_assert!(
                !topic.contains("//"),
                "Topic '{}' contains double slashes",
                topic
            );
            prop_assert!(
                !topic.starts_with('/'),
                "Topic '{}' has a leading slash",
                topic
            );
            prop_assert!(
                !topic.ends_with('/'),
                "Topic '{}' has a trailing slash",
                topic
            );
        }
    }
}
