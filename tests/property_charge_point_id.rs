//! Property-based tests for Charge Point ID preservation in upstream connection URL.
//!
//! **Property 17: Charge Point ID is preserved in upstream connection URL**
//!
//! For any valid Charge_Point_ID received in the downstream connection URL path,
//! the upstream connection SHALL use a URL containing the same Charge_Point_ID in its path.
//!
//! **Validates: Requirements 2.2**

use proptest::prelude::*;
use url::Url;

use ocpp_proxy::upstream::UpstreamHandler;

/// Strategy to generate valid Charge Point IDs.
///
/// Valid IDs are alphanumeric with hyphens and underscores, 1-50 characters.
fn valid_charge_point_id() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_\\-]{1,50}"
}

/// Strategy to generate valid base Central System URLs.
///
/// Covers:
/// - wss://hostname
/// - wss://hostname/path
/// - wss://hostname/multi/path
/// - ws://hostname:port
/// - ws://hostname:port/path
fn valid_central_system_url() -> impl Strategy<Value = Url> {
    prop_oneof![
        // wss://hostname (no path)
        "[a-z]{3,10}\\.[a-z]{2,6}".prop_map(|host| {
            Url::parse(&format!("wss://{}", host)).unwrap()
        }),
        // wss://hostname/path
        ("[a-z]{3,10}\\.[a-z]{2,6}", "[a-z]{2,10}").prop_map(|(host, path)| {
            Url::parse(&format!("wss://{}/{}", host, path)).unwrap()
        }),
        // wss://hostname/multi/path
        ("[a-z]{3,10}\\.[a-z]{2,6}", "[a-z]{2,8}", "[a-z]{2,8}").prop_map(
            |(host, p1, p2)| { Url::parse(&format!("wss://{}/{}/{}", host, p1, p2)).unwrap() }
        ),
        // ws://hostname:port
        ("[a-z]{3,10}\\.[a-z]{2,6}", 1024u16..65535u16).prop_map(|(host, port)| {
            Url::parse(&format!("ws://{}:{}", host, port)).unwrap()
        }),
        // ws://hostname:port/path
        ("[a-z]{3,10}\\.[a-z]{2,6}", 1024u16..65535u16, "[a-z]{2,10}").prop_map(
            |(host, port, path)| {
                Url::parse(&format!("ws://{}:{}/{}", host, port, path)).unwrap()
            }
        ),
        // wss with trailing slash
        "[a-z]{3,10}\\.[a-z]{2,6}".prop_map(|host| {
            Url::parse(&format!("wss://{}/", host)).unwrap()
        }),
    ]
}

proptest! {
    /// Property 17: Charge Point ID is preserved in upstream connection URL.
    ///
    /// For any valid Charge_Point_ID and any valid base Central System URL,
    /// the built connection URL path ends with the exact charge_point_id.
    ///
    /// **Validates: Requirements 2.2**
    #[test]
    fn prop_charge_point_id_preserved_in_url_path(
        base_url in valid_central_system_url(),
        charge_point_id in valid_charge_point_id(),
    ) {
        let handler = UpstreamHandler::new(
            base_url.clone(),
            charge_point_id.clone(),
            "ocpp1.6".to_string(),
        );

        let connection_url = handler.build_connection_url();
        let url_path = connection_url.path();

        // The URL path must end with the exact charge_point_id
        prop_assert!(
            url_path.ends_with(&charge_point_id),
            "Expected URL path '{}' to end with charge_point_id '{}' (base: {})",
            url_path,
            charge_point_id,
            base_url,
        );
    }

    /// Property 17: Charge Point ID appears as-is (no encoding for valid chars).
    ///
    /// Valid Charge Point IDs (alphanumeric, hyphens, underscores) should not
    /// be percent-encoded in the resulting URL.
    ///
    /// **Validates: Requirements 2.2**
    #[test]
    fn prop_charge_point_id_not_encoded_in_url(
        base_url in valid_central_system_url(),
        charge_point_id in valid_charge_point_id(),
    ) {
        let handler = UpstreamHandler::new(
            base_url.clone(),
            charge_point_id.clone(),
            "ocpp1.6".to_string(),
        );

        let connection_url = handler.build_connection_url();
        let url_str = connection_url.as_str();

        // The charge_point_id must appear literally in the URL (no percent-encoding)
        prop_assert!(
            url_str.contains(&charge_point_id),
            "Expected URL '{}' to contain charge_point_id '{}' as-is (no encoding), base: {}",
            url_str,
            charge_point_id,
            base_url,
        );
    }

    /// Property 17: The charge_point_id is the last path segment.
    ///
    /// The ID should be appended as the final segment of the URL path,
    /// separated from the base path by a '/'.
    ///
    /// **Validates: Requirements 2.2**
    #[test]
    fn prop_charge_point_id_is_last_path_segment(
        base_url in valid_central_system_url(),
        charge_point_id in valid_charge_point_id(),
    ) {
        let handler = UpstreamHandler::new(
            base_url.clone(),
            charge_point_id.clone(),
            "ocpp1.6".to_string(),
        );

        let connection_url = handler.build_connection_url();
        let url_path = connection_url.path();

        // Split path into segments and verify the last one matches
        let segments: Vec<&str> = url_path.split('/').filter(|s| !s.is_empty()).collect();

        prop_assert!(
            !segments.is_empty(),
            "URL path '{}' has no segments",
            url_path,
        );

        let last_segment = segments.last().unwrap();
        prop_assert_eq!(
            *last_segment,
            charge_point_id.as_str(),
            "Last path segment '{}' does not match charge_point_id '{}' (full path: {}, base: {})",
            last_segment,
            charge_point_id,
            url_path,
            base_url,
        );
    }
}
