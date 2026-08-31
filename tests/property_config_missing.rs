//! Property-based tests for missing parameter reporting.
//!
//! **Property 12: All missing required parameters are reported together**
//!
//! For any non-empty subset of required configuration parameters that are absent,
//! the startup error output SHALL name EVERY missing parameter from that subset,
//! in a single error.
//!
//! This previously asserted only that the error mentioned *at least one* of the
//! missing fields, because `try_deserialize` stops at the first absent field.
//! That weakened the property until it no longer tested Requirement 7.3 — an
//! operator missing four settings had to restart four times to find them all.
//! The production loader now collects them through an all-optional shadow
//! struct, so the property can be asserted as specified.
//!
//! **Validates: Requirements 7.2, 7.3**

use proptest::prelude::*;
use std::io::Write;
use std::sync::Mutex;
use tempfile::NamedTempFile;

use ocpp_proxy::config::ProxyConfig;

/// Serialises access to the process environment.
///
/// `std::env::set_var` is process-global while cargo runs tests in the same
/// binary concurrently, so without this the two properties below clobber each
/// other's configuration and one reads the other's file.
static ENV_GUARD: Mutex<()> = Mutex::new(());

/// Load a config from an explicit path with no `OCPP_PROXY_*` variables set.
///
/// Uses `load_from_path` rather than `CONFIG_FILE_PATH` so the file choice is
/// not global state that another test can overwrite mid-run.
fn load_isolated(path: &str) -> Result<ProxyConfig, ocpp_proxy::error::ProxyError> {
    let _guard = ENV_GUARD.lock().unwrap_or_else(|e| e.into_inner());

    let saved: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| k.starts_with("OCPP_PROXY"))
        .collect();
    for (k, _) in &saved {
        std::env::remove_var(k);
    }

    let result = ProxyConfig::load_from_path(path);

    for (k, v) in saved {
        std::env::set_var(k, v);
    }
    result
}

/// All required YAML field names and their sample valid values.
/// These are the fields that MUST be present for ProxyConfig::load() to succeed.
/// TLS certificate paths are deliberately NOT here. They are optional: the
/// broker is one LAN hop away on the Home Assistant VM, not across the
/// internet, so plaintext with username and password is a valid configuration.
const REQUIRED_FIELDS: &[(&str, &str)] = &[
    ("central_system_url", "\"wss://example.com/ocpp\""),
    ("listen_port", "9000"),
    ("mqtt.host", "\"mqtt.example.com\""),
    ("mqtt.port", "8883"),
    ("mqtt.username", "\"user\""),
    ("mqtt.password", "\"pass\""),
];

/// Build a YAML string containing all required fields EXCEPT those at the given indices.
fn build_yaml_without(omit_indices: &[usize]) -> String {
    let mut top_level_fields: Vec<String> = Vec::new();
    let mut mqtt_fields: Vec<String> = Vec::new();

    for (i, &(name, value)) in REQUIRED_FIELDS.iter().enumerate() {
        if omit_indices.contains(&i) {
            continue;
        }

        if let Some(mqtt_field) = name.strip_prefix("mqtt.") {
            mqtt_fields.push(format!("  {}: {}", mqtt_field, value));
        } else {
            top_level_fields.push(format!("{}: {}", name, value));
        }
    }

    let mut yaml = top_level_fields.join("\n");
    if !mqtt_fields.is_empty() {
        yaml.push_str("\nmqtt:\n");
        yaml.push_str(&mqtt_fields.join("\n"));
    }
    yaml.push('\n');
    yaml
}

/// Strategy to generate a non-empty subset of indices into REQUIRED_FIELDS.
/// Uses a boolean vector where at least one element is true.
fn omit_subset_strategy() -> impl Strategy<Value = Vec<usize>> {
    // Generate a boolean for each required field (whether to omit it)
    proptest::collection::vec(proptest::bool::ANY, REQUIRED_FIELDS.len())
        .prop_filter("at least one field must be omitted", |bools| {
            bools.iter().any(|&b| b)
        })
        .prop_map(|bools| {
            bools
                .iter()
                .enumerate()
                .filter_map(|(i, &omit)| if omit { Some(i) } else { None })
                .collect::<Vec<usize>>()
        })
}

proptest! {
    /// Property 12: All missing required parameters are reported together.
    ///
    /// Generates random non-empty subsets of the required parameters to omit,
    /// and asserts the single startup error names **every** one of them.
    ///
    /// **Validates: Requirements 7.2, 7.3**
    #[test]
    fn prop_all_missing_required_params_are_reported_together(
        omit_indices in omit_subset_strategy()
    ) {
        let yaml = build_yaml_without(&omit_indices);

        let mut config_file = NamedTempFile::new().unwrap();
        write!(config_file.as_file_mut(), "{}", yaml).unwrap();
        let config_path = config_file.path().to_str().unwrap().to_string();

        let result = load_isolated(&config_path);

        let omitted: Vec<&str> = omit_indices.iter().map(|&i| REQUIRED_FIELDS[i].0).collect();

        prop_assert!(
            result.is_err(),
            "load() should fail with {:?} omitted. YAML:\n{}",
            omitted,
            yaml
        );

        let err = result.unwrap_err();
        prop_assert_eq!(err.category(), "config");

        let desc = err.description().to_string();

        // The property proper: every omitted parameter is named, in one error.
        for name in &omitted {
            prop_assert!(
                desc.contains(name),
                "error must name the missing parameter '{}'. Omitted: {:?}\nError:\n{}",
                name,
                omitted,
                desc
            );
        }
    }

    /// The converse: a complete configuration is not reported as missing
    /// anything. Guards against an over-eager collector that flags fields
    /// which are present.
    #[test]
    fn prop_complete_config_reports_no_missing_params(seed in 0u8..16) {
        let _ = seed;
        let yaml = build_yaml_without(&[]);

        let mut config_file = NamedTempFile::new().unwrap();
        write!(config_file.as_file_mut(), "{}", yaml).unwrap();
        let config_path = config_file.path().to_str().unwrap().to_string();

        let result = load_isolated(&config_path);

        // It may still fail validation — the sample URL and ports are fine but
        // nothing guarantees more — yet it must never report a MISSING field.
        if let Err(e) = result {
            let desc = e.description().to_string();
            prop_assert!(
                !desc.contains("Missing required configuration parameter"),
                "a complete config must not report missing parameters, got:\n{}",
                desc
            );
        }
    }
}
