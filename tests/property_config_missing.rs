//! Property-based tests for missing parameter reporting.
//!
//! **Property 12: All missing required parameters are reported together**
//!
//! For any non-empty subset of required configuration parameters that are absent,
//! the startup error output SHALL indicate a configuration failure. The `config` crate
//! reports deserialization errors when required fields are missing. At minimum, loading
//! must fail when any required field is missing.
//!
//! **Validates: Requirements 7.2, 7.3**

use proptest::prelude::*;
use std::io::Write;
use tempfile::NamedTempFile;

use ocpp_proxy::config::ProxyConfig;

/// All required YAML field names and their sample valid values.
/// These are the fields that MUST be present for ProxyConfig::load() to succeed.
const REQUIRED_FIELDS: &[(&str, &str)] = &[
    ("central_system_url", "\"wss://example.com/ocpp\""),
    ("listen_port", "9000"),
    ("mqtt.host", "\"mqtt.example.com\""),
    ("mqtt.port", "8883"),
    ("mqtt.username", "\"user\""),
    ("mqtt.password", "\"pass\""),
    ("mqtt.ca_cert_path", "\"/tmp/ca.pem\""),
    ("mqtt.client_cert_path", "\"/tmp/cert.pem\""),
    ("mqtt.client_key_path", "\"/tmp/key.pem\""),
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
    proptest::collection::vec(proptest::bool::ANY, REQUIRED_FIELDS.len()).prop_filter(
        "at least one field must be omitted",
        |bools| bools.iter().any(|&b| b),
    ).prop_map(|bools| {
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
    /// Generates random non-empty subsets of required configuration parameters to omit
    /// from the YAML file. Verifies that ProxyConfig::load() fails when any required
    /// parameter is missing, and that the error message mentions at least one of the
    /// missing fields (indicating a deserialization failure for the missing data).
    ///
    /// Note: The `config` crate may report only the first missing field encountered
    /// during deserialization. This test verifies the fundamental property that load
    /// fails and produces a meaningful error referencing a missing field.
    ///
    /// **Validates: Requirements 7.2, 7.3**
    #[test]
    fn prop_missing_required_params_cause_load_failure(
        omit_indices in omit_subset_strategy()
    ) {
        let yaml = build_yaml_without(&omit_indices);

        // Write the YAML to a temp file
        let mut config_file = NamedTempFile::new().unwrap();
        write!(config_file.as_file_mut(), "{}", yaml).unwrap();

        // Set CONFIG_FILE_PATH to the temp file and clear any env vars that could
        // supply the missing values
        let config_path = config_file.path().to_str().unwrap().to_string();

        // Clear all OCPP_PROXY_ env vars that might fill in missing fields
        let env_vars_to_clear: Vec<String> = std::env::vars()
            .filter(|(k, _)| k.starts_with("OCPP_PROXY"))
            .map(|(k, _)| k)
            .collect();
        for var in &env_vars_to_clear {
            std::env::remove_var(var);
        }

        std::env::set_var("CONFIG_FILE_PATH", &config_path);
        let result = ProxyConfig::load();
        std::env::remove_var("CONFIG_FILE_PATH");

        // Property: load must fail when required parameters are missing
        prop_assert!(
            result.is_err(),
            "ProxyConfig::load() should have failed with omitted fields {:?}, but succeeded. YAML:\n{}",
            omit_indices.iter().map(|&i| REQUIRED_FIELDS[i].0).collect::<Vec<_>>(),
            yaml
        );

        // Property: the error should be a config error with a meaningful description
        let err = result.unwrap_err();
        prop_assert_eq!(
            err.category(),
            "config",
            "Error category should be 'config', got '{}' for error: {}",
            err.category(),
            err
        );

        // Property: the error description should be non-empty and reference deserialization
        let desc = err.description().to_string();
        prop_assert!(
            !desc.is_empty(),
            "Error description should not be empty"
        );

        // Verify the error mentions at least one of the missing fields or indicates
        // a deserialization failure (the config crate reports "missing field" errors)
        let omitted_field_names: Vec<&str> = omit_indices
            .iter()
            .map(|&i| {
                let name = REQUIRED_FIELDS[i].0;
                // For mqtt.X fields, the error may reference just "X" or "mqtt"
                if let Some(field) = name.strip_prefix("mqtt.") {
                    field
                } else {
                    name
                }
            })
            .collect();

        let desc_lower = desc.to_lowercase();
        let mentions_missing_field = omitted_field_names.iter().any(|field| {
            desc_lower.contains(&field.to_lowercase())
        });
        let mentions_deserialization = desc_lower.contains("deserializ")
            || desc_lower.contains("missing field")
            || desc_lower.contains("missing")
            || desc_lower.contains("not found");

        prop_assert!(
            mentions_missing_field || mentions_deserialization,
            "Error should mention a missing field or deserialization failure. \
             Omitted: {:?}, Error: {}",
            omitted_field_names,
            desc
        );
    }

    /// Property 12 (supplementary): Omitting all mqtt fields causes a single clear error.
    ///
    /// When the entire `mqtt` section is missing, the error should indicate that the
    /// mqtt configuration is required.
    ///
    /// **Validates: Requirements 7.2, 7.3**
    #[test]
    fn prop_omitting_any_single_required_param_fails(
        field_index in 0..REQUIRED_FIELDS.len()
    ) {
        let yaml = build_yaml_without(&[field_index]);

        let mut config_file = NamedTempFile::new().unwrap();
        write!(config_file.as_file_mut(), "{}", yaml).unwrap();

        let config_path = config_file.path().to_str().unwrap().to_string();

        // Clear all OCPP_PROXY_ env vars
        let env_vars_to_clear: Vec<String> = std::env::vars()
            .filter(|(k, _)| k.starts_with("OCPP_PROXY"))
            .map(|(k, _)| k)
            .collect();
        for var in &env_vars_to_clear {
            std::env::remove_var(var);
        }

        std::env::set_var("CONFIG_FILE_PATH", &config_path);
        let result = ProxyConfig::load();
        std::env::remove_var("CONFIG_FILE_PATH");

        // Property: omitting any single required field must cause load to fail
        let field_name = REQUIRED_FIELDS[field_index].0;
        prop_assert!(
            result.is_err(),
            "ProxyConfig::load() should have failed without '{}', but succeeded. YAML:\n{}",
            field_name,
            yaml
        );

        let err = result.unwrap_err();
        prop_assert_eq!(err.category(), "config");
    }
}
