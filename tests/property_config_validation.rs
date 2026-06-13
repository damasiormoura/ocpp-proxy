//! Property-based tests for configuration validation.
//!
//! **Property 10: Configuration validation rejects all invalid inputs**
//!
//! For any port number outside range 1–65535, for any URL not matching ws:// or wss:// scheme,
//! for any file path that does not exist or is not readable, the configuration validator
//! SHALL reject the value and report it as invalid.
//!
//! **Validates: Requirements 7.5**

use proptest::prelude::*;
use tempfile::NamedTempFile;

use ocpp_proxy::config::{BufferConfig, LogConfig, LogLevel, MqttConfig, ProxyConfig};

/// Helper to create a valid ProxyConfig using real temp files for TLS cert paths.
fn valid_config_with_files(
    ca: &NamedTempFile,
    cert: &NamedTempFile,
    key: &NamedTempFile,
) -> ProxyConfig {
    ProxyConfig {
        central_system_url: "wss://mobi-e.example.com/ocpp".to_string(),
        listen_port: 9000,
        health_port: 8080,
        mqtt: MqttConfig {
            host: "mqtt.example.com".to_string(),
            port: 8883,
            username: "user".to_string(),
            password: "pass".to_string(),
            ca_cert_path: ca.path().to_str().unwrap().to_string(),
            client_cert_path: cert.path().to_str().unwrap().to_string(),
            client_key_path: key.path().to_str().unwrap().to_string(),
        },
        logging: LogConfig {
            level: LogLevel::Info,
        },
        buffers: BufferConfig::default(),
    }
}

/// Strategy to generate invalid URLs that don't start with "ws://" or "wss://".
fn invalid_url_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // HTTP scheme
        Just("http://example.com/ocpp".to_string()),
        // HTTPS scheme
        Just("https://example.com/ocpp".to_string()),
        // FTP scheme
        Just("ftp://example.com/ocpp".to_string()),
        // Empty string
        Just("".to_string()),
        // Random string without any scheme
        "[a-zA-Z0-9]{1,20}".prop_map(|s| s),
        // Partial ws prefix but not complete
        Just("w://example.com".to_string()),
        Just("ws/example.com".to_string()),
        Just("wss/example.com".to_string()),
        // TCP scheme
        Just("tcp://example.com:9000".to_string()),
        // Just a path
        Just("/some/path".to_string()),
    ]
}

/// Strategy to generate non-existent file paths.
fn nonexistent_path_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-z]{3,10}".prop_map(|s| format!("/nonexistent/{}/file.pem", s)),
        "[a-z]{3,10}".prop_map(|s| format!("/tmp/does_not_exist_{}.pem", s)),
        Just("/no/such/path/cert.pem".to_string()),
        "[a-z]{5,15}".prop_map(|s| format!("/var/lib/{}/missing.crt", s)),
    ]
}

proptest! {
    /// Property 10: Port number 0 is always rejected for listen_port.
    ///
    /// Since listen_port is u16, the only invalid value is 0 (the type system
    /// prevents values > 65535). Verifies the validator rejects port 0.
    ///
    /// **Validates: Requirements 7.5**
    #[test]
    fn prop_listen_port_zero_rejected(
        health_port in 1u16..=65535u16,
        mqtt_port in 1u16..=65535u16,
    ) {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        let mut config = valid_config_with_files(&ca, &cert, &key);

        config.listen_port = 0;
        config.health_port = health_port;
        config.mqtt.port = mqtt_port;

        let errors = config.validate();
        prop_assert!(
            !errors.is_empty(),
            "Expected validation errors for listen_port=0, got none"
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("listen_port")),
            "Expected error mentioning 'listen_port', got: {:?}",
            errors
        );
    }

    /// Property 10: Port number 0 is always rejected for health_port.
    ///
    /// **Validates: Requirements 7.5**
    #[test]
    fn prop_health_port_zero_rejected(
        listen_port in 1u16..=65535u16,
        mqtt_port in 1u16..=65535u16,
    ) {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        let mut config = valid_config_with_files(&ca, &cert, &key);

        config.listen_port = listen_port;
        config.health_port = 0;
        config.mqtt.port = mqtt_port;

        let errors = config.validate();
        prop_assert!(
            !errors.is_empty(),
            "Expected validation errors for health_port=0, got none"
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("health_port")),
            "Expected error mentioning 'health_port', got: {:?}",
            errors
        );
    }

    /// Property 10: Port number 0 is always rejected for mqtt.port.
    ///
    /// **Validates: Requirements 7.5**
    #[test]
    fn prop_mqtt_port_zero_rejected(
        listen_port in 1u16..=65535u16,
        health_port in 1u16..=65535u16,
    ) {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        let mut config = valid_config_with_files(&ca, &cert, &key);

        config.listen_port = listen_port;
        config.health_port = health_port;
        config.mqtt.port = 0;

        let errors = config.validate();
        prop_assert!(
            !errors.is_empty(),
            "Expected validation errors for mqtt.port=0, got none"
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("mqtt.port")),
            "Expected error mentioning 'mqtt.port', got: {:?}",
            errors
        );
    }

    /// Property 10: Invalid URLs (not ws:// or wss://) are always rejected.
    ///
    /// Generates URLs that don't start with "ws://" or "wss://" and verifies
    /// the validator rejects each one with an error mentioning central_system_url.
    ///
    /// **Validates: Requirements 7.5**
    #[test]
    fn prop_invalid_url_scheme_rejected(
        invalid_url in invalid_url_strategy(),
    ) {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        let mut config = valid_config_with_files(&ca, &cert, &key);

        config.central_system_url = invalid_url.clone();

        let errors = config.validate();
        prop_assert!(
            !errors.is_empty(),
            "Expected validation errors for URL '{}', got none",
            invalid_url
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("central_system_url")),
            "Expected error mentioning 'central_system_url' for URL '{}', got: {:?}",
            invalid_url,
            errors
        );
    }

    /// Property 10: Non-existent file paths for ca_cert_path are always rejected.
    ///
    /// Generates file paths that don't exist and verifies the validator rejects them.
    ///
    /// **Validates: Requirements 7.5**
    #[test]
    fn prop_nonexistent_ca_cert_path_rejected(
        bad_path in nonexistent_path_strategy(),
    ) {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        let mut config = valid_config_with_files(&ca, &cert, &key);

        config.mqtt.ca_cert_path = bad_path.clone();

        let errors = config.validate();
        prop_assert!(
            !errors.is_empty(),
            "Expected validation errors for ca_cert_path '{}', got none",
            bad_path
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("ca_cert_path")),
            "Expected error mentioning 'ca_cert_path' for path '{}', got: {:?}",
            bad_path,
            errors
        );
    }

    /// Property 10: Non-existent file paths for client_cert_path are always rejected.
    ///
    /// **Validates: Requirements 7.5**
    #[test]
    fn prop_nonexistent_client_cert_path_rejected(
        bad_path in nonexistent_path_strategy(),
    ) {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        let mut config = valid_config_with_files(&ca, &cert, &key);

        config.mqtt.client_cert_path = bad_path.clone();

        let errors = config.validate();
        prop_assert!(
            !errors.is_empty(),
            "Expected validation errors for client_cert_path '{}', got none",
            bad_path
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("client_cert_path")),
            "Expected error mentioning 'client_cert_path' for path '{}', got: {:?}",
            bad_path,
            errors
        );
    }

    /// Property 10: Non-existent file paths for client_key_path are always rejected.
    ///
    /// **Validates: Requirements 7.5**
    #[test]
    fn prop_nonexistent_client_key_path_rejected(
        bad_path in nonexistent_path_strategy(),
    ) {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        let mut config = valid_config_with_files(&ca, &cert, &key);

        config.mqtt.client_key_path = bad_path.clone();

        let errors = config.validate();
        prop_assert!(
            !errors.is_empty(),
            "Expected validation errors for client_key_path '{}', got none",
            bad_path
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("client_key_path")),
            "Expected error mentioning 'client_key_path' for path '{}', got: {:?}",
            bad_path,
            errors
        );
    }

    /// Property 10: Multiple invalid values are all reported together.
    ///
    /// When multiple fields are invalid, the validator reports errors for all of them
    /// in a single validation pass, not just the first one found.
    ///
    /// **Validates: Requirements 7.5**
    #[test]
    fn prop_multiple_invalid_fields_all_reported(
        invalid_url in invalid_url_strategy(),
        bad_ca_path in nonexistent_path_strategy(),
        bad_cert_path in nonexistent_path_strategy(),
        bad_key_path in nonexistent_path_strategy(),
    ) {
        let config = ProxyConfig {
            central_system_url: invalid_url,
            listen_port: 0,
            health_port: 0,
            mqtt: MqttConfig {
                host: "localhost".to_string(),
                port: 0,
                username: "user".to_string(),
                password: "pass".to_string(),
                ca_cert_path: bad_ca_path,
                client_cert_path: bad_cert_path,
                client_key_path: bad_key_path,
            },
            logging: LogConfig {
                level: LogLevel::Info,
            },
            buffers: BufferConfig::default(),
        };

        let errors = config.validate();

        // All invalid fields should be reported
        prop_assert!(
            errors.iter().any(|e| e.contains("listen_port")),
            "Missing listen_port error in: {:?}",
            errors
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("health_port")),
            "Missing health_port error in: {:?}",
            errors
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("central_system_url")),
            "Missing central_system_url error in: {:?}",
            errors
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("mqtt.port")),
            "Missing mqtt.port error in: {:?}",
            errors
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("ca_cert_path")),
            "Missing ca_cert_path error in: {:?}",
            errors
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("client_cert_path")),
            "Missing client_cert_path error in: {:?}",
            errors
        );
        prop_assert!(
            errors.iter().any(|e| e.contains("client_key_path")),
            "Missing client_key_path error in: {:?}",
            errors
        );
    }

    /// Property 10: Valid configurations pass validation (soundness check).
    ///
    /// Generates valid port numbers and ws/wss URLs and verifies no errors are produced.
    /// This ensures the validator doesn't over-reject.
    ///
    /// **Validates: Requirements 7.5**
    #[test]
    fn prop_valid_config_passes_validation(
        listen_port in 1u16..=65535u16,
        health_port in 1u16..=65535u16,
        mqtt_port in 1u16..=65535u16,
        use_wss in proptest::bool::ANY,
        host in "[a-z]{3,10}\\.[a-z]{2,4}",
    ) {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();

        let url = if use_wss {
            format!("wss://{}/ocpp", host)
        } else {
            format!("ws://{}/ocpp", host)
        };

        let config = ProxyConfig {
            central_system_url: url,
            listen_port,
            health_port,
            mqtt: MqttConfig {
                host: host.clone(),
                port: mqtt_port,
                username: "user".to_string(),
                password: "pass".to_string(),
                ca_cert_path: ca.path().to_str().unwrap().to_string(),
                client_cert_path: cert.path().to_str().unwrap().to_string(),
                client_key_path: key.path().to_str().unwrap().to_string(),
            },
            logging: LogConfig {
                level: LogLevel::Info,
            },
            buffers: BufferConfig::default(),
        };

        let errors = config.validate();
        prop_assert!(
            errors.is_empty(),
            "Expected no validation errors for valid config, got: {:?}",
            errors
        );
    }
}
