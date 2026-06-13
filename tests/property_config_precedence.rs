//! Property-based tests for environment variable precedence over YAML configuration.
//!
//! **Property 11: Environment variables take precedence over YAML configuration**
//!
//! For any configuration parameter that appears in both an environment variable and the
//! YAML file, the value from the environment variable SHALL be used, and the YAML value
//! SHALL be ignored.
//!
//! **Validates: Requirements 7.1**

use std::io::Write;
use std::sync::{LazyLock, Mutex};

use proptest::prelude::*;
use tempfile::NamedTempFile;

use ocpp_proxy::config::ProxyConfig;

/// Global mutex to serialize tests that manipulate environment variables.
/// Environment variables are process-global, so concurrent tests would interfere.
static ENV_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Helper to create temporary cert files for validation to pass.
fn create_temp_cert_files() -> (NamedTempFile, NamedTempFile, NamedTempFile) {
    let mut ca = NamedTempFile::new().unwrap();
    let mut cert = NamedTempFile::new().unwrap();
    let mut key = NamedTempFile::new().unwrap();
    writeln!(ca.as_file_mut(), "ca cert").unwrap();
    writeln!(cert.as_file_mut(), "client cert").unwrap();
    writeln!(key.as_file_mut(), "client key").unwrap();
    (ca, cert, key)
}

/// Helper to generate a valid YAML config with the given values.
fn create_yaml_config(
    listen_port: u16,
    mqtt_host: &str,
    message_buffer_size: usize,
    ca_path: &str,
    cert_path: &str,
    key_path: &str,
) -> String {
    format!(
        r#"central_system_url: "wss://mobi-e.example.com/ocpp"
listen_port: {listen_port}
health_port: 8080
mqtt:
  host: "{mqtt_host}"
  port: 8883
  username: "user"
  password: "pass"
  ca_cert_path: "{ca_path}"
  client_cert_path: "{cert_path}"
  client_key_path: "{key_path}"
buffers:
  message_buffer_size: {message_buffer_size}
  mqtt_buffer_size: 500
  max_backoff_seconds: 60
"#
    )
}

/// Clean up all OCPP_PROXY_ environment variables that may interfere with tests.
fn clean_env_vars() {
    std::env::remove_var("CONFIG_FILE_PATH");
    std::env::remove_var("OCPP_PROXY_LISTEN_PORT");
    std::env::remove_var("OCPP_PROXY_HEALTH_PORT");
    std::env::remove_var("OCPP_PROXY_CENTRAL_SYSTEM_URL");
    std::env::remove_var("OCPP_PROXY_MQTT__HOST");
    std::env::remove_var("OCPP_PROXY_MQTT__PORT");
    std::env::remove_var("OCPP_PROXY_MQTT__USERNAME");
    std::env::remove_var("OCPP_PROXY_MQTT__PASSWORD");
    std::env::remove_var("OCPP_PROXY_MQTT__CA_CERT_PATH");
    std::env::remove_var("OCPP_PROXY_MQTT__CLIENT_CERT_PATH");
    std::env::remove_var("OCPP_PROXY_MQTT__CLIENT_KEY_PATH");
    std::env::remove_var("OCPP_PROXY_BUFFERS__MESSAGE_BUFFER_SIZE");
    std::env::remove_var("OCPP_PROXY_BUFFERS__MQTT_BUFFER_SIZE");
    std::env::remove_var("OCPP_PROXY_BUFFERS__MAX_BACKOFF_SECONDS");
    std::env::remove_var("OCPP_PROXY_LOGGING__LEVEL");
}

proptest! {
    /// Property 11: Environment variable for listen_port takes precedence over YAML value.
    ///
    /// Generates two different port numbers — one for YAML and one for the env var.
    /// Verifies that ProxyConfig::load() returns the env var value.
    ///
    /// **Validates: Requirements 7.1**
    #[test]
    fn prop_env_var_overrides_yaml_listen_port(
        yaml_port in 1024u16..=60000u16,
        env_port in 1024u16..=60000u16,
    ) {
        // Only meaningful if the values differ
        prop_assume!(yaml_port != env_port);

        let _lock = ENV_MUTEX.lock().unwrap();
        clean_env_vars();

        let (ca, cert, key) = create_temp_cert_files();
        let yaml = create_yaml_config(
            yaml_port,
            "yaml.host.com",
            100,
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );

        let mut config_file = NamedTempFile::new().unwrap();
        write!(config_file.as_file_mut(), "{}", yaml).unwrap();

        std::env::set_var("CONFIG_FILE_PATH", config_file.path().to_str().unwrap());
        std::env::set_var("OCPP_PROXY_LISTEN_PORT", env_port.to_string());

        let result = ProxyConfig::load();
        clean_env_vars();

        let config = result.expect("Config should load successfully");
        prop_assert_eq!(
            config.listen_port,
            env_port,
            "Expected env port {}, got {} (YAML had {})",
            env_port, config.listen_port, yaml_port
        );
    }

    /// Property 11: Environment variable for mqtt.host takes precedence over YAML value.
    ///
    /// Generates two different hostnames — one for YAML and one for the env var.
    /// Verifies that ProxyConfig::load() returns the env var value.
    ///
    /// **Validates: Requirements 7.1**
    #[test]
    fn prop_env_var_overrides_yaml_mqtt_host(
        yaml_host in "[a-z]{3,10}\\.[a-z]{2,5}\\.com",
        env_host in "[a-z]{3,10}\\.[a-z]{2,5}\\.net",
    ) {
        let _lock = ENV_MUTEX.lock().unwrap();
        clean_env_vars();

        let (ca, cert, key) = create_temp_cert_files();
        let yaml = create_yaml_config(
            9000,
            &yaml_host,
            100,
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );

        let mut config_file = NamedTempFile::new().unwrap();
        write!(config_file.as_file_mut(), "{}", yaml).unwrap();

        std::env::set_var("CONFIG_FILE_PATH", config_file.path().to_str().unwrap());
        std::env::set_var("OCPP_PROXY_MQTT__HOST", &env_host);

        let result = ProxyConfig::load();
        clean_env_vars();

        let config = result.expect("Config should load successfully");
        prop_assert_eq!(
            config.mqtt.host.clone(),
            env_host.clone(),
            "Expected env host '{}', got '{}' (YAML had '{}')",
            env_host, config.mqtt.host, yaml_host
        );
    }

    /// Property 11: Environment variable for buffers.message_buffer_size takes precedence over YAML.
    ///
    /// Generates two different buffer sizes — one for YAML and one for the env var.
    /// Verifies that ProxyConfig::load() returns the env var value.
    ///
    /// **Validates: Requirements 7.1**
    #[test]
    fn prop_env_var_overrides_yaml_buffer_size(
        yaml_buffer in 10usize..=500usize,
        env_buffer in 10usize..=500usize,
    ) {
        prop_assume!(yaml_buffer != env_buffer);

        let _lock = ENV_MUTEX.lock().unwrap();
        clean_env_vars();

        let (ca, cert, key) = create_temp_cert_files();
        let yaml = create_yaml_config(
            9000,
            "yaml.host.com",
            yaml_buffer,
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );

        let mut config_file = NamedTempFile::new().unwrap();
        write!(config_file.as_file_mut(), "{}", yaml).unwrap();

        std::env::set_var("CONFIG_FILE_PATH", config_file.path().to_str().unwrap());
        std::env::set_var("OCPP_PROXY_BUFFERS__MESSAGE_BUFFER_SIZE", env_buffer.to_string());

        let result = ProxyConfig::load();
        clean_env_vars();

        let config = result.expect("Config should load successfully");
        prop_assert_eq!(
            config.buffers.message_buffer_size,
            env_buffer,
            "Expected env buffer size {}, got {} (YAML had {})",
            env_buffer, config.buffers.message_buffer_size, yaml_buffer
        );
    }
}
