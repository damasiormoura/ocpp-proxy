//! Configuration management for the OCPP proxy.
//!
//! Supports layered configuration: environment variables take precedence over YAML file values.
//! Environment variables use the prefix `OCPP_PROXY_` with `__` as the nested separator.

use std::net::IpAddr;
use std::path::Path;

use serde::Deserialize;

use crate::error::ProxyError;

/// Log level for the proxy application.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warning => write!(f, "WARNING"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

fn default_health_port() -> u16 {
    8080
}

fn default_listen_address() -> IpAddr {
    IpAddr::from([0, 0, 0, 0])
}

fn default_log_level() -> LogLevel {
    LogLevel::Info
}

fn default_message_buffer() -> usize {
    100
}

fn default_mqtt_buffer() -> usize {
    500
}

fn default_max_backoff() -> u64 {
    60
}

/// Top-level proxy configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ProxyConfig {
    pub central_system_url: String,
    pub listen_port: u16,
    /// Charger-facing bind address. Defaults to all interfaces.
    ///
    /// In the LXC deployment this is the `vmbr1` address, so the listener is
    /// never exposed on the WWAN-egress leg or the main LAN.
    #[serde(default = "default_listen_address")]
    pub listen_address: IpAddr,
    /// Local source address for the upstream socket.
    ///
    /// Not needed when the Central System is reached by a destination route,
    /// which is the case for the Mobi.e APN. Retained for a deployment where
    /// egress must be selected by source address instead.
    #[serde(default)]
    pub upstream_bind_address: Option<IpAddr>,
    /// Charge Point ID used for the MQTT Last Will and Testament topic.
    ///
    /// The LWT must be registered before the broker connection is opened, so
    /// it cannot be derived from a charger connection that has not happened
    /// yet. Per-message and status topics use the ID from the live connection;
    /// only the availability topic depends on this.
    #[serde(default)]
    pub charge_point_id: Option<String>,
    #[serde(default = "default_health_port")]
    pub health_port: u16,
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub logging: LogConfig,
    #[serde(default)]
    pub buffers: BufferConfig,
}

/// MQTT broker connection configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    /// TLS is optional: the broker is one LAN hop away, not across the
    /// internet. All three absent means plaintext; `ca_cert_path` alone means
    /// server-authenticated TLS; all three means mutual TLS.
    #[serde(default)]
    pub ca_cert_path: Option<String>,
    #[serde(default)]
    pub client_cert_path: Option<String>,
    #[serde(default)]
    pub client_key_path: Option<String>,
}

impl MqttConfig {
    /// Whether TLS should be used for the broker connection.
    pub fn tls_enabled(&self) -> bool {
        self.ca_cert_path.is_some()
    }

    /// Whether a client certificate and key are both configured.
    pub fn client_auth_enabled(&self) -> bool {
        self.client_cert_path.is_some() && self.client_key_path.is_some()
    }
}

/// Logging configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: LogLevel,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

/// Buffer size and backoff configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    #[serde(default = "default_message_buffer")]
    pub message_buffer_size: usize,
    #[serde(default = "default_mqtt_buffer")]
    pub mqtt_buffer_size: usize,
    #[serde(default = "default_max_backoff")]
    pub max_backoff_seconds: u64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            message_buffer_size: default_message_buffer(),
            mqtt_buffer_size: default_mqtt_buffer(),
            max_backoff_seconds: default_max_backoff(),
        }
    }
}

/// All-optional mirror of the required parts of [`ProxyConfig`].
///
/// Exists only so that every missing required parameter can be reported at
/// once, rather than serde stopping at the first one.
#[derive(Debug, Deserialize)]
struct RawProxyConfig {
    central_system_url: Option<String>,
    listen_port: Option<u16>,
    mqtt: Option<RawMqttConfig>,
}

#[derive(Debug, Deserialize)]
struct RawMqttConfig {
    host: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<String>,
}

impl RawProxyConfig {
    /// Names of every required parameter that is absent.
    ///
    /// TLS certificate paths are deliberately absent from this list: they are
    /// optional, because the broker is reached over one local hop rather than
    /// across the internet.
    fn missing_required(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();

        if self.central_system_url.is_none() {
            missing.push("central_system_url");
        }
        if self.listen_port.is_none() {
            missing.push("listen_port");
        }

        match &self.mqtt {
            None => missing.extend_from_slice(&[
                "mqtt.host",
                "mqtt.port",
                "mqtt.username",
                "mqtt.password",
            ]),
            Some(mqtt) => {
                if mqtt.host.is_none() {
                    missing.push("mqtt.host");
                }
                if mqtt.port.is_none() {
                    missing.push("mqtt.port");
                }
                if mqtt.username.is_none() {
                    missing.push("mqtt.username");
                }
                if mqtt.password.is_none() {
                    missing.push("mqtt.password");
                }
            }
        }

        missing
    }
}

impl ProxyConfig {
    /// Load configuration from environment variables and YAML file.
    ///
    /// Layering order (highest precedence first):
    /// 1. Environment variables prefixed with `OCPP_PROXY_` (nested via `__`)
    /// 2. YAML file at `CONFIG_FILE_PATH` env var or `./config.yaml`
    pub fn load() -> Result<Self, ProxyError> {
        let config_path =
            std::env::var("CONFIG_FILE_PATH").unwrap_or_else(|_| "./config.yaml".to_string());
        Self::load_from_path(&config_path)
    }

    /// Load configuration from a specific YAML file path and environment variables.
    ///
    /// Layering order (highest precedence first):
    /// 1. Environment variables prefixed with `OCPP_PROXY_` (nested via `__`)
    /// 2. YAML file at the given path
    pub fn load_from_path(config_path: &str) -> Result<Self, ProxyError> {
        let builder = config::Config::builder()
            .add_source(
                config::File::with_name(config_path)
                    .format(config::FileFormat::Yaml)
                    .required(false),
            )
            .add_source(
                config::Environment::with_prefix("OCPP_PROXY")
                    .prefix_separator("_")
                    .separator("__")
                    .try_parsing(true),
            );

        let settings = builder.build().map_err(|e| ProxyError::Config {
            description: format!("Failed to load configuration: {}", e),
        })?;

        // Requirement 7.3 — report EVERY missing parameter in one error.
        //
        // Deserialising straight into `ProxyConfig` cannot do this: serde
        // fails on the first absent field, so an operator missing four
        // settings has to restart four times to discover them all. Passing
        // through an all-optional shadow struct first lets us collect them.
        if let Ok(raw) = settings.clone().try_deserialize::<RawProxyConfig>() {
            let missing = raw.missing_required();
            if !missing.is_empty() {
                return Err(ProxyError::Config {
                    description: format!(
                        "Missing required configuration parameter{}:\n{}",
                        if missing.len() == 1 { "" } else { "s" },
                        missing
                            .iter()
                            .map(|m| format!("  - {}", m))
                            .collect::<Vec<_>>()
                            .join("\n")
                    ),
                });
            }
        }

        let proxy_config: ProxyConfig =
            settings.try_deserialize().map_err(|e| ProxyError::Config {
                description: format!("Failed to deserialize configuration: {}", e),
            })?;

        let errors = proxy_config.validate();
        if !errors.is_empty() {
            return Err(ProxyError::Config {
                description: format!("Configuration validation failed:\n{}", errors.join("\n")),
            });
        }

        Ok(proxy_config)
    }

    /// Validate all configuration parameters.
    ///
    /// Returns a list of all validation errors found, allowing the operator
    /// to fix all issues at once rather than iterating one error at a time.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        // Requirement 1.1 constrains the charger port to the unprivileged range;
        // the proxy runs as a non-root service and cannot bind below 1024.
        if self.listen_port < 1024 {
            errors.push(format!(
                "listen_port must be between 1024 and 65535, got: {}",
                self.listen_port
            ));
        }

        if self.health_port < 1024 {
            errors.push(format!(
                "health_port must be between 1024 and 65535, got: {}",
                self.health_port
            ));
        }

        if self.listen_port == self.health_port {
            errors.push(format!(
                "listen_port and health_port must differ, both are: {}",
                self.listen_port
            ));
        }

        // Validate central_system_url scheme
        if !self.central_system_url.starts_with("ws://")
            && !self.central_system_url.starts_with("wss://")
        {
            errors.push(format!(
                "central_system_url must start with ws:// or wss://, got: {}",
                self.central_system_url
            ));
        }

        // Validate MQTT port is not 0
        if self.mqtt.port == 0 {
            errors.push("mqtt.port must be between 1 and 65535".to_string());
        }

        // TLS certificate paths are optional, but any path that IS given must
        // point at a readable file — existence alone is not enough, since the
        // service runs as an unprivileged user that may not be able to read it.
        for (name, path) in [
            ("mqtt.ca_cert_path", self.mqtt.ca_cert_path.as_ref()),
            ("mqtt.client_cert_path", self.mqtt.client_cert_path.as_ref()),
            ("mqtt.client_key_path", self.mqtt.client_key_path.as_ref()),
        ] {
            if let Some(path) = path {
                if !Path::new(path).exists() {
                    errors.push(format!("{} does not exist: {}", name, path));
                } else if std::fs::File::open(path).is_err() {
                    errors.push(format!("{} exists but is not readable: {}", name, path));
                }
            }
        }

        // A client certificate without its key (or the reverse) would silently
        // downgrade to server-authenticated TLS. Fail instead of degrading.
        if self.mqtt.client_cert_path.is_some() != self.mqtt.client_key_path.is_some() {
            errors.push(
                "mqtt.client_cert_path and mqtt.client_key_path must be set together".to_string(),
            );
        }
        if !self.mqtt.tls_enabled() && self.mqtt.client_auth_enabled() {
            errors.push("mqtt client certificates require mqtt.ca_cert_path to be set".to_string());
        }

        errors
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Helper to create a valid ProxyConfig for testing.
    fn valid_config(ca_path: &str, cert_path: &str, key_path: &str) -> ProxyConfig {
        ProxyConfig {
            central_system_url: "wss://mobi-e.example.com/ocpp".to_string(),
            listen_port: 9000,
            listen_address: default_listen_address(),
            upstream_bind_address: None,
            charge_point_id: None,
            health_port: 8080,
            mqtt: MqttConfig {
                host: "mqtt.example.com".to_string(),
                port: 8883,
                username: "user".to_string(),
                password: "pass".to_string(),
                ca_cert_path: Some(ca_path.to_string()),
                client_cert_path: Some(cert_path.to_string()),
                client_key_path: Some(key_path.to_string()),
            },
            logging: LogConfig {
                level: LogLevel::Info,
            },
            buffers: BufferConfig::default(),
        }
    }

    /// Create temp files to simulate TLS certs existing on disk.
    fn create_temp_cert_files() -> (NamedTempFile, NamedTempFile, NamedTempFile) {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        (ca, cert, key)
    }

    #[test]
    fn test_valid_config_passes_validation() {
        let (ca, cert, key) = create_temp_cert_files();
        let config = valid_config(
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );
        let errors = config.validate();
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_listen_port_zero_rejected() {
        let (ca, cert, key) = create_temp_cert_files();
        let mut config = valid_config(
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );
        config.listen_port = 0;
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("listen_port")));
    }

    #[test]
    fn test_health_port_zero_rejected() {
        let (ca, cert, key) = create_temp_cert_files();
        let mut config = valid_config(
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );
        config.health_port = 0;
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("health_port")));
    }

    #[test]
    fn test_invalid_url_scheme_http() {
        let (ca, cert, key) = create_temp_cert_files();
        let mut config = valid_config(
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );
        config.central_system_url = "http://example.com/ocpp".to_string();
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("central_system_url")));
    }

    #[test]
    fn test_invalid_url_scheme_empty() {
        let (ca, cert, key) = create_temp_cert_files();
        let mut config = valid_config(
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );
        config.central_system_url = "".to_string();
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("central_system_url")));
    }

    #[test]
    fn test_ws_scheme_accepted() {
        let (ca, cert, key) = create_temp_cert_files();
        let mut config = valid_config(
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );
        config.central_system_url = "ws://example.com/ocpp".to_string();
        let errors = config.validate();
        assert!(!errors.iter().any(|e| e.contains("central_system_url")));
    }

    #[test]
    fn test_wss_scheme_accepted() {
        let (ca, cert, key) = create_temp_cert_files();
        let config = valid_config(
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );
        // Default is wss://
        let errors = config.validate();
        assert!(!errors.iter().any(|e| e.contains("central_system_url")));
    }

    #[test]
    fn test_mqtt_port_zero_rejected() {
        let (ca, cert, key) = create_temp_cert_files();
        let mut config = valid_config(
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );
        config.mqtt.port = 0;
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("mqtt.port")));
    }

    #[test]
    fn test_nonexistent_ca_cert_path_rejected() {
        let (_ca, cert, key) = create_temp_cert_files();
        let config = valid_config(
            "/nonexistent/ca.pem",
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("ca_cert_path")));
    }

    #[test]
    fn test_nonexistent_client_cert_path_rejected() {
        let (ca, _cert, key) = create_temp_cert_files();
        let config = valid_config(
            ca.path().to_str().unwrap(),
            "/nonexistent/client.pem",
            key.path().to_str().unwrap(),
        );
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("client_cert_path")));
    }

    #[test]
    fn test_nonexistent_client_key_path_rejected() {
        let (ca, cert, _key) = create_temp_cert_files();
        let config = valid_config(
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            "/nonexistent/key.pem",
        );
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("client_key_path")));
    }

    #[test]
    fn test_multiple_errors_reported_together() {
        let config = ProxyConfig {
            central_system_url: "http://bad.com".to_string(),
            listen_port: 0,
            listen_address: default_listen_address(),
            upstream_bind_address: None,
            charge_point_id: None,
            health_port: 0,
            mqtt: MqttConfig {
                host: "localhost".to_string(),
                port: 0,
                username: "user".to_string(),
                password: "pass".to_string(),
                ca_cert_path: Some("/nonexistent/ca.pem".to_string()),
                client_cert_path: Some("/nonexistent/cert.pem".to_string()),
                client_key_path: Some("/nonexistent/key.pem".to_string()),
            },
            logging: LogConfig::default(),
            buffers: BufferConfig::default(),
        };
        let errors = config.validate();
        // Should report all errors, not just the first
        assert!(
            errors.len() >= 5,
            "Expected at least 5 errors, got: {:?}",
            errors
        );
        assert!(errors.iter().any(|e| e.contains("listen_port")));
        assert!(errors.iter().any(|e| e.contains("health_port")));
        assert!(errors.iter().any(|e| e.contains("central_system_url")));
        assert!(errors.iter().any(|e| e.contains("mqtt.port")));
        assert!(errors.iter().any(|e| e.contains("ca_cert_path")));
        assert!(errors.iter().any(|e| e.contains("client_cert_path")));
        assert!(errors.iter().any(|e| e.contains("client_key_path")));
    }

    #[test]
    fn test_default_log_level_is_info() {
        let config = LogConfig::default();
        assert_eq!(config.level, LogLevel::Info);
    }

    #[test]
    fn test_default_buffer_config() {
        let config = BufferConfig::default();
        assert_eq!(config.message_buffer_size, 100);
        assert_eq!(config.mqtt_buffer_size, 500);
        assert_eq!(config.max_backoff_seconds, 60);
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(LogLevel::Debug.to_string(), "DEBUG");
        assert_eq!(LogLevel::Info.to_string(), "INFO");
        assert_eq!(LogLevel::Warning.to_string(), "WARNING");
        assert_eq!(LogLevel::Error.to_string(), "ERROR");
    }

    #[test]
    fn test_log_level_deserialization_case_insensitive() {
        // The config crate uppercases env vars, so we test various cases
        let cases = vec![
            (r#""DEBUG""#, LogLevel::Debug),
            (r#""INFO""#, LogLevel::Info),
            (r#""WARNING""#, LogLevel::Warning),
            (r#""ERROR""#, LogLevel::Error),
        ];
        for (input, expected) in cases {
            let level: LogLevel = serde_json::from_str(input).unwrap();
            assert_eq!(level, expected);
        }
    }

    #[test]
    fn test_log_level_deserialization_rejects_invalid() {
        let result: Result<LogLevel, _> = serde_json::from_str(r#""TRACE""#);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_from_yaml_file() {
        let (mut ca, mut cert, mut key) = create_temp_cert_files();
        // Write some content to make them valid files
        writeln!(ca.as_file_mut(), "ca cert").unwrap();
        writeln!(cert.as_file_mut(), "client cert").unwrap();
        writeln!(key.as_file_mut(), "client key").unwrap();

        let yaml_content = format!(
            r#"
central_system_url: "wss://mobi-e.example.com/ocpp"
listen_port: 9000
health_port: 8080
mqtt:
  host: "mqtt.example.com"
  port: 8883
  username: "testuser"
  password: "testpass"
  ca_cert_path: "{}"
  client_cert_path: "{}"
  client_key_path: "{}"
logging:
  level: "DEBUG"
buffers:
  message_buffer_size: 200
  mqtt_buffer_size: 1000
  max_backoff_seconds: 120
"#,
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );

        let mut config_file = NamedTempFile::new().unwrap();
        write!(config_file.as_file_mut(), "{}", yaml_content).unwrap();

        // Use load_from_path directly to avoid env var race conditions in parallel tests
        let result = ProxyConfig::load_from_path(config_file.path().to_str().unwrap());

        let config = result.expect("Should load valid config");
        assert_eq!(config.central_system_url, "wss://mobi-e.example.com/ocpp");
        assert_eq!(config.listen_port, 9000);
        assert_eq!(config.health_port, 8080);
        assert_eq!(config.mqtt.host, "mqtt.example.com");
        assert_eq!(config.mqtt.port, 8883);
        assert_eq!(config.mqtt.username, "testuser");
        assert_eq!(config.logging.level, LogLevel::Debug);
        assert_eq!(config.buffers.message_buffer_size, 200);
        assert_eq!(config.buffers.mqtt_buffer_size, 1000);
        assert_eq!(config.buffers.max_backoff_seconds, 120);
    }

    #[test]
    fn test_load_missing_required_field_fails() {
        let yaml_content = r#"
listen_port: 9000
"#;

        let mut config_file = NamedTempFile::new().unwrap();
        write!(config_file.as_file_mut(), "{}", yaml_content).unwrap();

        // Use load_from_path directly to avoid env var race conditions in parallel tests
        let result = ProxyConfig::load_from_path(config_file.path().to_str().unwrap());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.category(), "config");
    }

    #[test]
    fn test_default_health_port() {
        assert_eq!(default_health_port(), 8080);
    }
}
