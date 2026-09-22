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
    /// Where to persist the retained charge point snapshot.
    ///
    /// Defaults to the systemd `StateDirectory` the unit declares. Set to an
    /// empty string to turn persistence off, in which case a proxy restart
    /// blanks the previous-session figures in Home Assistant until the next
    /// session ends — see `snapshot_store`.
    #[serde(default = "default_state_file")]
    pub state_file: String,
    /// Locally originated charger commands (SetChargingProfile and friends).
    /// Disabled unless `charging.enabled` is set: the proxy is transparent by
    /// default and only speaks OCPP itself when asked to.
    #[serde(default)]
    pub charging: ChargingConfig,
}

/// Matches `StateDirectory=ocpp-proxy` in the systemd unit, which creates the
/// directory and hands it to the service already owned and writable.
fn default_state_file() -> String {
    "/var/lib/ocpp-proxy/state.json".to_string()
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
/// Which OCPP charging-profile purpose the current limit is sent as.
///
/// `TxDefaultProfile` is the widely supported choice: it applies to every
/// transaction on the connector (connector 0 = all connectors) and yields to a
/// `TxProfile` the Central System might install, which is the right precedence
/// for a limit the site owner sets underneath the operator. `ChargePointMaxProfile`
/// is semantically a site limit and takes precedence over everything, but some
/// chargers reject it — check `SupportedFeatureProfiles` and try.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, serde::Serialize)]
pub enum ChargingProfilePurpose {
    ChargePointMaxProfile,
    TxDefaultProfile,
}

impl ChargingProfilePurpose {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChargingProfilePurpose::ChargePointMaxProfile => "ChargePointMaxProfile",
            ChargingProfilePurpose::TxDefaultProfile => "TxDefaultProfile",
        }
    }
}

/// `Absolute` schedules carry a `startSchedule` timestamp (the proxy uses
/// "now"), which every charger interprets the same way. `Relative` schedules
/// start at a charger-defined moment, which for a limit sent mid-transaction
/// varies by firmware.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, serde::Serialize)]
pub enum ChargingProfileKind {
    Absolute,
    Relative,
}

impl ChargingProfileKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChargingProfileKind::Absolute => "Absolute",
            ChargingProfileKind::Relative => "Relative",
        }
    }
}

/// Unit the limit is expressed in on the wire. Commands always take amps; with
/// `W` the proxy converts using `nominal_voltage_v` and `number_phases`, for
/// chargers whose `ChargingScheduleAllowedChargingRateUnit` is `Power` only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, serde::Serialize)]
pub enum ChargingRateUnit {
    A,
    W,
}

impl ChargingRateUnit {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChargingRateUnit::A => "A",
            ChargingRateUnit::W => "W",
        }
    }
}

fn default_max_limit_a() -> f64 {
    32.0
}

fn default_min_limit_a() -> f64 {
    6.0
}

fn default_profile_id() -> i64 {
    1001
}

fn default_purpose() -> ChargingProfilePurpose {
    ChargingProfilePurpose::TxDefaultProfile
}

fn default_profile_kind() -> ChargingProfileKind {
    ChargingProfileKind::Absolute
}

fn default_rate_unit() -> ChargingRateUnit {
    ChargingRateUnit::A
}

fn default_number_phases() -> Option<u32> {
    Some(1)
}

fn default_nominal_voltage() -> f64 {
    230.0
}

fn default_command_timeout() -> u64 {
    30
}

fn default_true() -> bool {
    true
}

/// Configuration keys `change_configuration` may touch unless overridden. These
/// are the ones a load controller has a reason to change (how often and what
/// the charger reports in MeterValues). Anything that could alter how the
/// charger reaches or authorises with the Central System is deliberately absent.
fn default_allowed_configuration_keys() -> Vec<String> {
    [
        "MeterValueSampleInterval",
        "MeterValuesSampledData",
        "ClockAlignedDataInterval",
        "MeterValuesAlignedData",
        "StopTxnSampledData",
        "StopTxnAlignedData",
    ]
    .iter()
    .map(|k| k.to_string())
    .collect()
}

/// Settings for the proxy's own OCPP commands towards the charger.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ChargingConfig {
    /// Master switch. Off: the proxy never subscribes to the command topics
    /// and never originates an OCPP message.
    #[serde(default)]
    pub enabled: bool,
    /// Ceiling on any limit the proxy will send, in amps. Requests above it
    /// are clamped, and the result says so. Set it under the site's contracted
    /// current so a misbehaving controller cannot ask for more than the mains
    /// can give.
    #[serde(default = "default_max_limit_a")]
    pub max_limit_a: f64,
    /// Lowest current an EV will actually charge at (IEC 61851: 6 A). A request
    /// below this is sent as 0 A, which pauses charging rather than asking for
    /// a current the car would refuse.
    #[serde(default = "default_min_limit_a")]
    pub min_limit_a: f64,
    /// Connector the profile is installed on. 0 = the whole charge point.
    #[serde(default)]
    pub connector_id: u32,
    /// `chargingProfileId` the proxy owns. The same id is reused on every set
    /// (replacing the previous limit) and named on clear. Kept away from the
    /// low numbers a Central System is likely to use for its own profiles.
    #[serde(default = "default_profile_id")]
    pub profile_id: i64,
    #[serde(default)]
    pub stack_level: u32,
    #[serde(default = "default_purpose")]
    pub purpose: ChargingProfilePurpose,
    #[serde(default = "default_profile_kind")]
    pub profile_kind: ChargingProfileKind,
    #[serde(default = "default_rate_unit")]
    pub rate_unit: ChargingRateUnit,
    /// `numberPhases` on the schedule period. `null` omits the field.
    #[serde(default = "default_number_phases")]
    pub number_phases: Option<u32>,
    /// Used only to convert amps to watts when `rate_unit` is `W`.
    #[serde(default = "default_nominal_voltage")]
    pub nominal_voltage_v: f64,
    /// How long a command may wait — queued or awaiting the charger's answer —
    /// before it is reported as timed out.
    #[serde(default = "default_command_timeout")]
    pub command_timeout_seconds: u64,
    /// Re-send the last accepted limit whenever the charger (re)connects, so a
    /// charger reboot that forgets its profiles does not silently return to
    /// full current.
    #[serde(default = "default_true")]
    pub reapply_on_connect: bool,
    #[serde(default = "default_allowed_configuration_keys")]
    pub allowed_configuration_keys: Vec<String>,
}

impl Default for ChargingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_limit_a: default_max_limit_a(),
            min_limit_a: default_min_limit_a(),
            connector_id: 0,
            profile_id: default_profile_id(),
            stack_level: 0,
            purpose: default_purpose(),
            profile_kind: default_profile_kind(),
            rate_unit: default_rate_unit(),
            number_phases: default_number_phases(),
            nominal_voltage_v: default_nominal_voltage(),
            command_timeout_seconds: default_command_timeout(),
            reapply_on_connect: true,
            allowed_configuration_keys: default_allowed_configuration_keys(),
        }
    }
}

impl ChargingConfig {
    pub fn command_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.command_timeout_seconds)
    }

    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if !self.max_limit_a.is_finite() || self.max_limit_a <= 0.0 || self.max_limit_a > 80.0 {
            errors.push(format!(
                "charging.max_limit_a must be between 0 and 80 (amps), got: {}",
                self.max_limit_a
            ));
        }
        if !self.min_limit_a.is_finite() || self.min_limit_a < 0.0 {
            errors.push(format!(
                "charging.min_limit_a must be zero or positive, got: {}",
                self.min_limit_a
            ));
        }
        if self.min_limit_a > self.max_limit_a {
            errors.push(format!(
                "charging.min_limit_a ({}) must not exceed charging.max_limit_a ({})",
                self.min_limit_a, self.max_limit_a
            ));
        }
        if self.profile_id <= 0 {
            errors.push(format!(
                "charging.profile_id must be a positive integer, got: {}",
                self.profile_id
            ));
        }
        if self.purpose == ChargingProfilePurpose::ChargePointMaxProfile && self.connector_id != 0 {
            errors.push(format!(
                "charging.connector_id must be 0 for ChargePointMaxProfile, got: {}",
                self.connector_id
            ));
        }
        if matches!(self.number_phases, Some(0) | Some(4..)) {
            errors.push(format!(
                "charging.number_phases must be 1, 2, 3 or null, got: {:?}",
                self.number_phases
            ));
        }
        if !self.nominal_voltage_v.is_finite() || !(100.0..=480.0).contains(&self.nominal_voltage_v)
        {
            errors.push(format!(
                "charging.nominal_voltage_v must be between 100 and 480, got: {}",
                self.nominal_voltage_v
            ));
        }
        if !(1..=300).contains(&self.command_timeout_seconds) {
            errors.push(format!(
                "charging.command_timeout_seconds must be between 1 and 300, got: {}",
                self.command_timeout_seconds
            ));
        }
        errors
    }
}

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

        errors.extend(self.charging.validate());

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
            state_file: default_state_file(),
            charging: ChargingConfig::default(),
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
            state_file: default_state_file(),
            charging: ChargingConfig::default(),
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

    // --- charging ---

    #[test]
    fn test_charging_is_off_by_default_with_sane_limits() {
        let c = ChargingConfig::default();
        assert!(!c.enabled);
        assert_eq!(c.max_limit_a, 32.0);
        assert_eq!(c.min_limit_a, 6.0);
        assert_eq!(c.connector_id, 0);
        assert_eq!(c.purpose, ChargingProfilePurpose::TxDefaultProfile);
        assert_eq!(c.profile_kind, ChargingProfileKind::Absolute);
        assert_eq!(c.rate_unit, ChargingRateUnit::A);
        assert_eq!(c.number_phases, Some(1));
        assert_eq!(c.command_timeout(), std::time::Duration::from_secs(30));
        assert!(c.reapply_on_connect);
        assert!(c
            .allowed_configuration_keys
            .iter()
            .any(|k| k == "MeterValueSampleInterval"));
        assert!(
            !c.allowed_configuration_keys
                .iter()
                .any(|k| k.contains("Authorize")),
            "nothing touching authorisation may be changeable by default"
        );
        assert!(c.validate().is_empty());
    }

    #[test]
    fn test_charging_validation_rejects_nonsense() {
        let bad = ChargingConfig {
            max_limit_a: 0.0,
            min_limit_a: -1.0,
            profile_id: 0,
            number_phases: Some(4),
            nominal_voltage_v: 12.0,
            command_timeout_seconds: 0,
            ..ChargingConfig::default()
        };
        let errors = bad.validate();
        for needle in [
            "charging.max_limit_a",
            "charging.min_limit_a",
            "charging.profile_id",
            "charging.number_phases",
            "charging.nominal_voltage_v",
            "charging.command_timeout_seconds",
        ] {
            assert!(
                errors.iter().any(|e| e.contains(needle)),
                "expected an error about {needle}, got {errors:?}"
            );
        }

        let inverted = ChargingConfig {
            min_limit_a: 20.0,
            max_limit_a: 16.0,
            ..ChargingConfig::default()
        };
        assert!(inverted
            .validate()
            .iter()
            .any(|e| e.contains("must not exceed")));

        let wrong_connector = ChargingConfig {
            purpose: ChargingProfilePurpose::ChargePointMaxProfile,
            connector_id: 1,
            ..ChargingConfig::default()
        };
        assert!(wrong_connector
            .validate()
            .iter()
            .any(|e| e.contains("must be 0 for ChargePointMaxProfile")));
    }

    #[test]
    fn test_charging_errors_surface_through_the_top_level_validate() {
        let (ca, cert, key) = create_temp_cert_files();
        let mut config = valid_config(
            ca.path().to_str().unwrap(),
            cert.path().to_str().unwrap(),
            key.path().to_str().unwrap(),
        );
        config.charging.max_limit_a = 500.0;
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("charging.max_limit_a")));
    }

    #[test]
    fn test_charging_section_loads_from_yaml_and_is_absent_by_default() {
        const BASE: &str = concat!(
            "central_system_url: \"ws://cs.example/ocpp\"\n",
            "listen_port: 9000\n",
            "mqtt:\n",
            "  host: mqtt.example\n",
            "  port: 1883\n",
            "  username: u\n",
            "  password: p\n",
        );
        const CHARGING: &str = concat!(
            "charging:\n",
            "  enabled: true\n",
            "  max_limit_a: 20\n",
            "  purpose: ChargePointMaxProfile\n",
            "  profile_kind: Relative\n",
            "  rate_unit: W\n",
            "  number_phases: null\n",
            "  allowed_configuration_keys: [MeterValueSampleInterval]\n",
        );

        let mut without = NamedTempFile::new().unwrap();
        without.write_all(BASE.as_bytes()).unwrap();
        let cfg = ProxyConfig::load_from_path(without.path().to_str().unwrap()).unwrap();
        assert_eq!(cfg.charging, ChargingConfig::default());

        let mut with = NamedTempFile::new().unwrap();
        with.write_all(BASE.as_bytes()).unwrap();
        with.write_all(CHARGING.as_bytes()).unwrap();
        let cfg = ProxyConfig::load_from_path(with.path().to_str().unwrap()).unwrap();
        assert!(cfg.charging.enabled);
        assert_eq!(cfg.charging.max_limit_a, 20.0);
        assert_eq!(
            cfg.charging.purpose,
            ChargingProfilePurpose::ChargePointMaxProfile
        );
        assert_eq!(cfg.charging.profile_kind, ChargingProfileKind::Relative);
        assert_eq!(cfg.charging.rate_unit, ChargingRateUnit::W);
        assert_eq!(cfg.charging.number_phases, None);
        assert_eq!(
            cfg.charging.allowed_configuration_keys,
            vec!["MeterValueSampleInterval".to_string()]
        );
        // Untouched keys keep their defaults.
        assert_eq!(cfg.charging.min_limit_a, 6.0);
        assert!(cfg.charging.reapply_on_connect);
    }
}
