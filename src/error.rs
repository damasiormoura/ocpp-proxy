//! Error types for the OCPP proxy.
//!
//! Categorized errors for connection, forwarding, configuration, protocol, and TLS issues.

use std::fmt;

/// Categorized error type for the OCPP proxy.
///
/// Each variant corresponds to an error category defined in the design:
/// - `ConnectionDownstream`: Charger WebSocket errors
/// - `ConnectionUpstream`: Mobi.e Central System WebSocket errors
/// - `ConnectionMqtt`: MQTT broker errors
/// - `Forwarding`: Message delivery failures
/// - `Config`: Configuration errors (fail fast at startup)
/// - `Protocol`: Invalid OCPP frames
/// - `Tls`: TLS handshake or certificate errors
#[derive(Debug)]
pub enum ProxyError {
    /// Charger WebSocket connection errors.
    /// Recovery: Log and wait for charger to reconnect.
    ConnectionDownstream { description: String },

    /// Mobi.e Central System WebSocket connection errors.
    /// Recovery: Exponential backoff reconnection (2s–60s).
    ConnectionUpstream { description: String },

    /// MQTT broker connection errors.
    /// Recovery: Exponential backoff reconnection (1s–30s), buffer messages.
    ConnectionMqtt { description: String },

    /// Message delivery failures.
    /// Recovery: Buffer if destination disconnecting, discard if buffer full.
    Forwarding { description: String },

    /// Configuration errors.
    /// Recovery: Fail fast at startup with all errors reported.
    Config { description: String },

    /// Invalid OCPP frames or protocol violations.
    /// Recovery: Log at ERROR, drop the invalid frame, do not forward.
    Protocol { description: String },

    /// TLS handshake or certificate errors.
    /// Recovery: Log at ERROR, retry connection.
    Tls { description: String },
}

impl fmt::Display for ProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProxyError::ConnectionDownstream { description } => {
                write!(f, "[connection_downstream] {}", description)
            }
            ProxyError::ConnectionUpstream { description } => {
                write!(f, "[connection_upstream] {}", description)
            }
            ProxyError::ConnectionMqtt { description } => {
                write!(f, "[connection_mqtt] {}", description)
            }
            ProxyError::Forwarding { description } => {
                write!(f, "[forwarding] {}", description)
            }
            ProxyError::Config { description } => {
                write!(f, "[config] {}", description)
            }
            ProxyError::Protocol { description } => {
                write!(f, "[protocol] {}", description)
            }
            ProxyError::Tls { description } => {
                write!(f, "[tls] {}", description)
            }
        }
    }
}

impl std::error::Error for ProxyError {}

impl ProxyError {
    /// Returns the error category as a static string for structured logging.
    pub fn category(&self) -> &'static str {
        match self {
            ProxyError::ConnectionDownstream { .. } => "connection_downstream",
            ProxyError::ConnectionUpstream { .. } => "connection_upstream",
            ProxyError::ConnectionMqtt { .. } => "connection_mqtt",
            ProxyError::Forwarding { .. } => "forwarding",
            ProxyError::Config { .. } => "config",
            ProxyError::Protocol { .. } => "protocol",
            ProxyError::Tls { .. } => "tls",
        }
    }

    /// Returns the error description.
    pub fn description(&self) -> &str {
        match self {
            ProxyError::ConnectionDownstream { description }
            | ProxyError::ConnectionUpstream { description }
            | ProxyError::ConnectionMqtt { description }
            | ProxyError::Forwarding { description }
            | ProxyError::Config { description }
            | ProxyError::Protocol { description }
            | ProxyError::Tls { description } => description,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_format() {
        let err = ProxyError::ConnectionDownstream {
            description: "Connection refused".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "[connection_downstream] Connection refused"
        );
    }

    #[test]
    fn test_error_category() {
        assert_eq!(
            ProxyError::ConnectionDownstream {
                description: String::new()
            }
            .category(),
            "connection_downstream"
        );
        assert_eq!(
            ProxyError::ConnectionUpstream {
                description: String::new()
            }
            .category(),
            "connection_upstream"
        );
        assert_eq!(
            ProxyError::ConnectionMqtt {
                description: String::new()
            }
            .category(),
            "connection_mqtt"
        );
        assert_eq!(
            ProxyError::Forwarding {
                description: String::new()
            }
            .category(),
            "forwarding"
        );
        assert_eq!(
            ProxyError::Config {
                description: String::new()
            }
            .category(),
            "config"
        );
        assert_eq!(
            ProxyError::Protocol {
                description: String::new()
            }
            .category(),
            "protocol"
        );
        assert_eq!(
            ProxyError::Tls {
                description: String::new()
            }
            .category(),
            "tls"
        );
    }

    #[test]
    fn test_error_description() {
        let err = ProxyError::Protocol {
            description: "Invalid frame format".to_string(),
        };
        assert_eq!(err.description(), "Invalid frame format");
    }

    #[test]
    fn test_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(ProxyError::Config {
            description: "Missing parameter".to_string(),
        });
        assert_eq!(err.to_string(), "[config] Missing parameter");
    }
}
