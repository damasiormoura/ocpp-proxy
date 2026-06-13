//! Core data models for OCPP message handling.
//!
//! Includes OCPP frame representation, message types, direction, connection state,
//! and exponential backoff for reconnection strategies.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::error::ProxyError;

/// Raw OCPP message frame — preserved byte-for-byte.
#[derive(Debug, Clone)]
pub struct OcppFrame {
    /// The raw JSON text exactly as received (preserves field order, whitespace).
    pub raw: String,
    /// Parsed message type for routing and logging.
    pub message_type: OcppMessageType,
    /// Unique message ID from the OCPP frame.
    pub unique_id: String,
    /// Timestamp when the proxy received this frame.
    pub received_at: DateTime<Utc>,
}

impl OcppFrame {
    /// Parse a raw JSON string into an OcppFrame.
    ///
    /// The raw string is preserved byte-for-byte. The parser extracts the message type
    /// identifier, unique ID, and (for Call messages) the action from the JSON array.
    ///
    /// OCPP 1.6J message format:
    /// - Call:       [2, UniqueId, Action, Payload]
    /// - CallResult: [3, UniqueId, Payload]
    /// - CallError:  [4, UniqueId, ErrorCode, ErrorDescription, ErrorDetails]
    pub fn parse(raw: &str) -> Result<Self, ProxyError> {
        let parsed: serde_json::Value =
            serde_json::from_str(raw).map_err(|e| ProxyError::Protocol {
                description: format!("Invalid JSON: {}", e),
            })?;

        let arr = parsed.as_array().ok_or_else(|| ProxyError::Protocol {
            description: "OCPP message must be a JSON array".to_string(),
        })?;

        if arr.len() < 3 {
            return Err(ProxyError::Protocol {
                description: format!(
                    "OCPP message array must have at least 3 elements, got {}",
                    arr.len()
                ),
            });
        }

        let type_id = arr[0].as_u64().ok_or_else(|| ProxyError::Protocol {
            description: "First element of OCPP message must be a message type integer".to_string(),
        })?;

        let unique_id = arr[1]
            .as_str()
            .ok_or_else(|| ProxyError::Protocol {
                description: "Second element of OCPP message must be a string (UniqueId)"
                    .to_string(),
            })?
            .to_string();

        let message_type = match type_id {
            2 => {
                if arr.len() < 4 {
                    return Err(ProxyError::Protocol {
                        description: "Call message must have at least 4 elements".to_string(),
                    });
                }
                let action =
                    arr[2]
                        .as_str()
                        .ok_or_else(|| ProxyError::Protocol {
                            description:
                                "Third element of Call message must be a string (Action)"
                                    .to_string(),
                        })?
                        .to_string();
                OcppMessageType::Call { action }
            }
            3 => OcppMessageType::CallResult,
            4 => OcppMessageType::CallError,
            other => {
                return Err(ProxyError::Protocol {
                    description: format!(
                        "Unknown OCPP message type: {}. Expected 2 (Call), 3 (CallResult), or 4 (CallError)",
                        other
                    ),
                });
            }
        };

        Ok(OcppFrame {
            raw: raw.to_string(),
            message_type,
            unique_id,
            received_at: Utc::now(),
        })
    }
}

/// OCPP 1.6J message types.
#[derive(Debug, Clone, PartialEq)]
pub enum OcppMessageType {
    /// [2, UniqueId, Action, Payload] — Request
    Call { action: String },
    /// [3, UniqueId, Payload] — Response
    CallResult,
    /// [4, UniqueId, ErrorCode, ErrorDescription, ErrorDetails] — Error
    CallError,
}

/// Direction of message flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Direction {
    /// Charger → Central System
    ChargerToCentral,
    /// Central System → Charger
    CentralToCharger,
}

/// Connection lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

/// Identifies which connection a state change or event belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionId {
    Upstream,
    Downstream,
    Mqtt,
}

/// State change event emitted when a connection transitions between states.
#[derive(Debug, Clone)]
pub struct StateChange {
    /// Which connection changed.
    pub connection: ConnectionId,
    /// The previous state before the transition.
    pub previous: ConnectionState,
    /// The current state after the transition.
    pub current: ConnectionState,
    /// When the transition occurred.
    pub timestamp: DateTime<Utc>,
}

/// Exponential backoff strategy for reconnection attempts.
///
/// Computes delays using: `min(initial × multiplier^(attempts - 1), max)`
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    /// Initial delay before the first retry.
    pub initial: Duration,
    /// Maximum allowed delay.
    pub max: Duration,
    /// Current computed delay value.
    pub current: Duration,
    /// Multiplier applied on each attempt (default: 2.0).
    pub multiplier: f64,
}

impl ExponentialBackoff {
    /// Create a new ExponentialBackoff with the given parameters.
    pub fn new(initial: Duration, max: Duration, multiplier: f64) -> Self {
        Self {
            initial,
            max,
            current: initial,
            multiplier,
        }
    }

    /// Create a new ExponentialBackoff with a default multiplier of 2.0.
    pub fn with_defaults(initial: Duration, max: Duration) -> Self {
        Self::new(initial, max, 2.0)
    }

    /// Returns the current delay and advances to the next one.
    ///
    /// The returned delay is `min(current, max)`. After returning,
    /// `current` is updated to `min(current * multiplier, max)`.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current.min(self.max);
        let next_millis = (self.current.as_secs_f64() * self.multiplier).min(self.max.as_secs_f64());
        self.current = Duration::from_secs_f64(next_millis);
        delay
    }

    /// Reset the backoff to its initial state.
    pub fn reset(&mut self) {
        self.current = self.initial;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_call_message() {
        let raw = r#"[2, "abc123", "BootNotification", {"chargePointModel": "Model"}]"#;
        let frame = OcppFrame::parse(raw).unwrap();
        assert_eq!(frame.raw, raw);
        assert_eq!(frame.unique_id, "abc123");
        assert_eq!(
            frame.message_type,
            OcppMessageType::Call {
                action: "BootNotification".to_string()
            }
        );
    }

    #[test]
    fn test_parse_call_result_message() {
        let raw = r#"[3, "abc123", {"status": "Accepted"}]"#;
        let frame = OcppFrame::parse(raw).unwrap();
        assert_eq!(frame.raw, raw);
        assert_eq!(frame.unique_id, "abc123");
        assert_eq!(frame.message_type, OcppMessageType::CallResult);
    }

    #[test]
    fn test_parse_call_error_message() {
        let raw =
            r#"[4, "abc123", "InternalError", "Something went wrong", {}]"#;
        let frame = OcppFrame::parse(raw).unwrap();
        assert_eq!(frame.raw, raw);
        assert_eq!(frame.unique_id, "abc123");
        assert_eq!(frame.message_type, OcppMessageType::CallError);
    }

    #[test]
    fn test_parse_preserves_raw_exactly() {
        // Test with unusual whitespace and formatting
        let raw = r#"[  2 ,  "id-1" , "Heartbeat" , { } ]"#;
        let frame = OcppFrame::parse(raw).unwrap();
        assert_eq!(frame.raw, raw);
    }

    #[test]
    fn test_parse_invalid_json() {
        let result = OcppFrame::parse("not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_not_array() {
        let result = OcppFrame::parse(r#"{"type": 2}"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_too_few_elements() {
        let result = OcppFrame::parse(r#"[2, "id"]"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unknown_message_type() {
        let result = OcppFrame::parse(r#"[5, "id", "Action", {}]"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_call_too_few_elements() {
        // Call requires at least 4 elements
        let result = OcppFrame::parse(r#"[2, "id", "Action"]"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_exponential_backoff_sequence() {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_secs(2),
            Duration::from_secs(60),
            2.0,
        );

        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(), Duration::from_secs(8));
        assert_eq!(backoff.next_delay(), Duration::from_secs(16));
        assert_eq!(backoff.next_delay(), Duration::from_secs(32));
        // Next would be 64 but max is 60
        assert_eq!(backoff.next_delay(), Duration::from_secs(60));
        // Should stay at max
        assert_eq!(backoff.next_delay(), Duration::from_secs(60));
    }

    #[test]
    fn test_exponential_backoff_reset() {
        let mut backoff = ExponentialBackoff::with_defaults(
            Duration::from_secs(1),
            Duration::from_secs(30),
        );

        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(), Duration::from_secs(4));

        backoff.reset();

        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
    }

    #[test]
    fn test_exponential_backoff_never_exceeds_max() {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_secs(1),
            Duration::from_secs(10),
            3.0,
        );

        for _ in 0..20 {
            let delay = backoff.next_delay();
            assert!(delay <= Duration::from_secs(10));
        }
    }

    #[test]
    fn test_connection_state_serialization() {
        let state = ConnectionState::Connected;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, r#""connected""#);

        let state = ConnectionState::Reconnecting;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, r#""reconnecting""#);
    }

    #[test]
    fn test_connection_state_deserialization() {
        let state: ConnectionState = serde_json::from_str(r#""disconnected""#).unwrap();
        assert_eq!(state, ConnectionState::Disconnected);
    }
}
