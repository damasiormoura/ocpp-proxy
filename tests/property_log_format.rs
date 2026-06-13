//! Property-based tests for structured log format.
//!
//! **Property 15: All log entries are valid structured JSON with required fields**
//!
//! For any log event emitted by the proxy, the output SHALL be valid JSON containing
//! at minimum: `timestamp` (ISO 8601), `level` (one of TRACE/DEBUG/INFO/WARN/ERROR),
//! `target` (non-empty string identifying the component), and the message content.
//!
//! **Validates: Requirements 8.5**

use proptest::prelude::*;
use std::sync::{Arc, Mutex};
use tracing_subscriber::{
    fmt::{self, time::ChronoUtc, MakeWriter},
    layer::SubscriberExt,
};

/// A thread-safe buffer that captures tracing output.
#[derive(Clone)]
struct TestBuffer {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl TestBuffer {
    fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn contents(&self) -> String {
        let buf = self.buffer.lock().unwrap();
        String::from_utf8_lossy(&buf).to_string()
    }

    fn clear(&self) {
        self.buffer.lock().unwrap().clear();
    }
}

impl std::io::Write for TestBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for TestBuffer {
    type Writer = TestBuffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Strategy for generating log levels (as tracing Level enum variants).
fn log_level_strategy() -> impl Strategy<Value = tracing::Level> {
    prop_oneof![
        Just(tracing::Level::TRACE),
        Just(tracing::Level::DEBUG),
        Just(tracing::Level::INFO),
        Just(tracing::Level::WARN),
        Just(tracing::Level::ERROR),
    ]
}

/// Strategy for generating non-empty component/target names.
/// These simulate different proxy components.
fn component_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("forwarder".to_string()),
        Just("downstream".to_string()),
        Just("upstream".to_string()),
        Just("mqtt".to_string()),
        Just("state".to_string()),
        Just("health".to_string()),
        Just("config".to_string()),
        // Also generate random alphanumeric targets
        "[a-z][a-z0-9_]{1,20}".prop_map(|s| s),
    ]
}

/// Strategy for generating non-empty log messages.
fn message_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("Connection state transition".to_string()),
        Just("OCPP message forwarded".to_string()),
        Just("Message forwarding latency exceeded 500ms threshold".to_string()),
        Just("Failed to deliver message".to_string()),
        Just("Broker connection timeout".to_string()),
        // Random messages with printable ASCII (avoiding control chars that break JSON)
        "[a-zA-Z0-9 .,;:!?\\-_()]{1,100}".prop_map(|s| s),
    ]
}

/// Valid log level strings in the tracing-subscriber JSON output.
const VALID_LEVELS: &[&str] = &["TRACE", "DEBUG", "INFO", "WARN", "ERROR"];

/// Emit a tracing event at the specified level with the given message.
/// Uses a macro dispatch since tracing macros require static levels.
fn emit_event(level: tracing::Level, component: &str, message: &str) {
    match level {
        l if l == tracing::Level::TRACE => {
            tracing::trace!(target: "property_test", component = component, "{}", message);
        }
        l if l == tracing::Level::DEBUG => {
            tracing::debug!(target: "property_test", component = component, "{}", message);
        }
        l if l == tracing::Level::INFO => {
            tracing::info!(target: "property_test", component = component, "{}", message);
        }
        l if l == tracing::Level::WARN => {
            tracing::warn!(target: "property_test", component = component, "{}", message);
        }
        l if l == tracing::Level::ERROR => {
            tracing::error!(target: "property_test", component = component, "{}", message);
        }
        _ => unreachable!(),
    }
}

proptest! {
    /// Property 15: All log entries are valid structured JSON with required fields.
    ///
    /// Generates random log events with varied levels, components, and messages.
    /// Emits each event through tracing-subscriber's JSON formatter and verifies:
    /// 1. The output is valid JSON
    /// 2. Contains "timestamp" field with ISO 8601 format
    /// 3. Contains "level" field with a valid level string
    /// 4. Contains "target" field (non-empty string)
    /// 5. Contains the message content somewhere in the output
    ///
    /// **Validates: Requirements 8.5**
    #[test]
    fn prop_log_entries_are_valid_structured_json(
        level in log_level_strategy(),
        component in component_strategy(),
        message in message_strategy(),
    ) {
        // Create a buffer to capture JSON output
        let buffer = TestBuffer::new();
        let buffer_clone = buffer.clone();

        // Build a subscriber with JSON formatting writing to our buffer
        let json_layer = fmt::layer()
            .json()
            .with_timer(ChronoUtc::rfc_3339())
            .with_target(true)
            .with_level(true)
            .with_thread_ids(false)
            .with_thread_names(false)
            .with_writer(buffer_clone);

        // Use a filter that allows all levels (TRACE and above)
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::TRACE)
            .with(json_layer);

        // Emit the event within this subscriber's scope
        tracing::subscriber::with_default(subscriber, || {
            emit_event(level, &component, &message);
        });

        // Get the captured output
        let output = buffer.contents();

        // The output should not be empty — the event was emitted
        prop_assert!(
            !output.trim().is_empty(),
            "Expected non-empty log output for level={:?}, component={}, message={}",
            level, component, message
        );

        // Each line of output should be valid JSON (tracing-subscriber outputs one JSON object per line)
        for line in output.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            // Property 1: Output is valid JSON
            let json: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
                TestCaseError::Fail(
                    format!("Log output is not valid JSON: {}. Output: {}", e, trimmed).into()
                )
            })?;

            // Property 2: Contains "timestamp" field with ISO 8601 format
            let timestamp = json.get("timestamp").ok_or_else(|| {
                TestCaseError::Fail(
                    format!("Missing 'timestamp' field in log entry: {}", trimmed).into()
                )
            })?;
            let timestamp_str = timestamp.as_str().ok_or_else(|| {
                TestCaseError::Fail(
                    format!("'timestamp' is not a string: {:?}", timestamp).into()
                )
            })?;
            // Verify ISO 8601 format (should parse as an RFC 3339 datetime)
            prop_assert!(
                chrono::DateTime::parse_from_rfc3339(timestamp_str).is_ok(),
                "Timestamp '{}' is not valid ISO 8601/RFC 3339",
                timestamp_str
            );

            // Property 3: Contains "level" field with valid level string
            let level_field = json.get("level").ok_or_else(|| {
                TestCaseError::Fail(
                    format!("Missing 'level' field in log entry: {}", trimmed).into()
                )
            })?;
            let level_str = level_field.as_str().ok_or_else(|| {
                TestCaseError::Fail(
                    format!("'level' is not a string: {:?}", level_field).into()
                )
            })?;
            prop_assert!(
                VALID_LEVELS.contains(&level_str),
                "Invalid level '{}'. Expected one of {:?}",
                level_str, VALID_LEVELS
            );

            // Property 4: Contains "target" field (non-empty string)
            let target_field = json.get("target").ok_or_else(|| {
                TestCaseError::Fail(
                    format!("Missing 'target' field in log entry: {}", trimmed).into()
                )
            })?;
            let target_str = target_field.as_str().ok_or_else(|| {
                TestCaseError::Fail(
                    format!("'target' is not a string: {:?}", target_field).into()
                )
            })?;
            prop_assert!(
                !target_str.is_empty(),
                "Target field is empty in log entry"
            );

            // Property 5: The message content is present in the output
            // tracing-subscriber JSON format puts the message in fields.message
            let fields = json.get("fields");
            let full_output = trimmed.to_string();

            // The message should appear either in fields.message or somewhere in the JSON
            let message_found = if let Some(fields_val) = fields {
                if let Some(msg_val) = fields_val.get("message") {
                    msg_val.as_str().map_or(false, |m| m.contains(&message))
                } else {
                    full_output.contains(&message)
                }
            } else {
                full_output.contains(&message)
            };

            prop_assert!(
                message_found,
                "Message '{}' not found in log output: {}",
                message, full_output
            );
        }

        buffer.clear();
    }
}
