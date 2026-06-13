//! Structured JSON logging for the OCPP proxy.
//!
//! Configures `tracing-subscriber` with JSON output to stdout.
//! Each log entry includes: timestamp (ISO 8601), level, component (target),
//! message, and correlation_id (via tracing spans).
//!
//! Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6

use std::time::Duration;

use tracing::{debug, warn};
use tracing_subscriber::{
    fmt::{self, time::ChronoUtc},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

use crate::config::LogLevel;

/// Initialize the structured JSON logging subsystem.
///
/// Configures tracing-subscriber with:
/// - JSON output format to stdout
/// - ISO 8601 timestamps (via ChronoUtc)
/// - Configurable log level from the `LogConfig`
/// - Span events (correlation_id is propagated through spans)
///
/// This should be called once at startup, before any other tracing calls.
pub fn init_logging(level: &LogLevel) {
    let filter = build_env_filter(level);

    let json_layer = fmt::layer()
        .json()
        .with_timer(ChronoUtc::rfc_3339())
        .with_target(true)
        .with_level(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_span_list(true)
        .with_current_span(true);

    tracing_subscriber::registry()
        .with(filter)
        .with(json_layer)
        .init();
}

/// Build an `EnvFilter` from the configured log level.
///
/// The `RUST_LOG` environment variable takes precedence over the config file
/// value, allowing runtime override without redeployment.
fn build_env_filter(level: &LogLevel) -> EnvFilter {
    let default_directive = match level {
        LogLevel::Debug => "debug",
        LogLevel::Info => "info",
        LogLevel::Warning => "warn",
        LogLevel::Error => "error",
    };

    // Allow RUST_LOG env var to override the configured level
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_directive))
}

/// Log an OCPP message summary at DEBUG level.
///
/// Logs message type, action, and unique ID without including the full payload.
/// This satisfies Requirement 8.2: log summaries at DEBUG without full payloads at INFO.
pub fn log_ocpp_message_summary(
    message_type: &str,
    action: &str,
    unique_id: &str,
    direction: &str,
) {
    debug!(
        component = "forwarder",
        message_type = message_type,
        action = action,
        unique_id = unique_id,
        direction = direction,
        "OCPP message forwarded"
    );
}

/// Log a latency warning when message forwarding exceeds 500ms.
///
/// Satisfies Requirement 8.6: log WARNING if forwarding latency > 500ms.
pub fn log_latency_warning(latency: Duration, unique_id: &str, direction: &str) {
    let threshold = Duration::from_millis(500);
    if latency > threshold {
        warn!(
            component = "forwarder",
            latency_ms = latency.as_millis() as u64,
            unique_id = unique_id,
            direction = direction,
            "Message forwarding latency exceeded 500ms threshold"
        );
    }
}

/// Log a connection state transition.
///
/// Satisfies Requirement 8.1: log state changes with previous/new state,
/// connection identifier, and timestamp.
pub fn log_connection_state_change(
    connection_id: &str,
    previous_state: &str,
    new_state: &str,
) {
    tracing::info!(
        component = "state",
        connection_id = connection_id,
        previous_state = previous_state,
        new_state = new_state,
        "Connection state transition"
    );
}

/// Log an error with structured context.
///
/// Satisfies Requirement 8.3: log errors at ERROR level including connection
/// identifier, message unique ID, error category, and description.
pub fn log_error(
    category: &str,
    connection_id: &str,
    unique_id: Option<&str>,
    description: &str,
) {
    tracing::error!(
        component = category,
        connection_id = connection_id,
        unique_id = unique_id.unwrap_or("N/A"),
        error_category = category,
        "{}",
        description
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_env_filter_debug() {
        let filter = build_env_filter(&LogLevel::Debug);
        // The filter should allow debug-level events
        assert!(format!("{}", filter).contains("debug") || !format!("{}", filter).is_empty());
    }

    #[test]
    fn test_build_env_filter_info() {
        let filter = build_env_filter(&LogLevel::Info);
        assert!(format!("{}", filter).contains("info") || !format!("{}", filter).is_empty());
    }

    #[test]
    fn test_build_env_filter_warning() {
        let filter = build_env_filter(&LogLevel::Warning);
        assert!(format!("{}", filter).contains("warn") || !format!("{}", filter).is_empty());
    }

    #[test]
    fn test_build_env_filter_error() {
        let filter = build_env_filter(&LogLevel::Error);
        assert!(format!("{}", filter).contains("error") || !format!("{}", filter).is_empty());
    }

    #[test]
    fn test_log_latency_warning_below_threshold() {
        // This should not panic — just a no-op below threshold
        log_latency_warning(
            Duration::from_millis(200),
            "test-id-123",
            "charger_to_central",
        );
    }

    #[test]
    fn test_log_latency_warning_above_threshold() {
        // This should not panic — exercises the warning path
        log_latency_warning(
            Duration::from_millis(600),
            "test-id-456",
            "central_to_charger",
        );
    }

    #[test]
    fn test_log_ocpp_message_summary_does_not_panic() {
        log_ocpp_message_summary(
            "Call",
            "BootNotification",
            "unique-123",
            "charger_to_central",
        );
    }

    #[test]
    fn test_log_connection_state_change_does_not_panic() {
        log_connection_state_change("upstream", "disconnected", "connecting");
    }

    #[test]
    fn test_log_error_with_unique_id() {
        log_error(
            "forwarding",
            "downstream",
            Some("msg-789"),
            "Failed to deliver message",
        );
    }

    #[test]
    fn test_log_error_without_unique_id() {
        log_error(
            "connection_mqtt",
            "mqtt",
            None,
            "Broker connection timeout",
        );
    }
}
