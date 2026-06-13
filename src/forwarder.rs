//! Message forwarder with buffering and ordering guarantees.
//!
//! Routes OCPP messages between downstream and upstream, preserving byte-for-byte payload
//! integrity and FIFO ordering per direction. This is the priority path — forwarding
//! completes before any MQTT publishing is initiated.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::error::ProxyError;
use crate::models::{ConnectionState, Direction, OcppFrame, OcppMessageType};

/// Event sent to the MQTT publisher task.
#[derive(Debug, Clone)]
pub enum MqttEvent {
    /// An OCPP message was successfully forwarded.
    MessageForwarded {
        /// The complete OCPP frame (including raw JSON).
        frame: OcppFrame,
        /// Direction the message was forwarded in.
        direction: Direction,
        /// Resolved action name. For CallResult/CallError, this is the originating Call's action.
        action: String,
    },
    /// Connection state changed.
    StateChange {
        /// Current upstream connection state.
        upstream: ConnectionState,
        /// Current downstream connection state.
        downstream: ConnectionState,
    },
}

/// A pending Call message awaiting its CallResult or CallError response.
#[derive(Debug, Clone)]
pub struct PendingCall {
    /// The OCPP action name from the Call message (e.g., "BootNotification").
    pub action: String,
    /// Direction in which the Call was forwarded.
    pub direction: Direction,
    /// Timestamp when the Call was tracked.
    pub sent_at: DateTime<Utc>,
}

/// Tracks in-flight Call messages to correlate CallResult/CallError with their originating action.
#[derive(Debug)]
pub struct CallTracker {
    /// Maps UniqueId → PendingCall.
    pending_calls: HashMap<String, PendingCall>,
    /// Maximum age before entries are evicted during cleanup.
    max_age: Duration,
}

impl CallTracker {
    /// Create a new CallTracker with the given maximum entry age.
    pub fn new(max_age: Duration) -> Self {
        Self {
            pending_calls: HashMap::new(),
            max_age,
        }
    }

    /// Track a Call message by storing its UniqueId → (action, direction, timestamp).
    pub fn track_call(&mut self, unique_id: &str, action: &str, direction: Direction) {
        self.pending_calls.insert(
            unique_id.to_string(),
            PendingCall {
                action: action.to_string(),
                direction,
                sent_at: Utc::now(),
            },
        );
        debug!(
            unique_id = unique_id,
            action = action,
            "Tracked Call message"
        );
    }

    /// Resolve the action for a CallResult or CallError by looking up the UniqueId.
    ///
    /// If found, the entry is removed from the tracker (consumed).
    /// Returns the action string if the Call was tracked, or None if unknown.
    pub fn resolve(&mut self, unique_id: &str) -> Option<String> {
        self.pending_calls
            .remove(unique_id)
            .map(|pending| pending.action)
    }

    /// Remove entries older than `max_age` to prevent unbounded memory growth.
    ///
    /// Returns the number of evicted entries.
    pub fn cleanup_expired(&mut self) -> usize {
        let now = Utc::now();
        let max_age_chrono = chrono::Duration::from_std(self.max_age)
            .unwrap_or_else(|_| chrono::Duration::seconds(300));

        let before = self.pending_calls.len();
        self.pending_calls.retain(|unique_id, pending| {
            let age = now - pending.sent_at;
            let keep = age < max_age_chrono;
            if !keep {
                warn!(
                    unique_id = unique_id,
                    action = %pending.action,
                    age_secs = age.num_seconds(),
                    "Evicting expired Call tracker entry"
                );
            }
            keep
        });

        before - self.pending_calls.len()
    }

    /// Returns the number of currently tracked pending calls.
    pub fn pending_count(&self) -> usize {
        self.pending_calls.len()
    }
}

/// Abstraction for sending raw WebSocket messages to a destination.
///
/// The forwarder sends the raw JSON bytes through this trait rather than
/// depending on concrete WebSocket types directly.
#[async_trait::async_trait]
pub trait MessageSink: Send {
    /// Send a raw message string to the destination.
    async fn send_raw(&mut self, raw: &str) -> Result<(), ProxyError>;
}

/// An mpsc-based MessageSink for use in production and testing.
///
/// Messages are sent through a channel that the upstream/downstream handlers consume.
pub struct ChannelSink {
    tx: mpsc::Sender<String>,
}

impl ChannelSink {
    /// Create a new ChannelSink wrapping the given sender.
    pub fn new(tx: mpsc::Sender<String>) -> Self {
        Self { tx }
    }
}

#[async_trait::async_trait]
impl MessageSink for ChannelSink {
    async fn send_raw(&mut self, raw: &str) -> Result<(), ProxyError> {
        self.tx
            .send(raw.to_string())
            .await
            .map_err(|_| ProxyError::Forwarding {
                description: "Destination channel closed".to_string(),
            })
    }
}

/// The latency threshold above which a WARNING is logged.
const FORWARDING_LATENCY_WARNING_MS: u128 = 500;

/// Message forwarder — the priority path for OCPP message routing.
///
/// Orchestrates forwarding between downstream (charger) and upstream (central system),
/// maintaining FIFO order per direction, byte-for-byte payload preservation, and
/// call tracking for correlating responses with their originating requests.
pub struct MessageForwarder {
    /// Buffer for messages destined upstream when the connection is unavailable.
    pub upstream_buffer: VecDeque<OcppFrame>,
    /// Buffer for messages destined downstream when the connection is unavailable.
    pub downstream_buffer: VecDeque<OcppFrame>,
    /// Maximum number of messages to buffer per direction.
    pub max_buffer_size: usize,
    /// Maximum duration to retain buffered messages.
    pub max_buffer_duration: Duration,
    /// Channel to send events to the MQTT publisher (non-blocking, async).
    mqtt_tx: mpsc::Sender<MqttEvent>,
    /// Tracks Call messages to resolve actions for CallResult/CallError.
    call_tracker: CallTracker,
}

impl MessageForwarder {
    /// Create a new MessageForwarder.
    ///
    /// # Arguments
    /// * `mqtt_tx` - Channel sender for MQTT events (async, non-blocking)
    /// * `max_buffer_size` - Maximum messages to buffer per direction (default: 100)
    /// * `max_buffer_duration` - Maximum time to keep buffered messages (default: 30s)
    /// * `call_tracker_max_age` - Maximum age for call tracker entries (default: 5 minutes)
    pub fn new(
        mqtt_tx: mpsc::Sender<MqttEvent>,
        max_buffer_size: usize,
        max_buffer_duration: Duration,
        call_tracker_max_age: Duration,
    ) -> Self {
        Self {
            upstream_buffer: VecDeque::new(),
            downstream_buffer: VecDeque::new(),
            max_buffer_size,
            max_buffer_duration,
            mqtt_tx,
            call_tracker: CallTracker::new(call_tracker_max_age),
        }
    }

    /// Forward a message from the charger to the central system (upstream).
    ///
    /// 1. Sends `frame.raw` byte-for-byte through the sink
    /// 2. Tracks Call messages in the CallTracker
    /// 3. After successful forwarding, sends an MQTT event
    /// 4. Logs WARNING if forwarding latency exceeds 500ms
    pub async fn forward_upstream(
        &mut self,
        frame: OcppFrame,
        sink: &mut dyn MessageSink,
    ) -> Result<(), ProxyError> {
        self.forward_message(frame, sink, Direction::ChargerToCentral)
            .await
    }

    /// Forward a message from the central system to the charger (downstream).
    ///
    /// 1. Sends `frame.raw` byte-for-byte through the sink
    /// 2. Tracks Call messages in the CallTracker
    /// 3. After successful forwarding, sends an MQTT event
    /// 4. Logs WARNING if forwarding latency exceeds 500ms
    pub async fn forward_downstream(
        &mut self,
        frame: OcppFrame,
        sink: &mut dyn MessageSink,
    ) -> Result<(), ProxyError> {
        self.forward_message(frame, sink, Direction::CentralToCharger)
            .await
    }

    /// Internal forwarding logic shared by both directions.
    async fn forward_message(
        &mut self,
        frame: OcppFrame,
        sink: &mut dyn MessageSink,
        direction: Direction,
    ) -> Result<(), ProxyError> {
        let start = Instant::now();
        let unique_id = frame.unique_id.clone();

        // Step 1: Forward the raw message byte-for-byte (PRIORITY PATH)
        sink.send_raw(&frame.raw).await?;

        // Step 2: Measure forwarding latency and log if threshold exceeded
        let latency = start.elapsed();
        if latency.as_millis() > FORWARDING_LATENCY_WARNING_MS {
            warn!(
                unique_id = %unique_id,
                latency_ms = latency.as_millis(),
                direction = ?direction,
                "Forwarding latency exceeded 500ms threshold"
            );
        }

        // Step 3: Track Call messages or resolve CallResult/CallError actions
        let action = self.resolve_action(&frame, direction);

        // Step 4: Send MQTT event AFTER successful forwarding (non-blocking)
        let mqtt_event = MqttEvent::MessageForwarded {
            frame,
            direction,
            action,
        };

        // Use try_send or send — if the MQTT channel is full, we don't block forwarding
        if let Err(e) = self.mqtt_tx.try_send(mqtt_event) {
            warn!(
                unique_id = %unique_id,
                error = %e,
                "Failed to send MQTT event (channel full or closed); forwarding unaffected"
            );
        }

        Ok(())
    }

    /// Resolve the action name for a frame, tracking or looking up as needed.
    ///
    /// - For Call messages: stores the action in the tracker and returns it.
    /// - For CallResult/CallError: looks up the originating Call's action.
    /// - If no originating Call is found, returns "Unknown".
    fn resolve_action(&mut self, frame: &OcppFrame, direction: Direction) -> String {
        match &frame.message_type {
            OcppMessageType::Call { action } => {
                self.call_tracker
                    .track_call(&frame.unique_id, action, direction);
                action.clone()
            }
            OcppMessageType::CallResult | OcppMessageType::CallError => {
                self.call_tracker
                    .resolve(&frame.unique_id)
                    .unwrap_or_else(|| {
                        warn!(
                            unique_id = %frame.unique_id,
                            "No originating Call found for response; using 'Unknown'"
                        );
                        "Unknown".to_string()
                    })
            }
        }
    }

    /// Buffer a message destined for upstream when the upstream connection is unavailable.
    ///
    /// If the buffer exceeds `max_buffer_size`, the oldest message is evicted (FIFO)
    /// and a WARNING is logged with the evicted message's unique ID.
    pub fn buffer_upstream(&mut self, frame: OcppFrame) {
        if self.upstream_buffer.len() >= self.max_buffer_size {
            if let Some(evicted) = self.upstream_buffer.pop_front() {
                warn!(
                    unique_id = %evicted.unique_id,
                    buffer = "upstream",
                    buffer_size = self.max_buffer_size,
                    "Buffer full: evicting oldest message"
                );
            }
        }
        self.upstream_buffer.push_back(frame);
    }

    /// Buffer a message destined for downstream when the downstream connection is unavailable.
    ///
    /// If the buffer exceeds `max_buffer_size`, the oldest message is evicted (FIFO)
    /// and a WARNING is logged with the evicted message's unique ID.
    pub fn buffer_downstream(&mut self, frame: OcppFrame) {
        if self.downstream_buffer.len() >= self.max_buffer_size {
            if let Some(evicted) = self.downstream_buffer.pop_front() {
                warn!(
                    unique_id = %evicted.unique_id,
                    buffer = "downstream",
                    buffer_size = self.max_buffer_size,
                    "Buffer full: evicting oldest message"
                );
            }
        }
        self.downstream_buffer.push_back(frame);
    }

    /// Flush all buffered upstream messages in FIFO order through the provided sink.
    ///
    /// Returns the number of messages flushed. The buffer is cleared after flushing.
    /// If a send fails, remaining messages stay in the buffer and the error is returned.
    pub async fn flush_upstream(
        &mut self,
        sink: &mut dyn MessageSink,
    ) -> Result<usize, ProxyError> {
        let count = self.upstream_buffer.len();
        while let Some(frame) = self.upstream_buffer.pop_front() {
            if let Err(e) = sink.send_raw(&frame.raw).await {
                // Put the frame back at the front and return error
                self.upstream_buffer.push_front(frame);
                return Err(e);
            }
        }
        Ok(count)
    }

    /// Flush all buffered downstream messages in FIFO order through the provided sink.
    ///
    /// Returns the number of messages flushed. The buffer is cleared after flushing.
    /// If a send fails, remaining messages stay in the buffer and the error is returned.
    pub async fn flush_downstream(
        &mut self,
        sink: &mut dyn MessageSink,
    ) -> Result<usize, ProxyError> {
        let count = self.downstream_buffer.len();
        while let Some(frame) = self.downstream_buffer.pop_front() {
            if let Err(e) = sink.send_raw(&frame.raw).await {
                // Put the frame back at the front and return error
                self.downstream_buffer.push_front(frame);
                return Err(e);
            }
        }
        Ok(count)
    }

    /// Discard the entire downstream buffer.
    ///
    /// Called when the downstream connection is lost — central-to-charger messages
    /// are no longer deliverable. Logs the count of discarded messages at WARNING.
    pub fn discard_downstream_buffer(&mut self) {
        let count = self.downstream_buffer.len();
        if count > 0 {
            warn!(
                discarded_count = count,
                buffer = "downstream",
                "Downstream connection lost: discarding buffered central-to-charger messages"
            );
            self.downstream_buffer.clear();
        }
    }

    /// Remove messages older than `max_buffer_duration` from both buffers.
    ///
    /// Messages with `received_at` older than `now - max_buffer_duration` are evicted.
    /// Returns the total number of evicted messages across both buffers.
    pub fn evict_expired_messages(&mut self) -> usize {
        let now = Utc::now();
        let max_age = chrono::Duration::from_std(self.max_buffer_duration)
            .unwrap_or_else(|_| chrono::Duration::seconds(30));
        let cutoff = now - max_age;

        let upstream_before = self.upstream_buffer.len();
        self.upstream_buffer.retain(|frame| {
            let keep = frame.received_at >= cutoff;
            if !keep {
                warn!(
                    unique_id = %frame.unique_id,
                    buffer = "upstream",
                    age_secs = (now - frame.received_at).num_seconds(),
                    "Evicting expired buffered message"
                );
            }
            keep
        });
        let upstream_evicted = upstream_before - self.upstream_buffer.len();

        let downstream_before = self.downstream_buffer.len();
        self.downstream_buffer.retain(|frame| {
            let keep = frame.received_at >= cutoff;
            if !keep {
                warn!(
                    unique_id = %frame.unique_id,
                    buffer = "downstream",
                    age_secs = (now - frame.received_at).num_seconds(),
                    "Evicting expired buffered message"
                );
            }
            keep
        });
        let downstream_evicted = downstream_before - self.downstream_buffer.len();

        upstream_evicted + downstream_evicted
    }

    /// Run periodic cleanup of expired call tracker entries.
    pub fn cleanup_expired_calls(&mut self) -> usize {
        self.call_tracker.cleanup_expired()
    }

    /// Get the number of pending calls being tracked.
    pub fn pending_calls_count(&self) -> usize {
        self.call_tracker.pending_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::sync::{Arc, Mutex};

    /// A test sink that records all sent messages.
    struct TestSink {
        messages: Arc<Mutex<Vec<String>>>,
    }

    impl TestSink {
        fn new() -> (Self, Arc<Mutex<Vec<String>>>) {
            let messages = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    messages: messages.clone(),
                },
                messages,
            )
        }
    }

    #[async_trait::async_trait]
    impl MessageSink for TestSink {
        async fn send_raw(&mut self, raw: &str) -> Result<(), ProxyError> {
            self.messages.lock().unwrap().push(raw.to_string());
            Ok(())
        }
    }

    /// A test sink that always fails.
    struct FailingSink;

    #[async_trait::async_trait]
    impl MessageSink for FailingSink {
        async fn send_raw(&mut self, _raw: &str) -> Result<(), ProxyError> {
            Err(ProxyError::Forwarding {
                description: "Simulated send failure".to_string(),
            })
        }
    }

    // --- CallTracker tests ---

    #[test]
    fn test_call_tracker_track_and_resolve() {
        let mut tracker = CallTracker::new(Duration::from_secs(300));

        tracker.track_call("msg-1", "BootNotification", Direction::ChargerToCentral);
        tracker.track_call("msg-2", "Heartbeat", Direction::ChargerToCentral);

        assert_eq!(tracker.pending_count(), 2);

        let action = tracker.resolve("msg-1");
        assert_eq!(action, Some("BootNotification".to_string()));
        assert_eq!(tracker.pending_count(), 1);

        let action = tracker.resolve("msg-2");
        assert_eq!(action, Some("Heartbeat".to_string()));
        assert_eq!(tracker.pending_count(), 0);
    }

    #[test]
    fn test_call_tracker_resolve_unknown_returns_none() {
        let mut tracker = CallTracker::new(Duration::from_secs(300));
        assert_eq!(tracker.resolve("nonexistent"), None);
    }

    #[test]
    fn test_call_tracker_resolve_consumes_entry() {
        let mut tracker = CallTracker::new(Duration::from_secs(300));
        tracker.track_call("msg-1", "StatusNotification", Direction::CentralToCharger);

        // First resolve succeeds
        assert_eq!(
            tracker.resolve("msg-1"),
            Some("StatusNotification".to_string())
        );
        // Second resolve returns None (already consumed)
        assert_eq!(tracker.resolve("msg-1"), None);
    }

    #[test]
    fn test_call_tracker_cleanup_expired() {
        let mut tracker = CallTracker::new(Duration::from_secs(1));

        // Insert an entry with a timestamp in the past
        tracker.pending_calls.insert(
            "old-msg".to_string(),
            PendingCall {
                action: "OldAction".to_string(),
                direction: Direction::ChargerToCentral,
                sent_at: Utc::now() - chrono::Duration::seconds(10),
            },
        );
        // Insert a fresh entry
        tracker.track_call("fresh-msg", "FreshAction", Direction::ChargerToCentral);

        assert_eq!(tracker.pending_count(), 2);

        let evicted = tracker.cleanup_expired();
        assert_eq!(evicted, 1);
        assert_eq!(tracker.pending_count(), 1);

        // The fresh one should still be there
        assert_eq!(
            tracker.resolve("fresh-msg"),
            Some("FreshAction".to_string())
        );
        // The old one should be gone
        assert_eq!(tracker.resolve("old-msg"), None);
    }

    #[test]
    fn test_call_tracker_overwrite_same_unique_id() {
        let mut tracker = CallTracker::new(Duration::from_secs(300));

        tracker.track_call("msg-1", "BootNotification", Direction::ChargerToCentral);
        tracker.track_call("msg-1", "Heartbeat", Direction::ChargerToCentral);

        // Should return the latest action
        assert_eq!(tracker.resolve("msg-1"), Some("Heartbeat".to_string()));
        assert_eq!(tracker.pending_count(), 0);
    }

    // --- MessageForwarder tests ---

    fn make_call_frame(unique_id: &str, action: &str) -> OcppFrame {
        OcppFrame {
            raw: format!(
                r#"[2, "{}", "{}", {{}}]"#,
                unique_id, action
            ),
            message_type: OcppMessageType::Call {
                action: action.to_string(),
            },
            unique_id: unique_id.to_string(),
            received_at: Utc::now(),
        }
    }

    fn make_call_result_frame(unique_id: &str) -> OcppFrame {
        OcppFrame {
            raw: format!(r#"[3, "{}", {{"status": "Accepted"}}]"#, unique_id),
            message_type: OcppMessageType::CallResult,
            unique_id: unique_id.to_string(),
            received_at: Utc::now(),
        }
    }

    fn make_call_error_frame(unique_id: &str) -> OcppFrame {
        OcppFrame {
            raw: format!(
                r#"[4, "{}", "InternalError", "Something failed", {{}}]"#,
                unique_id
            ),
            message_type: OcppMessageType::CallError,
            unique_id: unique_id.to_string(),
            received_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_forward_upstream_preserves_raw_bytes() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let raw = r#"[2,  "id-42" , "BootNotification",  {"model": "Autel"}]"#;
        let frame = OcppFrame {
            raw: raw.to_string(),
            message_type: OcppMessageType::Call {
                action: "BootNotification".to_string(),
            },
            unique_id: "id-42".to_string(),
            received_at: Utc::now(),
        };

        let (mut sink, sent) = TestSink::new();
        forwarder.forward_upstream(frame, &mut sink).await.unwrap();

        let messages = sent.lock().unwrap();
        assert_eq!(messages.len(), 1);
        // Byte-for-byte preservation
        assert_eq!(messages[0], raw);
    }

    #[tokio::test]
    async fn test_forward_downstream_preserves_raw_bytes() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let raw = r#"[3,"resp-1",{"status":"Accepted"}]"#;
        let frame = OcppFrame {
            raw: raw.to_string(),
            message_type: OcppMessageType::CallResult,
            unique_id: "resp-1".to_string(),
            received_at: Utc::now(),
        };

        let (mut sink, sent) = TestSink::new();
        forwarder
            .forward_downstream(frame, &mut sink)
            .await
            .unwrap();

        let messages = sent.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0], raw);
    }

    #[tokio::test]
    async fn test_forward_maintains_fifo_order() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let frames: Vec<OcppFrame> = (0..5)
            .map(|i| make_call_frame(&format!("msg-{}", i), "Heartbeat"))
            .collect();

        let (mut sink, sent) = TestSink::new();
        for frame in frames.iter() {
            forwarder
                .forward_upstream(frame.clone(), &mut sink)
                .await
                .unwrap();
        }

        let messages = sent.lock().unwrap();
        assert_eq!(messages.len(), 5);
        for (i, msg) in messages.iter().enumerate() {
            assert!(msg.contains(&format!("msg-{}", i)));
        }
    }

    #[tokio::test]
    async fn test_forward_emits_mqtt_event_after_forwarding() {
        let (mqtt_tx, mut mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let frame = make_call_frame("evt-1", "StatusNotification");
        let (mut sink, _sent) = TestSink::new();

        forwarder.forward_upstream(frame, &mut sink).await.unwrap();

        // Should have received an MQTT event
        let event = mqtt_rx.try_recv().unwrap();
        match event {
            MqttEvent::MessageForwarded {
                frame,
                direction,
                action,
            } => {
                assert_eq!(frame.unique_id, "evt-1");
                assert_eq!(direction, Direction::ChargerToCentral);
                assert_eq!(action, "StatusNotification");
            }
            _ => panic!("Expected MessageForwarded event"),
        }
    }

    #[tokio::test]
    async fn test_call_tracking_resolves_call_result() {
        let (mqtt_tx, mut mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let (mut sink, _sent) = TestSink::new();

        // Forward a Call upstream
        let call = make_call_frame("track-1", "RemoteStartTransaction");
        forwarder.forward_upstream(call, &mut sink).await.unwrap();

        // Now forward the CallResult downstream
        let result = make_call_result_frame("track-1");
        forwarder
            .forward_downstream(result, &mut sink)
            .await
            .unwrap();

        // Drain the first MQTT event (the Call)
        let _call_event = mqtt_rx.try_recv().unwrap();

        // The CallResult event should have the resolved action
        let result_event = mqtt_rx.try_recv().unwrap();
        match result_event {
            MqttEvent::MessageForwarded { action, .. } => {
                assert_eq!(action, "RemoteStartTransaction");
            }
            _ => panic!("Expected MessageForwarded event"),
        }
    }

    #[tokio::test]
    async fn test_call_tracking_resolves_call_error() {
        let (mqtt_tx, mut mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let (mut sink, _sent) = TestSink::new();

        // Forward a Call
        let call = make_call_frame("err-1", "Authorize");
        forwarder
            .forward_downstream(call, &mut sink)
            .await
            .unwrap();

        // Forward the CallError
        let error = make_call_error_frame("err-1");
        forwarder.forward_upstream(error, &mut sink).await.unwrap();

        // Drain call event
        let _call_event = mqtt_rx.try_recv().unwrap();

        // The CallError event should have resolved action
        let error_event = mqtt_rx.try_recv().unwrap();
        match error_event {
            MqttEvent::MessageForwarded { action, .. } => {
                assert_eq!(action, "Authorize");
            }
            _ => panic!("Expected MessageForwarded event"),
        }
    }

    #[tokio::test]
    async fn test_untracked_response_uses_unknown_action() {
        let (mqtt_tx, mut mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let (mut sink, _sent) = TestSink::new();

        // Forward a CallResult without a preceding Call
        let result = make_call_result_frame("orphan-1");
        forwarder
            .forward_downstream(result, &mut sink)
            .await
            .unwrap();

        let event = mqtt_rx.try_recv().unwrap();
        match event {
            MqttEvent::MessageForwarded { action, .. } => {
                assert_eq!(action, "Unknown");
            }
            _ => panic!("Expected MessageForwarded event"),
        }
    }

    #[tokio::test]
    async fn test_forwarding_failure_does_not_emit_mqtt_event() {
        let (mqtt_tx, mut mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let frame = make_call_frame("fail-1", "Heartbeat");
        let mut sink = FailingSink;

        let result = forwarder.forward_upstream(frame, &mut sink).await;
        assert!(result.is_err());

        // No MQTT event should have been sent since forwarding failed
        assert!(mqtt_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_mqtt_channel_full_does_not_block_forwarding() {
        // Create a channel with capacity 1
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(1);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let (mut sink, sent) = TestSink::new();

        // Send multiple messages — the MQTT channel will fill up but forwarding should continue
        for i in 0..5 {
            let frame = make_call_frame(&format!("full-{}", i), "Heartbeat");
            forwarder.forward_upstream(frame, &mut sink).await.unwrap();
        }

        // All 5 messages should have been forwarded regardless of MQTT backpressure
        let messages = sent.lock().unwrap();
        assert_eq!(messages.len(), 5);
    }

    // --- Buffer tests ---

    fn make_frame_with_timestamp(unique_id: &str, received_at: DateTime<Utc>) -> OcppFrame {
        OcppFrame {
            raw: format!(r#"[2, "{}", "Heartbeat", {{}}]"#, unique_id),
            message_type: OcppMessageType::Call {
                action: "Heartbeat".to_string(),
            },
            unique_id: unique_id.to_string(),
            received_at,
        }
    }

    #[test]
    fn test_buffer_upstream_adds_messages() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        for i in 0..5 {
            let frame = make_call_frame(&format!("buf-{}", i), "Heartbeat");
            forwarder.buffer_upstream(frame);
        }

        assert_eq!(forwarder.upstream_buffer.len(), 5);
    }

    #[test]
    fn test_buffer_downstream_adds_messages() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        for i in 0..5 {
            let frame = make_call_frame(&format!("buf-{}", i), "Heartbeat");
            forwarder.buffer_downstream(frame);
        }

        assert_eq!(forwarder.downstream_buffer.len(), 5);
    }

    #[test]
    fn test_buffer_upstream_evicts_oldest_when_full() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            5, // small buffer for testing
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        // Fill the buffer
        for i in 0..5 {
            let frame = make_call_frame(&format!("msg-{}", i), "Heartbeat");
            forwarder.buffer_upstream(frame);
        }
        assert_eq!(forwarder.upstream_buffer.len(), 5);

        // Adding one more should evict the oldest (msg-0)
        let frame = make_call_frame("msg-5", "Heartbeat");
        forwarder.buffer_upstream(frame);

        assert_eq!(forwarder.upstream_buffer.len(), 5);
        // The oldest (msg-0) should be gone
        assert_eq!(forwarder.upstream_buffer[0].unique_id, "msg-1");
        // The newest should be at the back
        assert_eq!(forwarder.upstream_buffer[4].unique_id, "msg-5");
    }

    #[test]
    fn test_buffer_downstream_evicts_oldest_when_full() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            3, // small buffer for testing
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        // Fill and overflow
        for i in 0..6 {
            let frame = make_call_frame(&format!("msg-{}", i), "Heartbeat");
            forwarder.buffer_downstream(frame);
        }

        // Buffer should be at capacity
        assert_eq!(forwarder.downstream_buffer.len(), 3);
        // Should contain the 3 newest messages
        assert_eq!(forwarder.downstream_buffer[0].unique_id, "msg-3");
        assert_eq!(forwarder.downstream_buffer[1].unique_id, "msg-4");
        assert_eq!(forwarder.downstream_buffer[2].unique_id, "msg-5");
    }

    #[tokio::test]
    async fn test_flush_upstream_delivers_in_fifo_order() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        // Buffer messages
        for i in 0..5 {
            let frame = make_call_frame(&format!("flush-{}", i), "Heartbeat");
            forwarder.buffer_upstream(frame);
        }

        // Flush through test sink
        let (mut sink, sent) = TestSink::new();
        let count = forwarder.flush_upstream(&mut sink).await.unwrap();

        assert_eq!(count, 5);
        assert_eq!(forwarder.upstream_buffer.len(), 0);

        let messages = sent.lock().unwrap();
        assert_eq!(messages.len(), 5);
        // Verify FIFO order
        for i in 0..5 {
            assert!(messages[i].contains(&format!("flush-{}", i)));
        }
    }

    #[tokio::test]
    async fn test_flush_downstream_delivers_in_fifo_order() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        // Buffer messages
        for i in 0..3 {
            let frame = make_call_frame(&format!("ds-{}", i), "StatusNotification");
            forwarder.buffer_downstream(frame);
        }

        // Flush through test sink
        let (mut sink, sent) = TestSink::new();
        let count = forwarder.flush_downstream(&mut sink).await.unwrap();

        assert_eq!(count, 3);
        assert_eq!(forwarder.downstream_buffer.len(), 0);

        let messages = sent.lock().unwrap();
        assert_eq!(messages.len(), 3);
        for i in 0..3 {
            assert!(messages[i].contains(&format!("ds-{}", i)));
        }
    }

    #[tokio::test]
    async fn test_flush_upstream_on_empty_buffer_returns_zero() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let (mut sink, sent) = TestSink::new();
        let count = forwarder.flush_upstream(&mut sink).await.unwrap();

        assert_eq!(count, 0);
        let messages = sent.lock().unwrap();
        assert_eq!(messages.len(), 0);
    }

    #[tokio::test]
    async fn test_flush_upstream_stops_on_send_failure() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        // Buffer 3 messages
        for i in 0..3 {
            let frame = make_call_frame(&format!("fail-flush-{}", i), "Heartbeat");
            forwarder.buffer_upstream(frame);
        }

        // Flush through a failing sink — first message fails
        let mut sink = FailingSink;
        let result = forwarder.flush_upstream(&mut sink).await;

        assert!(result.is_err());
        // The failed message should be put back, so buffer should still have 3 messages
        assert_eq!(forwarder.upstream_buffer.len(), 3);
        assert_eq!(forwarder.upstream_buffer[0].unique_id, "fail-flush-0");
    }

    #[test]
    fn test_discard_downstream_buffer() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        // Buffer messages
        for i in 0..10 {
            let frame = make_call_frame(&format!("discard-{}", i), "Heartbeat");
            forwarder.buffer_downstream(frame);
        }

        assert_eq!(forwarder.downstream_buffer.len(), 10);

        // Discard all
        forwarder.discard_downstream_buffer();

        assert_eq!(forwarder.downstream_buffer.len(), 0);
    }

    #[test]
    fn test_discard_downstream_buffer_empty_is_noop() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        // Discarding an empty buffer should not panic
        forwarder.discard_downstream_buffer();
        assert_eq!(forwarder.downstream_buffer.len(), 0);
    }

    #[test]
    fn test_evict_expired_messages_removes_old_from_both_buffers() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30), // 30 second max duration
            Duration::from_secs(300),
        );

        let now = Utc::now();
        let old_time = now - chrono::Duration::seconds(60); // 60 seconds ago (expired)
        let fresh_time = now - chrono::Duration::seconds(5); // 5 seconds ago (fresh)

        // Add old and fresh messages to upstream
        forwarder
            .upstream_buffer
            .push_back(make_frame_with_timestamp("old-up-1", old_time));
        forwarder
            .upstream_buffer
            .push_back(make_frame_with_timestamp("old-up-2", old_time));
        forwarder
            .upstream_buffer
            .push_back(make_frame_with_timestamp("fresh-up-1", fresh_time));

        // Add old and fresh messages to downstream
        forwarder
            .downstream_buffer
            .push_back(make_frame_with_timestamp("old-down-1", old_time));
        forwarder
            .downstream_buffer
            .push_back(make_frame_with_timestamp("fresh-down-1", fresh_time));
        forwarder
            .downstream_buffer
            .push_back(make_frame_with_timestamp("fresh-down-2", fresh_time));

        let evicted = forwarder.evict_expired_messages();

        // Should have evicted 3 old messages (2 upstream + 1 downstream)
        assert_eq!(evicted, 3);
        assert_eq!(forwarder.upstream_buffer.len(), 1);
        assert_eq!(forwarder.upstream_buffer[0].unique_id, "fresh-up-1");
        assert_eq!(forwarder.downstream_buffer.len(), 2);
        assert_eq!(forwarder.downstream_buffer[0].unique_id, "fresh-down-1");
        assert_eq!(forwarder.downstream_buffer[1].unique_id, "fresh-down-2");
    }

    #[test]
    fn test_evict_expired_messages_keeps_all_when_none_expired() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        // Add fresh messages
        for i in 0..5 {
            let frame = make_call_frame(&format!("fresh-{}", i), "Heartbeat");
            forwarder.buffer_upstream(frame);
        }

        let evicted = forwarder.evict_expired_messages();
        assert_eq!(evicted, 0);
        assert_eq!(forwarder.upstream_buffer.len(), 5);
    }

    #[test]
    fn test_evict_expired_messages_empty_buffers() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            100,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let evicted = forwarder.evict_expired_messages();
        assert_eq!(evicted, 0);
    }

    #[test]
    fn test_buffer_never_exceeds_max_size() {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(100);
        let max_size = 10;
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            max_size,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        // Push way more messages than the buffer can hold
        for i in 0..50 {
            let frame = make_call_frame(&format!("overflow-{}", i), "Heartbeat");
            forwarder.buffer_upstream(frame);
            // Buffer should never exceed max_size
            assert!(forwarder.upstream_buffer.len() <= max_size);
        }

        // Final buffer should be exactly at max_size with the 10 newest messages
        assert_eq!(forwarder.upstream_buffer.len(), max_size);
        assert_eq!(forwarder.upstream_buffer[0].unique_id, "overflow-40");
        assert_eq!(forwarder.upstream_buffer[9].unique_id, "overflow-49");
    }
}
