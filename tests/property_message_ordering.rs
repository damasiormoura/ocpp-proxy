//! Property-based tests for message ordering preservation.
//!
//! **Property 2: Message ordering is preserved per direction**
//!
//! For any sequence of OCPP messages received on a single direction of the connection,
//! the forwarded messages SHALL appear in the exact same order as they were received.
//!
//! **Validates: Requirements 3.4**

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use proptest::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use ocpp_proxy::error::ProxyError;
use ocpp_proxy::forwarder::{MessageForwarder, MessageSink};
use ocpp_proxy::models::{OcppFrame, OcppMessageType};

/// A test sink that records all sent messages in order.
struct RecordingSink {
    messages: Arc<Mutex<Vec<String>>>,
}

impl RecordingSink {
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

#[async_trait]
impl MessageSink for RecordingSink {
    async fn send_raw(&mut self, raw: &str) -> Result<(), ProxyError> {
        self.messages.lock().unwrap().push(raw.to_string());
        Ok(())
    }
}

/// Generate a sequence of OcppFrames with unique IDs like "msg-0", "msg-1", etc.
/// Each frame's raw JSON includes the sequence number for verification.
fn arb_frame_sequence(max_len: usize) -> impl Strategy<Value = Vec<OcppFrame>> {
    (1..=max_len).prop_flat_map(|len| {
        prop::collection::vec(arb_action(), len).prop_map(move |actions| {
            actions
                .into_iter()
                .enumerate()
                .map(|(i, action)| {
                    let unique_id = format!("msg-{}", i);
                    let raw = format!(
                        r#"[2, "{}", "{}", {{"seq": {}}}]"#,
                        unique_id, action, i
                    );
                    OcppFrame {
                        raw,
                        message_type: OcppMessageType::Call { action },
                        unique_id,
                        received_at: Utc::now(),
                    }
                })
                .collect::<Vec<_>>()
        })
    })
}

/// Generate a random OCPP action name (PascalCase-like).
fn arb_action() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Z][a-zA-Z0-9]{2,20}").unwrap()
}

proptest! {
    /// **Property 2: Message ordering is preserved per direction (forward_upstream)**
    ///
    /// **Validates: Requirements 3.4**
    ///
    /// For any sequence of 1–100 OcppFrames forwarded via forward_upstream,
    /// the output messages appear in exactly the same order as the input.
    #[test]
    fn forward_upstream_preserves_order(frames in arb_frame_sequence(100)) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (mqtt_tx, _mqtt_rx) = mpsc::channel(1000);
            let mut forwarder = MessageForwarder::new(
                mqtt_tx,
                100,
                Duration::from_secs(30),
                Duration::from_secs(300),
            );

            let (mut sink, recorded) = RecordingSink::new();

            for frame in &frames {
                forwarder
                    .forward_upstream(frame.clone(), &mut sink)
                    .await
                    .expect("forwarding should succeed");
            }

            let messages = recorded.lock().unwrap();
            prop_assert_eq!(messages.len(), frames.len(),
                "Number of forwarded messages must equal number of input frames");

            for (i, (sent, original)) in messages.iter().zip(frames.iter()).enumerate() {
                prop_assert_eq!(
                    sent, &original.raw,
                    "Message at position {} must match input order.\n  Expected: {}\n  Got: {}",
                    i, original.raw, sent
                );
            }

            Ok(())
        })?;
    }

    /// **Property 2: Message ordering is preserved per direction (forward_downstream)**
    ///
    /// **Validates: Requirements 3.4**
    ///
    /// For any sequence of 1–100 OcppFrames forwarded via forward_downstream,
    /// the output messages appear in exactly the same order as the input.
    #[test]
    fn forward_downstream_preserves_order(frames in arb_frame_sequence(100)) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (mqtt_tx, _mqtt_rx) = mpsc::channel(1000);
            let mut forwarder = MessageForwarder::new(
                mqtt_tx,
                100,
                Duration::from_secs(30),
                Duration::from_secs(300),
            );

            let (mut sink, recorded) = RecordingSink::new();

            for frame in &frames {
                forwarder
                    .forward_downstream(frame.clone(), &mut sink)
                    .await
                    .expect("forwarding should succeed");
            }

            let messages = recorded.lock().unwrap();
            prop_assert_eq!(messages.len(), frames.len(),
                "Number of forwarded messages must equal number of input frames");

            for (i, (sent, original)) in messages.iter().zip(frames.iter()).enumerate() {
                prop_assert_eq!(
                    sent, &original.raw,
                    "Message at position {} must match input order.\n  Expected: {}\n  Got: {}",
                    i, original.raw, sent
                );
            }

            Ok(())
        })?;
    }

    /// **Property 2: Buffer flush preserves FIFO ordering (upstream)**
    ///
    /// **Validates: Requirements 3.4**
    ///
    /// When messages are buffered (because destination is disconnected) and then
    /// flushed, they must be delivered in FIFO order — the same order they were buffered.
    #[test]
    fn buffer_flush_upstream_preserves_fifo_order(frames in arb_frame_sequence(100)) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (mqtt_tx, _mqtt_rx) = mpsc::channel(1000);
            let mut forwarder = MessageForwarder::new(
                mqtt_tx,
                100,
                Duration::from_secs(30),
                Duration::from_secs(300),
            );

            // Buffer all messages (simulating upstream disconnected)
            for frame in &frames {
                forwarder.buffer_upstream(frame.clone());
            }

            // Now flush the buffer through a recording sink
            let (mut sink, recorded) = RecordingSink::new();
            let flushed = forwarder
                .flush_upstream(&mut sink)
                .await
                .expect("flush should succeed");

            prop_assert_eq!(flushed, frames.len(),
                "Flush count must equal number of buffered frames");

            let messages = recorded.lock().unwrap();
            prop_assert_eq!(messages.len(), frames.len(),
                "Number of flushed messages must equal number of buffered frames");

            for (i, (sent, original)) in messages.iter().zip(frames.iter()).enumerate() {
                prop_assert_eq!(
                    sent, &original.raw,
                    "Flushed message at position {} must match buffer input order (FIFO).\n  Expected: {}\n  Got: {}",
                    i, original.raw, sent
                );
            }

            Ok(())
        })?;
    }

    /// **Property 2: Buffer flush preserves FIFO ordering (downstream)**
    ///
    /// **Validates: Requirements 3.4**
    ///
    /// When messages are buffered for downstream and then flushed,
    /// they must be delivered in FIFO order.
    #[test]
    fn buffer_flush_downstream_preserves_fifo_order(frames in arb_frame_sequence(100)) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (mqtt_tx, _mqtt_rx) = mpsc::channel(1000);
            let mut forwarder = MessageForwarder::new(
                mqtt_tx,
                100,
                Duration::from_secs(30),
                Duration::from_secs(300),
            );

            // Buffer all messages (simulating downstream disconnected)
            for frame in &frames {
                forwarder.buffer_downstream(frame.clone());
            }

            // Now flush the buffer through a recording sink
            let (mut sink, recorded) = RecordingSink::new();
            let flushed = forwarder
                .flush_downstream(&mut sink)
                .await
                .expect("flush should succeed");

            prop_assert_eq!(flushed, frames.len(),
                "Flush count must equal number of buffered frames");

            let messages = recorded.lock().unwrap();
            prop_assert_eq!(messages.len(), frames.len(),
                "Number of flushed messages must equal number of buffered frames");

            for (i, (sent, original)) in messages.iter().zip(frames.iter()).enumerate() {
                prop_assert_eq!(
                    sent, &original.raw,
                    "Flushed message at position {} must match buffer input order (FIFO).\n  Expected: {}\n  Got: {}",
                    i, original.raw, sent
                );
            }

            Ok(())
        })?;
    }
}
