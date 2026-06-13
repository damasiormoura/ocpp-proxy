//! Property-based tests for message buffer capacity and eviction policy.
//!
//! **Property 3: Message buffer respects capacity and eviction policy**
//!
//! For any sequence of messages arriving while the destination is disconnected,
//! the buffer SHALL never contain more than `max_buffer_size` messages, and when
//! the buffer is full, the oldest message SHALL be evicted first (FIFO eviction).
//!
//! **Validates: Requirements 3.5, 4.5**

use std::time::Duration;

use chrono::Utc;
use ocpp_proxy::forwarder::MessageForwarder;
use ocpp_proxy::models::{OcppFrame, OcppMessageType};
use proptest::prelude::*;
use tokio::sync::mpsc;

/// Create a test OcppFrame with a given index for identification.
fn make_test_frame(index: usize) -> OcppFrame {
    OcppFrame {
        raw: format!(
            r#"[2, "msg-{}", "Heartbeat", {{}}]"#,
            index
        ),
        message_type: OcppMessageType::Call {
            action: "Heartbeat".to_string(),
        },
        unique_id: format!("msg-{}", index),
        received_at: Utc::now(),
    }
}

proptest! {
    /// **Property 3: Buffer never exceeds max_buffer_size (upstream direction)**
    ///
    /// Validates: Requirements 3.5, 4.5
    ///
    /// For any (max_buffer_size, message_count) pair, buffering `message_count` messages
    /// into the upstream buffer must never cause the buffer length to exceed max_buffer_size.
    #[test]
    fn upstream_buffer_never_exceeds_capacity(
        max_buffer_size in 1usize..=200,
        message_count in 1usize..=500,
    ) {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(1);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            max_buffer_size,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        for i in 0..message_count {
            let frame = make_test_frame(i);
            forwarder.buffer_upstream(frame);

            // Invariant: buffer length must never exceed max_buffer_size
            prop_assert!(
                forwarder.upstream_buffer.len() <= max_buffer_size,
                "Upstream buffer length {} exceeded max_buffer_size {} after inserting message {}",
                forwarder.upstream_buffer.len(),
                max_buffer_size,
                i
            );
        }
    }

    /// **Property 3: Buffer never exceeds max_buffer_size (downstream direction)**
    ///
    /// Validates: Requirements 3.5, 4.5
    ///
    /// Same invariant as above but for the downstream buffer direction.
    #[test]
    fn downstream_buffer_never_exceeds_capacity(
        max_buffer_size in 1usize..=200,
        message_count in 1usize..=500,
    ) {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(1);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            max_buffer_size,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        for i in 0..message_count {
            let frame = make_test_frame(i);
            forwarder.buffer_downstream(frame);

            // Invariant: buffer length must never exceed max_buffer_size
            prop_assert!(
                forwarder.downstream_buffer.len() <= max_buffer_size,
                "Downstream buffer length {} exceeded max_buffer_size {} after inserting message {}",
                forwarder.downstream_buffer.len(),
                max_buffer_size,
                i
            );
        }
    }

    /// **Property 3: After buffering, the buffer contains the most recent messages**
    ///
    /// Validates: Requirements 3.5, 4.5
    ///
    /// After buffering `message_count` messages, the buffer must contain exactly
    /// min(message_count, max_buffer_size) messages, and they must be the most recent ones.
    #[test]
    fn buffer_contains_most_recent_messages(
        max_buffer_size in 1usize..=200,
        message_count in 1usize..=500,
    ) {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(1);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            max_buffer_size,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        for i in 0..message_count {
            let frame = make_test_frame(i);
            forwarder.buffer_upstream(frame);
        }

        let expected_len = message_count.min(max_buffer_size);
        prop_assert_eq!(
            forwarder.upstream_buffer.len(),
            expected_len,
            "Buffer should contain min(message_count, max_buffer_size) = {} messages, got {}",
            expected_len,
            forwarder.upstream_buffer.len()
        );

        // The buffer should contain the most recent messages (indices from
        // message_count - expected_len to message_count - 1)
        let first_retained_index = message_count - expected_len;
        for (buf_pos, frame) in forwarder.upstream_buffer.iter().enumerate() {
            let expected_index = first_retained_index + buf_pos;
            let expected_uid = format!("msg-{}", expected_index);
            prop_assert_eq!(
                &frame.unique_id,
                &expected_uid,
                "At buffer position {}, expected unique_id '{}' but got '{}'",
                buf_pos,
                expected_uid,
                frame.unique_id
            );
        }
    }

    /// **Property 3: Evicted messages are the oldest (not in the buffer)**
    ///
    /// Validates: Requirements 3.5, 4.5
    ///
    /// After buffering, messages that should have been evicted (the oldest ones)
    /// must NOT be present in the buffer.
    #[test]
    fn evicted_messages_are_not_in_buffer(
        max_buffer_size in 1usize..=200,
        message_count in 1usize..=500,
    ) {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(1);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            max_buffer_size,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        for i in 0..message_count {
            let frame = make_test_frame(i);
            forwarder.buffer_downstream(frame);
        }

        // If message_count > max_buffer_size, the first (message_count - max_buffer_size)
        // messages should have been evicted
        if message_count > max_buffer_size {
            let evicted_count = message_count - max_buffer_size;
            let buffer_uids: Vec<&str> = forwarder
                .downstream_buffer
                .iter()
                .map(|f| f.unique_id.as_str())
                .collect();

            for evicted_idx in 0..evicted_count {
                let evicted_uid = format!("msg-{}", evicted_idx);
                prop_assert!(
                    !buffer_uids.contains(&evicted_uid.as_str()),
                    "Evicted message '{}' should NOT be in the buffer, but was found",
                    evicted_uid
                );
            }
        }
    }

    /// **Property 3: FIFO eviction — when buffer is full, the oldest message is evicted**
    ///
    /// Validates: Requirements 3.5, 4.5
    ///
    /// When a new message is added to a full buffer, the message at the front (oldest)
    /// is removed and the new message is placed at the back. The order of remaining
    /// messages is preserved.
    #[test]
    fn fifo_eviction_removes_oldest_first(
        max_buffer_size in 1usize..=50,
        extra_messages in 1usize..=100,
    ) {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(1);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            max_buffer_size,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        // Fill the buffer to capacity
        for i in 0..max_buffer_size {
            let frame = make_test_frame(i);
            forwarder.buffer_upstream(frame);
        }

        prop_assert_eq!(forwarder.upstream_buffer.len(), max_buffer_size);

        // Now add extra messages one at a time and verify FIFO eviction
        for extra in 0..extra_messages {
            let new_index = max_buffer_size + extra;
            let frame = make_test_frame(new_index);

            // Before adding: the front of the buffer is the oldest message still present
            let oldest_before = forwarder.upstream_buffer.front()
                .map(|f| f.unique_id.clone());

            forwarder.buffer_upstream(frame);

            // After adding: buffer size stays at max
            prop_assert_eq!(
                forwarder.upstream_buffer.len(),
                max_buffer_size,
                "Buffer size must remain at max_buffer_size after eviction"
            );

            // The oldest message (front before insert) must no longer be at the front
            // (it was evicted)
            if let Some(old_uid) = oldest_before {
                let new_front = forwarder.upstream_buffer.front()
                    .map(|f| f.unique_id.clone())
                    .unwrap_or_default();
                prop_assert_ne!(
                    new_front,
                    old_uid,
                    "After eviction, the oldest message should no longer be at the front"
                );
            }

            // The newest message must be at the back
            let back = forwarder.upstream_buffer.back().unwrap();
            let expected_uid = format!("msg-{}", new_index);
            prop_assert_eq!(
                &back.unique_id,
                &expected_uid,
                "The most recently added message should be at the back of the buffer"
            );
        }
    }

    /// **Property 3: Buffer maintains FIFO order at all times**
    ///
    /// Validates: Requirements 3.5, 4.5
    ///
    /// After any number of insertions, the messages in the buffer must be in
    /// ascending order of their unique_id indices (FIFO ordering preserved).
    #[test]
    fn buffer_maintains_fifo_ordering(
        max_buffer_size in 1usize..=200,
        message_count in 1usize..=500,
    ) {
        let (mqtt_tx, _mqtt_rx) = mpsc::channel(1);
        let mut forwarder = MessageForwarder::new(
            mqtt_tx,
            max_buffer_size,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        for i in 0..message_count {
            let frame = make_test_frame(i);
            forwarder.buffer_upstream(frame);
        }

        // Verify the buffer is in strictly ascending order of message indices
        let indices: Vec<usize> = forwarder
            .upstream_buffer
            .iter()
            .map(|f| {
                f.unique_id
                    .strip_prefix("msg-")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
            })
            .collect();

        for window in indices.windows(2) {
            prop_assert!(
                window[0] < window[1],
                "Buffer must maintain FIFO order: found msg-{} before msg-{} (not ascending)",
                window[0],
                window[1]
            );
        }
    }
}
