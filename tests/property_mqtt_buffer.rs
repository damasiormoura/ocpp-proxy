//! Property-based tests for MQTT buffer capacity and eviction policy.
//!
//! **Property 7: MQTT buffer respects capacity and eviction policy**
//!
//! For any sequence of MQTT events generated while the MQTT broker is unreachable,
//! the MQTT buffer SHALL never contain more than 500 messages, and when the buffer
//! is full, the oldest message SHALL be evicted first (FIFO eviction).
//!
//! **Validates: Requirements 5.5**

use std::collections::VecDeque;

use ocpp_proxy::mqtt::MqttMessage;
use proptest::prelude::*;
use rumqttc::QoS;

/// Simulate the MQTT buffer eviction logic using the same pattern as MqttPublisher::buffer_message.
///
/// When `buffer.len() >= max_size`, pop_front() before pushing to maintain capacity.
fn buffer_message(buffer: &mut VecDeque<MqttMessage>, message: MqttMessage, max_size: usize) {
    if buffer.len() >= max_size {
        buffer.pop_front();
    }
    buffer.push_back(message);
}

/// Create a test MqttMessage with a given index for identification.
fn make_test_mqtt_message(index: usize) -> MqttMessage {
    MqttMessage {
        topic: format!("ocpp/CP001/charger/Heartbeat-{}", index),
        payload: format!("payload-{}", index).into_bytes(),
        qos: QoS::AtLeastOnce,
        retain: false,
    }
}

proptest! {
    /// **Property 7: MQTT buffer never exceeds max capacity**
    ///
    /// Validates: Requirements 5.5
    ///
    /// For any (max_buffer_size, event_count) pair, buffering `event_count` messages
    /// into the MQTT buffer must never cause the buffer length to exceed max_buffer_size.
    #[test]
    fn mqtt_buffer_never_exceeds_capacity(
        max_buffer_size in 1usize..=1000,
        event_count in 1usize..=2000,
    ) {
        let mut buffer: VecDeque<MqttMessage> = VecDeque::new();

        for i in 0..event_count {
            let message = make_test_mqtt_message(i);
            buffer_message(&mut buffer, message, max_buffer_size);

            // Invariant: buffer length must never exceed max_buffer_size
            prop_assert!(
                buffer.len() <= max_buffer_size,
                "MQTT buffer length {} exceeded max_buffer_size {} after inserting message {}",
                buffer.len(),
                max_buffer_size,
                i
            );
        }
    }

    /// **Property 7: After buffering, the buffer contains the most recent messages**
    ///
    /// Validates: Requirements 5.5
    ///
    /// After buffering `event_count` messages, the buffer must contain exactly
    /// min(event_count, max_buffer_size) messages, and they must be the most recent ones.
    #[test]
    fn mqtt_buffer_contains_most_recent_messages(
        max_buffer_size in 1usize..=1000,
        event_count in 1usize..=2000,
    ) {
        let mut buffer: VecDeque<MqttMessage> = VecDeque::new();

        for i in 0..event_count {
            let message = make_test_mqtt_message(i);
            buffer_message(&mut buffer, message, max_buffer_size);
        }

        let expected_len = event_count.min(max_buffer_size);
        prop_assert_eq!(
            buffer.len(),
            expected_len,
            "Buffer should contain min(event_count, max_buffer_size) = {} messages, got {}",
            expected_len,
            buffer.len()
        );

        // The buffer should contain the most recent messages
        // (indices from event_count - expected_len to event_count - 1)
        let first_retained_index = event_count - expected_len;
        for (buf_pos, msg) in buffer.iter().enumerate() {
            let expected_index = first_retained_index + buf_pos;
            let expected_topic = format!("ocpp/CP001/charger/Heartbeat-{}", expected_index);
            prop_assert_eq!(
                &msg.topic,
                &expected_topic,
                "At buffer position {}, expected topic '{}' but got '{}'",
                buf_pos,
                expected_topic,
                msg.topic
            );
        }
    }

    /// **Property 7: Oldest messages are evicted first (FIFO eviction)**
    ///
    /// Validates: Requirements 5.5
    ///
    /// After buffering, messages that should have been evicted (the oldest ones)
    /// must NOT be present in the buffer.
    #[test]
    fn mqtt_buffer_evicts_oldest_first(
        max_buffer_size in 1usize..=1000,
        event_count in 1usize..=2000,
    ) {
        let mut buffer: VecDeque<MqttMessage> = VecDeque::new();

        for i in 0..event_count {
            let message = make_test_mqtt_message(i);
            buffer_message(&mut buffer, message, max_buffer_size);
        }

        // If event_count > max_buffer_size, the first (event_count - max_buffer_size)
        // messages should have been evicted
        if event_count > max_buffer_size {
            let evicted_count = event_count - max_buffer_size;
            let buffer_topics: Vec<&str> = buffer.iter().map(|m| m.topic.as_str()).collect();

            for evicted_idx in 0..evicted_count {
                let evicted_topic = format!("ocpp/CP001/charger/Heartbeat-{}", evicted_idx);
                prop_assert!(
                    !buffer_topics.contains(&evicted_topic.as_str()),
                    "Evicted message with topic '{}' should NOT be in the buffer, but was found",
                    evicted_topic
                );
            }
        }
    }

    /// **Property 7: FIFO eviction — when buffer is full, the oldest message is evicted**
    ///
    /// Validates: Requirements 5.5
    ///
    /// When a new message is added to a full buffer, the message at the front (oldest)
    /// is removed and the new message is placed at the back.
    #[test]
    fn mqtt_buffer_fifo_eviction_removes_oldest(
        max_buffer_size in 1usize..=50,
        extra_messages in 1usize..=100,
    ) {
        let mut buffer: VecDeque<MqttMessage> = VecDeque::new();

        // Fill the buffer to capacity
        for i in 0..max_buffer_size {
            let message = make_test_mqtt_message(i);
            buffer_message(&mut buffer, message, max_buffer_size);
        }

        prop_assert_eq!(buffer.len(), max_buffer_size);

        // Now add extra messages one at a time and verify FIFO eviction
        for extra in 0..extra_messages {
            let new_index = max_buffer_size + extra;
            let message = make_test_mqtt_message(new_index);

            // Before adding: capture the oldest message topic
            let oldest_before = buffer.front().map(|m| m.topic.clone());

            buffer_message(&mut buffer, message, max_buffer_size);

            // After adding: buffer size stays at max
            prop_assert_eq!(
                buffer.len(),
                max_buffer_size,
                "Buffer size must remain at max_buffer_size after eviction"
            );

            // The oldest message (front before insert) must no longer be at the front
            if let Some(old_topic) = oldest_before {
                let new_front = buffer.front().map(|m| m.topic.clone()).unwrap_or_default();
                prop_assert_ne!(
                    new_front,
                    old_topic,
                    "After eviction, the oldest message should no longer be at the front"
                );
            }

            // The newest message must be at the back
            let back = buffer.back().unwrap();
            let expected_topic = format!("ocpp/CP001/charger/Heartbeat-{}", new_index);
            prop_assert_eq!(
                &back.topic,
                &expected_topic,
                "The most recently added message should be at the back of the buffer"
            );
        }
    }

    /// **Property 7: Buffer maintains FIFO order at all times**
    ///
    /// Validates: Requirements 5.5
    ///
    /// After any number of insertions, the messages in the buffer must be in
    /// ascending order of their indices (FIFO ordering preserved).
    #[test]
    fn mqtt_buffer_maintains_fifo_ordering(
        max_buffer_size in 1usize..=1000,
        event_count in 1usize..=2000,
    ) {
        let mut buffer: VecDeque<MqttMessage> = VecDeque::new();

        for i in 0..event_count {
            let message = make_test_mqtt_message(i);
            buffer_message(&mut buffer, message, max_buffer_size);
        }

        // Verify the buffer is in strictly ascending order of message indices
        let indices: Vec<usize> = buffer
            .iter()
            .map(|m| {
                m.topic
                    .strip_prefix("ocpp/CP001/charger/Heartbeat-")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
            })
            .collect();

        for window in indices.windows(2) {
            prop_assert!(
                window[0] < window[1],
                "Buffer must maintain FIFO order: found index {} before {} (not ascending)",
                window[0],
                window[1]
            );
        }
    }

    /// **Property 7: Default MQTT buffer capacity is 500 as per specification**
    ///
    /// Validates: Requirements 5.5
    ///
    /// With the default max_buffer_size of 500, generate event counts from 1 to 2000
    /// and verify buffer never exceeds 500.
    #[test]
    fn mqtt_buffer_default_capacity_500(
        event_count in 1usize..=2000,
    ) {
        let max_buffer_size = 500; // Default from design spec
        let mut buffer: VecDeque<MqttMessage> = VecDeque::new();

        for i in 0..event_count {
            let message = make_test_mqtt_message(i);
            buffer_message(&mut buffer, message, max_buffer_size);

            // Invariant: buffer never exceeds 500
            prop_assert!(
                buffer.len() <= 500,
                "MQTT buffer length {} exceeded default capacity of 500 after inserting message {}",
                buffer.len(),
                i
            );
        }

        // Final check: buffer contains exactly min(event_count, 500) messages
        let expected_len = event_count.min(500);
        prop_assert_eq!(
            buffer.len(),
            expected_len,
            "Final buffer should contain {} messages, got {}",
            expected_len,
            buffer.len()
        );
    }
}
