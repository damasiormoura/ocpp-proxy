//! Property-based tests for exponential backoff computation.
//!
//! **Property 4: Exponential backoff computes correct delays**
//!
//! For any exponential backoff configuration (initial delay, multiplier, maximum delay)
//! and for any number of retry attempts, the computed delay for attempt N equals
//! min(initial × multiplier^(N-1), maximum), and the delay never exceeds the configured maximum.
//!
//! **Validates: Requirements 2.4, 6.3**

use proptest::prelude::*;
use std::time::Duration;

use ocpp_proxy::models::ExponentialBackoff;

proptest! {
    /// Property 4: Exponential backoff computes correct delays.
    ///
    /// Generates random (initial_ms, multiplier, max_ms, attempts) tuples and verifies:
    /// 1. Each returned delay equals min(initial × multiplier^(N-1), maximum) within floating-point tolerance
    /// 2. No delay ever exceeds the configured maximum
    ///
    /// **Validates: Requirements 2.4, 6.3**
    #[test]
    fn prop_backoff_computes_correct_delays(
        initial_ms in 1u64..=10_000u64,
        multiplier in 1.1f64..=4.0f64,
        max_ms in 1u64..=120_000u64,
        attempts in 1u32..=30u32,
    ) {
        // Ensure max >= initial for a valid configuration
        let max_ms = max_ms.max(initial_ms);

        let initial = Duration::from_millis(initial_ms);
        let max = Duration::from_millis(max_ms);
        let mut backoff = ExponentialBackoff::new(initial, max, multiplier);

        for n in 1..=attempts {
            let delay = backoff.next_delay();

            // Property: delay never exceeds configured maximum
            prop_assert!(
                delay <= max,
                "Delay {:?} exceeded maximum {:?} at attempt {}",
                delay, max, n
            );

            // Compute expected delay: min(initial × multiplier^(N-1), maximum)
            let expected_secs = (initial.as_secs_f64() * multiplier.powi((n - 1) as i32))
                .min(max.as_secs_f64());
            let expected = Duration::from_secs_f64(expected_secs);

            // Allow floating-point tolerance: within 1 microsecond or 0.001% relative error
            let diff = if delay > expected {
                (delay - expected).as_secs_f64()
            } else {
                (expected - delay).as_secs_f64()
            };

            let tolerance = expected.as_secs_f64() * 0.00001 + 1e-6; // relative + absolute tolerance

            prop_assert!(
                diff <= tolerance,
                "Delay mismatch at attempt {}: got {:?}, expected {:?} (diff: {}s, tolerance: {}s). \
                 Config: initial={}ms, multiplier={}, max={}ms",
                n, delay, expected, diff, tolerance,
                initial_ms, multiplier, max_ms
            );
        }
    }

    /// Property 4 (supplementary): Delay is monotonically non-decreasing until capped at max.
    ///
    /// Once the backoff reaches the maximum, all subsequent delays remain at the maximum.
    ///
    /// **Validates: Requirements 2.4, 6.3**
    #[test]
    fn prop_backoff_delays_never_exceed_max(
        initial_ms in 1u64..=10_000u64,
        multiplier in 1.1f64..=4.0f64,
        max_ms in 1u64..=120_000u64,
        attempts in 1u32..=30u32,
    ) {
        let max_ms = max_ms.max(initial_ms);

        let initial = Duration::from_millis(initial_ms);
        let max = Duration::from_millis(max_ms);
        let mut backoff = ExponentialBackoff::new(initial, max, multiplier);

        let mut prev_delay = Duration::ZERO;
        let mut hit_max = false;

        for _ in 1..=attempts {
            let delay = backoff.next_delay();

            // Property: delay never exceeds maximum
            prop_assert!(
                delay <= max,
                "Delay {:?} exceeded configured max {:?}",
                delay, max
            );

            // Property: delays are monotonically non-decreasing
            prop_assert!(
                delay >= prev_delay,
                "Delay decreased from {:?} to {:?}",
                prev_delay, delay
            );

            // Once we hit max, all subsequent delays must equal max
            if hit_max {
                prop_assert!(
                    delay == max,
                    "After reaching max, delay {:?} != max {:?}",
                    delay, max
                );
            }

            if delay == max {
                hit_max = true;
            }

            prev_delay = delay;
        }
    }
}
