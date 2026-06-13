//! Graceful startup and shutdown coordination.
//!
//! Provides signal handling (SIGTERM/SIGINT), shutdown sequencing, and startup timing.
//! Uses `tokio_util::sync::CancellationToken` to coordinate shutdown across all tasks.
//!
//! Shutdown sequence (per design doc):
//! 1. Receive SIGTERM/SIGINT
//! 2. Stop accepting new WebSocket connections (cancel the token)
//! 3. Complete forwarding of in-flight messages (up to 10 seconds)
//! 4. Send WebSocket close frame (code 1000) to both charger and Mobi.e
//! 5. Wait up to 5 seconds for close acknowledgments
//! 6. Publish offline status to MQTT (if connected)
//! 7. Log discarded message counts from buffers
//! 8. Exit with code 0

use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Default timeout for completing in-flight message forwarding during shutdown.
pub const INFLIGHT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default timeout for waiting for WebSocket close acknowledgments.
pub const CLOSE_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Default timeout for MQTT connection attempt at startup.
pub const MQTT_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Coordinates the graceful shutdown of all proxy components.
///
/// Holds a `CancellationToken` that is shared across all async tasks.
/// When a shutdown signal is received, the token is cancelled, signaling
/// all tasks to begin their graceful shutdown procedures.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    /// Token used to signal all tasks to begin shutdown.
    token: CancellationToken,
}

impl ShutdownCoordinator {
    /// Create a new `ShutdownCoordinator`.
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    /// Get a clone of the cancellation token for use in async tasks.
    ///
    /// Tasks should select on `token.cancelled()` to detect shutdown.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Check if shutdown has been initiated.
    pub fn is_shutting_down(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Initiate shutdown by cancelling the token.
    ///
    /// All tasks selecting on the token will be notified.
    pub fn initiate_shutdown(&self) {
        info!(component = "shutdown", "Initiating graceful shutdown");
        self.token.cancel();
    }

    /// Execute the graceful shutdown sequence.
    ///
    /// This method orchestrates the full shutdown:
    /// 1. Wait for in-flight messages to complete (up to `inflight_timeout`)
    /// 2. Close WebSocket connections (send close code 1000)
    /// 3. Wait for close acknowledgments (up to `close_ack_timeout`)
    /// 4. Publish offline to MQTT
    /// 5. Log discarded message counts
    ///
    /// The actual WebSocket and MQTT operations are performed by callbacks
    /// since this module doesn't own those connections directly. This method
    /// provides the timing and coordination framework.
    pub async fn graceful_shutdown(&self, context: ShutdownContext) {
        let shutdown_start = Instant::now();

        info!(
            component = "shutdown",
            "Starting graceful shutdown sequence"
        );

        // Step 1: Wait for in-flight messages to complete (up to 10s)
        info!(
            component = "shutdown",
            timeout_secs = context.inflight_timeout.as_secs(),
            "Waiting for in-flight messages to complete"
        );

        if let Some(inflight_waiter) = context.inflight_complete {
            match tokio::time::timeout(context.inflight_timeout, inflight_waiter).await {
                Ok(_) => {
                    info!(
                        component = "shutdown",
                        "All in-flight messages forwarded successfully"
                    );
                }
                Err(_) => {
                    warn!(
                        component = "shutdown",
                        timeout_secs = context.inflight_timeout.as_secs(),
                        "In-flight message timeout reached, proceeding with shutdown"
                    );
                }
            }
        }

        // Step 2: Send WebSocket close frames (code 1000)
        info!(
            component = "shutdown",
            "Sending WebSocket close frames (code 1000)"
        );

        if let Some(close_fn) = context.close_connections {
            close_fn.await;
        }

        // Step 3: Wait for close acknowledgments (up to 5s)
        info!(
            component = "shutdown",
            timeout_secs = context.close_ack_timeout.as_secs(),
            "Waiting for WebSocket close acknowledgments"
        );

        if let Some(ack_waiter) = context.close_ack_complete {
            match tokio::time::timeout(context.close_ack_timeout, ack_waiter).await {
                Ok(_) => {
                    info!(
                        component = "shutdown",
                        "Received close acknowledgments from both endpoints"
                    );
                }
                Err(_) => {
                    warn!(
                        component = "shutdown",
                        timeout_secs = context.close_ack_timeout.as_secs(),
                        "Close acknowledgment timeout reached, proceeding"
                    );
                }
            }
        }

        // Step 4: Publish offline to MQTT
        info!(component = "shutdown", "Publishing offline status to MQTT");

        if let Some(mqtt_offline_fn) = context.publish_offline {
            mqtt_offline_fn.await;
        }

        // Step 5: Log discarded message counts
        if context.upstream_discarded > 0 || context.downstream_discarded > 0 {
            warn!(
                component = "shutdown",
                upstream_discarded = context.upstream_discarded,
                downstream_discarded = context.downstream_discarded,
                "Messages discarded during shutdown"
            );
        }

        let elapsed = shutdown_start.elapsed();
        info!(
            component = "shutdown",
            elapsed_ms = elapsed.as_millis() as u64,
            "Graceful shutdown complete"
        );
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Context provided to the graceful shutdown sequence.
///
/// Contains futures and values needed to execute each shutdown step.
/// The actual connection/MQTT resources remain owned by their respective modules;
/// this struct only holds the hooks needed for coordination.
pub struct ShutdownContext {
    /// Timeout for waiting for in-flight messages to complete.
    pub inflight_timeout: Duration,

    /// Timeout for waiting for WebSocket close acknowledgments.
    pub close_ack_timeout: Duration,

    /// Future that resolves when all in-flight messages have been forwarded.
    /// If None, the step is skipped.
    pub inflight_complete: Option<futures_util::future::BoxFuture<'static, ()>>,

    /// Future that sends WebSocket close frames to both endpoints.
    /// If None, the step is skipped.
    pub close_connections: Option<futures_util::future::BoxFuture<'static, ()>>,

    /// Future that resolves when close acknowledgments are received.
    /// If None, the step is skipped.
    pub close_ack_complete: Option<futures_util::future::BoxFuture<'static, ()>>,

    /// Future that publishes offline status to MQTT.
    /// If None, the step is skipped.
    pub publish_offline: Option<futures_util::future::BoxFuture<'static, ()>>,

    /// Count of upstream (charger→central) messages discarded during shutdown.
    pub upstream_discarded: u64,

    /// Count of downstream (central→charger) messages discarded during shutdown.
    pub downstream_discarded: u64,
}

impl ShutdownContext {
    /// Create a minimal shutdown context with default timeouts and no hooks.
    ///
    /// Used when the proxy is shutting down before connections were fully established.
    pub fn minimal() -> Self {
        Self {
            inflight_timeout: INFLIGHT_TIMEOUT,
            close_ack_timeout: CLOSE_ACK_TIMEOUT,
            inflight_complete: None,
            close_connections: None,
            close_ack_complete: None,
            publish_offline: None,
            upstream_discarded: 0,
            downstream_discarded: 0,
        }
    }
}

/// Wait for a shutdown signal (SIGTERM or SIGINT).
///
/// This function blocks until either:
/// - A SIGTERM signal is received (Unix only)
/// - A SIGINT signal (Ctrl+C) is received
///
/// Returns the name of the signal that was received.
pub async fn wait_for_shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");

        tokio::select! {
            _ = sigterm.recv() => {
                info!(component = "shutdown", signal = "SIGTERM", "Received shutdown signal");
                "SIGTERM"
            }
            _ = tokio::signal::ctrl_c() => {
                info!(component = "shutdown", signal = "SIGINT", "Received shutdown signal");
                "SIGINT"
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to register Ctrl+C handler");
        info!(component = "shutdown", signal = "SIGINT", "Received shutdown signal");
        "SIGINT"
    }
}

/// Log startup completion and total time to ready state.
///
/// Should be called once the proxy is actively listening for charger connections.
pub fn log_startup_complete(startup_time: Instant) {
    let elapsed = startup_time.elapsed();
    info!(
        component = "startup",
        elapsed_ms = elapsed.as_millis() as u64,
        "Proxy startup complete — ready to accept connections"
    );
}

/// Log the beginning of the startup sequence.
///
/// Returns the `Instant` that should be passed to `log_startup_complete`
/// once the proxy is ready.
pub fn log_startup_begin() -> Instant {
    info!(component = "startup", "OCPP Proxy startup sequence initiated");
    Instant::now()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_coordinator_new() {
        let coordinator = ShutdownCoordinator::new();
        assert!(!coordinator.is_shutting_down());
    }

    #[test]
    fn test_shutdown_coordinator_default() {
        let coordinator = ShutdownCoordinator::default();
        assert!(!coordinator.is_shutting_down());
    }

    #[test]
    fn test_initiate_shutdown_cancels_token() {
        let coordinator = ShutdownCoordinator::new();
        assert!(!coordinator.is_shutting_down());

        coordinator.initiate_shutdown();
        assert!(coordinator.is_shutting_down());
    }

    #[test]
    fn test_token_clone_shares_state() {
        let coordinator = ShutdownCoordinator::new();
        let token = coordinator.token();

        assert!(!token.is_cancelled());
        coordinator.initiate_shutdown();
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_multiple_tokens_all_cancelled() {
        let coordinator = ShutdownCoordinator::new();
        let token1 = coordinator.token();
        let token2 = coordinator.token();
        let token3 = coordinator.token();

        coordinator.initiate_shutdown();

        assert!(token1.is_cancelled());
        assert!(token2.is_cancelled());
        assert!(token3.is_cancelled());
    }

    #[test]
    fn test_coordinator_clone_shares_state() {
        let coordinator = ShutdownCoordinator::new();
        let cloned = coordinator.clone();

        coordinator.initiate_shutdown();
        assert!(cloned.is_shutting_down());
    }

    #[test]
    fn test_shutdown_context_minimal() {
        let ctx = ShutdownContext::minimal();
        assert_eq!(ctx.inflight_timeout, INFLIGHT_TIMEOUT);
        assert_eq!(ctx.close_ack_timeout, CLOSE_ACK_TIMEOUT);
        assert!(ctx.inflight_complete.is_none());
        assert!(ctx.close_connections.is_none());
        assert!(ctx.close_ack_complete.is_none());
        assert!(ctx.publish_offline.is_none());
        assert_eq!(ctx.upstream_discarded, 0);
        assert_eq!(ctx.downstream_discarded, 0);
    }

    #[test]
    fn test_constants() {
        assert_eq!(INFLIGHT_TIMEOUT, Duration::from_secs(10));
        assert_eq!(CLOSE_ACK_TIMEOUT, Duration::from_secs(5));
        assert_eq!(MQTT_STARTUP_TIMEOUT, Duration::from_secs(10));
    }

    #[test]
    fn test_log_startup_begin_returns_instant() {
        let start = log_startup_begin();
        // Verify it's a recent instant (within last second)
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_graceful_shutdown_minimal_context() {
        let coordinator = ShutdownCoordinator::new();
        coordinator.initiate_shutdown();

        // Minimal context — all steps are skipped, should complete quickly
        let ctx = ShutdownContext::minimal();
        coordinator.graceful_shutdown(ctx).await;

        // If we get here, shutdown completed without panicking
        assert!(coordinator.is_shutting_down());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_with_inflight_complete() {
        let coordinator = ShutdownCoordinator::new();
        coordinator.initiate_shutdown();

        let ctx = ShutdownContext {
            inflight_timeout: Duration::from_millis(100),
            close_ack_timeout: Duration::from_millis(100),
            inflight_complete: Some(Box::pin(async {
                // Simulate quick in-flight completion
                tokio::time::sleep(Duration::from_millis(10)).await;
            })),
            close_connections: None,
            close_ack_complete: None,
            publish_offline: None,
            upstream_discarded: 0,
            downstream_discarded: 0,
        };

        coordinator.graceful_shutdown(ctx).await;
        assert!(coordinator.is_shutting_down());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_inflight_timeout() {
        let coordinator = ShutdownCoordinator::new();
        coordinator.initiate_shutdown();

        let ctx = ShutdownContext {
            inflight_timeout: Duration::from_millis(50),
            close_ack_timeout: Duration::from_millis(50),
            inflight_complete: Some(Box::pin(async {
                // Simulate slow in-flight — will exceed timeout
                tokio::time::sleep(Duration::from_secs(60)).await;
            })),
            close_connections: None,
            close_ack_complete: None,
            publish_offline: None,
            upstream_discarded: 5,
            downstream_discarded: 3,
        };

        let start = Instant::now();
        coordinator.graceful_shutdown(ctx).await;
        let elapsed = start.elapsed();

        // Should have timed out around 50ms, not waited 60s
        assert!(elapsed < Duration::from_secs(1));
        assert!(coordinator.is_shutting_down());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_close_ack_timeout() {
        let coordinator = ShutdownCoordinator::new();
        coordinator.initiate_shutdown();

        let ctx = ShutdownContext {
            inflight_timeout: Duration::from_millis(50),
            close_ack_timeout: Duration::from_millis(50),
            inflight_complete: None,
            close_connections: Some(Box::pin(async {
                // Simulate sending close frames quickly
            })),
            close_ack_complete: Some(Box::pin(async {
                // Simulate close ack never arriving
                tokio::time::sleep(Duration::from_secs(60)).await;
            })),
            publish_offline: None,
            upstream_discarded: 0,
            downstream_discarded: 0,
        };

        let start = Instant::now();
        coordinator.graceful_shutdown(ctx).await;
        let elapsed = start.elapsed();

        // Should have timed out around 50ms for close ack
        assert!(elapsed < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_graceful_shutdown_full_sequence() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let coordinator = ShutdownCoordinator::new();
        coordinator.initiate_shutdown();

        let inflight_done = Arc::new(AtomicBool::new(false));
        let close_sent = Arc::new(AtomicBool::new(false));
        let ack_received = Arc::new(AtomicBool::new(false));
        let offline_published = Arc::new(AtomicBool::new(false));

        let inflight_done_c = inflight_done.clone();
        let close_sent_c = close_sent.clone();
        let ack_received_c = ack_received.clone();
        let offline_published_c = offline_published.clone();

        let ctx = ShutdownContext {
            inflight_timeout: Duration::from_millis(200),
            close_ack_timeout: Duration::from_millis(200),
            inflight_complete: Some(Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                inflight_done_c.store(true, Ordering::SeqCst);
            })),
            close_connections: Some(Box::pin(async move {
                close_sent_c.store(true, Ordering::SeqCst);
            })),
            close_ack_complete: Some(Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ack_received_c.store(true, Ordering::SeqCst);
            })),
            publish_offline: Some(Box::pin(async move {
                offline_published_c.store(true, Ordering::SeqCst);
            })),
            upstream_discarded: 2,
            downstream_discarded: 1,
        };

        coordinator.graceful_shutdown(ctx).await;

        assert!(inflight_done.load(Ordering::SeqCst));
        assert!(close_sent.load(Ordering::SeqCst));
        assert!(ack_received.load(Ordering::SeqCst));
        assert!(offline_published.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_token_cancelled_is_awaitable() {
        let coordinator = ShutdownCoordinator::new();
        let token = coordinator.token();

        // Spawn a task that waits on the token
        let handle = tokio::spawn(async move {
            token.cancelled().await;
            true
        });

        // Small delay then cancel
        tokio::time::sleep(Duration::from_millis(10)).await;
        coordinator.initiate_shutdown();

        let result = handle.await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_wait_for_shutdown_signal_ctrl_c() {
        // We can't easily test real signals in unit tests,
        // but we can verify the function compiles and the types are correct.
        // The actual signal testing would require integration tests.

        // Instead, test that the coordinator integrates properly with signal handling:
        let coordinator = ShutdownCoordinator::new();
        let token = coordinator.token();

        // Simulate what main would do: spawn signal handler that cancels token
        let coordinator_clone = coordinator.clone();
        let handle = tokio::spawn(async move {
            // In real code: wait_for_shutdown_signal().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator_clone.initiate_shutdown();
        });

        // Wait for the token to be cancelled
        token.cancelled().await;
        handle.await.unwrap();

        assert!(coordinator.is_shutting_down());
    }
}
