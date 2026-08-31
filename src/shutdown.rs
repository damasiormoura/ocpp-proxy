//! Graceful startup and shutdown coordination.
//!
//! Provides signal handling (SIGTERM/SIGINT), shutdown sequencing, and startup timing.
//! Uses `tokio_util::sync::CancellationToken` to coordinate shutdown across all tasks.
//!
//! This module provides the signal handling and the cancellation token that
//! coordinates shutdown. The sequence itself is carried out by `main` and the
//! session tasks, which own the connections:
//!
//! 1. A signal cancels the token
//! 2. The listener stops accepting and health reports the proxy as unavailable
//! 3. Each session observes the token, sends its charger a close frame (1000)
//!    and unwinds; `main` waits for the registry to empty, bounded to 10s
//! 4. Every MQTT sender is dropped so the publisher's channel closes and its
//!    thread exits
//!
//! An earlier version of this module owned a `ShutdownContext` of optional
//! hooks and logged each step of that sequence. Every hook was passed as
//! `None`, so it narrated a shutdown it never performed. Doing the work where
//! the resources actually live is both shorter and honest.

use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::info;

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
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
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
        info!(
            component = "shutdown",
            signal = "SIGINT",
            "Received shutdown signal"
        );
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
    info!(
        component = "startup",
        "OCPP Proxy startup sequence initiated"
    );
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
