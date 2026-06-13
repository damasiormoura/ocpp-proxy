//! Health check HTTP server for AWS load balancer integration.
//!
//! Exposes a `/health` endpoint reporting connection states and message counters.
//! Returns HTTP 200 for healthy/degraded status and HTTP 503 for unhealthy status,
//! supporting AWS ECS/NLB health checks.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::models::ConnectionState;
use crate::state::{ConnectionStateManager, HealthStatus};

/// Shared state accessible by the health check handler.
pub struct HealthState {
    /// Connection state manager holding upstream/downstream/MQTT states and metrics.
    pub connection_manager: Mutex<ConnectionStateManager>,
    /// Instant when the proxy started, used to compute uptime.
    pub start_time: Instant,
}

/// JSON response returned by the `/health` endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub upstream: ConnectionState,
    pub downstream: ConnectionState,
    pub mqtt: ConnectionState,
    pub uptime_seconds: u64,
    pub messages: MessageCounters,
}

/// Message counters for forwarded and dropped messages per direction.
#[derive(Debug, Clone, Serialize)]
pub struct MessageCounters {
    pub charger_to_central_forwarded: u64,
    pub charger_to_central_dropped: u64,
    pub central_to_charger_forwarded: u64,
    pub central_to_charger_dropped: u64,
}

/// Build the health check axum router.
///
/// The router exposes a single GET `/health` endpoint that reads from the shared
/// `HealthState` to compute and return the current health status.
pub fn health_router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .with_state(state)
}

/// Handler for GET `/health`.
///
/// Locks the connection state manager, computes the health status, and returns
/// a JSON response with the appropriate HTTP status code:
/// - 200 for `healthy` or `degraded`
/// - 503 for `unhealthy`
async fn health_handler(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let response = {
        let manager = state.connection_manager.lock().await;
        let status = manager.health_status();
        let metrics = manager.metrics();
        let uptime = state.start_time.elapsed().as_secs();

        HealthResponse {
            status,
            upstream: manager.upstream_state(),
            downstream: manager.downstream_state(),
            mqtt: manager.mqtt_state(),
            uptime_seconds: uptime,
            messages: MessageCounters {
                charger_to_central_forwarded: metrics.charger_to_central_forwarded,
                charger_to_central_dropped: metrics.charger_to_central_dropped,
                central_to_charger_forwarded: metrics.central_to_charger_forwarded,
                central_to_charger_dropped: metrics.central_to_charger_dropped,
            },
        }
    };

    let http_status = match response.status {
        HealthStatus::Healthy | HealthStatus::Degraded => StatusCode::OK,
        HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    (http_status, Json(response))
}

/// Start the health check HTTP server on the given port.
///
/// This function spawns the server and blocks until it is shut down.
/// It is intended to be run as a Tokio task.
pub async fn run_health_server(port: u16, state: Arc<HealthState>) -> Result<(), std::io::Error> {
    let app = health_router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::models::ConnectionId;

    /// Helper to create a shared HealthState for testing.
    fn make_health_state() -> Arc<HealthState> {
        Arc::new(HealthState {
            connection_manager: Mutex::new(ConnectionStateManager::new(16)),
            start_time: Instant::now(),
        })
    }

    #[tokio::test]
    async fn test_health_endpoint_returns_unhealthy_when_all_disconnected() {
        let state = make_health_state();
        let app = health_router(state);

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let health: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(health["status"], "unhealthy");
        assert_eq!(health["upstream"], "disconnected");
        assert_eq!(health["downstream"], "disconnected");
        assert_eq!(health["mqtt"], "disconnected");
    }

    #[tokio::test]
    async fn test_health_endpoint_returns_healthy_when_all_connected() {
        let state = make_health_state();

        {
            let mut mgr = state.connection_manager.lock().await;
            mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
            mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
            mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
        }

        let app = health_router(state);

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let health: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(health["status"], "healthy");
        assert_eq!(health["upstream"], "connected");
        assert_eq!(health["downstream"], "connected");
        assert_eq!(health["mqtt"], "connected");
    }

    #[tokio::test]
    async fn test_health_endpoint_returns_degraded_when_mqtt_lost() {
        let state = make_health_state();

        {
            let mut mgr = state.connection_manager.lock().await;
            mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
            mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
            // MQTT stays Disconnected
        }

        let app = health_router(state);

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let health: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(health["status"], "degraded");
    }

    #[tokio::test]
    async fn test_health_endpoint_returns_unhealthy_when_downstream_lost() {
        let state = make_health_state();

        {
            let mut mgr = state.connection_manager.lock().await;
            mgr.transition(ConnectionId::Upstream, ConnectionState::Connected);
            mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
            // Downstream stays Disconnected
        }

        let app = health_router(state);

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let health: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(health["status"], "unhealthy");
    }

    #[tokio::test]
    async fn test_health_endpoint_includes_uptime() {
        let state = make_health_state();
        let app = health_router(state);

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let health: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Uptime should be a non-negative number (at least 0)
        assert!(health["uptime_seconds"].is_u64());
    }

    #[tokio::test]
    async fn test_health_endpoint_includes_message_counters() {
        let state = make_health_state();

        {
            let mut mgr = state.connection_manager.lock().await;
            mgr.metrics_mut().charger_to_central_forwarded = 42;
            mgr.metrics_mut().charger_to_central_dropped = 3;
            mgr.metrics_mut().central_to_charger_forwarded = 38;
            mgr.metrics_mut().central_to_charger_dropped = 1;
        }

        let app = health_router(state);

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let health: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let messages = &health["messages"];
        assert_eq!(messages["charger_to_central_forwarded"], 42);
        assert_eq!(messages["charger_to_central_dropped"], 3);
        assert_eq!(messages["central_to_charger_forwarded"], 38);
        assert_eq!(messages["central_to_charger_dropped"], 1);
    }

    #[tokio::test]
    async fn test_health_endpoint_returns_json_content_type() {
        let state = make_health_state();
        let app = health_router(state);

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let content_type = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(content_type.contains("application/json"));
    }

    #[tokio::test]
    async fn test_health_response_serialization_format() {
        let response = HealthResponse {
            status: HealthStatus::Healthy,
            upstream: ConnectionState::Connected,
            downstream: ConnectionState::Connected,
            mqtt: ConnectionState::Connected,
            uptime_seconds: 120,
            messages: MessageCounters {
                charger_to_central_forwarded: 10,
                charger_to_central_dropped: 0,
                central_to_charger_forwarded: 8,
                central_to_charger_dropped: 0,
            },
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["status"], "healthy");
        assert_eq!(json["upstream"], "connected");
        assert_eq!(json["downstream"], "connected");
        assert_eq!(json["mqtt"], "connected");
        assert_eq!(json["uptime_seconds"], 120);
        assert_eq!(json["messages"]["charger_to_central_forwarded"], 10);
        assert_eq!(json["messages"]["charger_to_central_dropped"], 0);
        assert_eq!(json["messages"]["central_to_charger_forwarded"], 8);
        assert_eq!(json["messages"]["central_to_charger_dropped"], 0);
    }

    #[tokio::test]
    async fn test_health_endpoint_503_when_upstream_and_mqtt_lost() {
        let state = make_health_state();

        {
            let mut mgr = state.connection_manager.lock().await;
            mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
            // Upstream and MQTT stay Disconnected
        }

        let app = health_router(state);

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let health: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(health["status"], "unhealthy");
    }
}
