//! Connecting to the Mobi.e Central System.
//!
//! This module only opens the connection. Reconnection, backoff and the
//! five-minute window that decides when to give up all live in `session`,
//! which owns both sockets and is the only place that can act on an upstream
//! failure.
//!
//! An earlier `UpstreamHandler` struct held a connection plus `send`, `recv`
//! and `reconnect` methods. Nothing ever called them — the compiler said so —
//! and its presence made the proxy look finished while no bytes moved.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{connect_async_tls_with_config, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::error::ProxyError;

/// Default connection timeout (10 seconds), per Requirement 2.4.
///
/// Sessions pass their configured timeout explicitly; this is the reference
/// value the tests assert against.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Build the full upstream URL: `{base_url}/{charge_point_id}`.
///
/// Requirement 2.2 — the Charge Point ID the charger presented is mirrored
/// into the Central System URL path unchanged.
pub fn build_upstream_url(base_url: &Url, charge_point_id: &str) -> Url {
    let mut url = base_url.clone();
    let mut path = url.path().to_string();
    if !path.ends_with('/') {
        path.push('/');
    }
    path.push_str(charge_point_id);
    url.set_path(&path);
    url
}

/// Connect to the Central System, returning the raw WebSocket stream.
///
/// The session splits this into sink and stream halves so it can read from the
/// Central System and write to it concurrently.
///
/// `bind_address` selects the local source address. It is unnecessary when the
/// Central System is reached by a destination route — which is the case for
/// the Mobi.e APN — and is provided for deployments that must select egress by
/// source address instead.
pub async fn connect_upstream(
    url: &Url,
    subprotocol: &str,
    bind_address: Option<IpAddr>,
    connect_timeout: Duration,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, ProxyError> {
    let mut request =
        url.as_str()
            .into_client_request()
            .map_err(|e| ProxyError::ConnectionUpstream {
                description: format!("Failed to build WebSocket request: {}", e),
            })?;

    // Requirement 2.3 — mirror the subprotocol the charger negotiated.
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_str(subprotocol).map_err(|e| ProxyError::ConnectionUpstream {
            description: format!("Invalid subprotocol header value: {}", e),
        })?,
    );

    let connect = async {
        match bind_address {
            None => connect_async_tls_with_config(request, None, false, None)
                .await
                .map(|(stream, _response)| stream)
                .map_err(|e| ProxyError::ConnectionUpstream {
                    description: format!("WebSocket connection failed: {}", e),
                }),
            Some(bind) => {
                // Binding a source address means we own the TCP connect, so
                // the target has to be resolved here rather than inside
                // tokio-tungstenite.
                let stream = connect_bound(url, bind).await?;
                tokio_tungstenite::client_async_tls_with_config(request, stream, None, None)
                    .await
                    .map(|(stream, _response)| stream)
                    .map_err(|e| ProxyError::ConnectionUpstream {
                        description: format!("WebSocket connection failed: {}", e),
                    })
            }
        }
    };

    timeout(connect_timeout, connect)
        .await
        .map_err(|_| ProxyError::ConnectionUpstream {
            description: format!(
                "Connection timed out after {} seconds",
                connect_timeout.as_secs()
            ),
        })?
}

/// Open a TCP connection from a specific local source address.
async fn connect_bound(url: &Url, bind: IpAddr) -> Result<TcpStream, ProxyError> {
    let host = url
        .host_str()
        .ok_or_else(|| ProxyError::ConnectionUpstream {
            description: "Central System URL has no host".to_string(),
        })?;
    let port = url.port_or_known_default().unwrap_or(80);

    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| ProxyError::ConnectionUpstream {
            description: format!("Failed to resolve '{}': {}", host, e),
        })?
        .collect();

    // Only try targets of the same address family as the bind address; a v4
    // socket cannot connect to a v6 peer and the error would be obscure.
    let mut last_err = None;
    for addr in addrs.iter().filter(|a| a.is_ipv4() == bind.is_ipv4()) {
        let socket = if bind.is_ipv4() {
            tokio::net::TcpSocket::new_v4()
        } else {
            tokio::net::TcpSocket::new_v6()
        }
        .map_err(|e| ProxyError::ConnectionUpstream {
            description: format!("Failed to create socket: {}", e),
        })?;

        socket
            .bind(SocketAddr::new(bind, 0))
            .map_err(|e| ProxyError::ConnectionUpstream {
                description: format!("Failed to bind upstream socket to {}: {}", bind, e),
            })?;

        match socket.connect(*addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }

    Err(ProxyError::ConnectionUpstream {
        description: match last_err {
            Some(e) => format!("Failed to connect to '{}': {}", host, e),
            None => format!("No address for '{}' matching bind family {}", host, bind),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ExponentialBackoff;

    #[test]
    fn test_build_upstream_url_basic() {
        let url = build_upstream_url(&Url::parse("wss://cs.mobi-e.pt").unwrap(), "CP001");
        assert_eq!(url.as_str(), "wss://cs.mobi-e.pt/CP001");
    }

    #[test]
    fn test_build_upstream_url_with_trailing_slash() {
        let url = build_upstream_url(&Url::parse("wss://cs.mobi-e.pt/").unwrap(), "CP001");
        assert_eq!(url.as_str(), "wss://cs.mobi-e.pt/CP001");
    }

    #[test]
    fn test_build_upstream_url_with_path() {
        let url = build_upstream_url(
            &Url::parse("wss://cs.mobi-e.pt/ocpp").unwrap(),
            "CHARGE_POINT_42",
        );
        assert_eq!(url.as_str(), "wss://cs.mobi-e.pt/ocpp/CHARGE_POINT_42");
    }

    #[test]
    fn test_build_upstream_url_with_port() {
        let url = build_upstream_url(&Url::parse("ws://localhost:8080").unwrap(), "test-cp");
        assert_eq!(url.as_str(), "ws://localhost:8080/test-cp");
    }

    /// The real Mobi.e endpoint shape: a plaintext private address with a
    /// versioned path, and the Charge Point ID appended unchanged.
    #[test]
    fn test_build_upstream_url_matches_the_mobie_endpoint() {
        let url = build_upstream_url(
            &Url::parse("ws://10.200.10.200/ocpp/1.6").unwrap(),
            "MOBI-ALM-00058",
        );
        assert_eq!(url.as_str(), "ws://10.200.10.200/ocpp/1.6/MOBI-ALM-00058");
    }

    /// Requirement 2.2 — the ID is mirrored verbatim, whatever it contains.
    #[test]
    fn test_build_upstream_url_preserves_charge_point_id() {
        for id in [
            "PT-MOB-CP-12345-AB",
            "MOBI-ALM-00058",
            "lowercase-id",
            "ID_WITH_UNDERSCORES",
            "1234567890",
        ] {
            let url =
                build_upstream_url(&Url::parse("wss://central-system.example.com").unwrap(), id);
            assert!(url.as_str().ends_with(id), "{} should end with {}", url, id);
        }
    }

    #[test]
    fn test_backoff_sequence_matches_requirements() {
        // Requirement 2.4: start at 2s, double, cap at 60s.
        let mut backoff =
            ExponentialBackoff::with_defaults(Duration::from_secs(2), Duration::from_secs(60));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(), Duration::from_secs(8));
        assert_eq!(backoff.next_delay(), Duration::from_secs(16));
        assert_eq!(backoff.next_delay(), Duration::from_secs(32));
        assert_eq!(backoff.next_delay(), Duration::from_secs(60));
        assert_eq!(backoff.next_delay(), Duration::from_secs(60));
    }

    #[test]
    fn test_connect_timeout_constant() {
        assert_eq!(CONNECT_TIMEOUT, Duration::from_secs(10));
    }

    /// Connecting to a closed port fails inside the timeout rather than
    /// hanging, so a session can move on to its backoff.
    #[tokio::test]
    async fn test_connect_upstream_fails_fast_on_closed_port() {
        let url = Url::parse("ws://127.0.0.1:1/ocpp").unwrap();
        let result = connect_upstream(&url, "ocpp1.6", None, Duration::from_secs(2)).await;
        assert!(result.is_err());
    }

    /// A bind address that is not local must fail with a clear message rather
    /// than silently falling back to the default route — which, for an
    /// APN-only endpoint, would be a connection that can never succeed.
    #[tokio::test]
    async fn test_connect_upstream_rejects_unusable_bind_address() {
        let url = Url::parse("ws://127.0.0.1:1/ocpp").unwrap();
        let bind = Some("203.0.113.99".parse().unwrap());
        let result = connect_upstream(&url, "ocpp1.6", bind, Duration::from_secs(2)).await;
        assert!(result.is_err(), "binding to a non-local address must fail");
    }
}
