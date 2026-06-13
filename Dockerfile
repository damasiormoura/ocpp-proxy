# ==============================================================================
# OCPP Proxy - Multi-stage Docker Build
# ==============================================================================
# Stage 1: Build the Rust binary in release mode
# Stage 2: Minimal runtime image with only required dependencies
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Builder
# ------------------------------------------------------------------------------
FROM rust:1.82-bookworm AS builder

WORKDIR /usr/src/ocpp-proxy

# Cache dependency compilation by building a dummy project first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs && \
    cargo build --release && \
    rm -rf src

# Copy the actual source code and rebuild
COPY src/ src/
COPY tests/ tests/

# Touch main.rs to ensure cargo rebuilds our code (not the cached dummy)
RUN touch src/main.rs src/lib.rs && \
    cargo build --release && \
    strip target/release/ocpp-proxy

# ------------------------------------------------------------------------------
# Stage 2: Runtime
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install only the minimal runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libssl3 \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN groupadd -r ocpp && useradd -r -g ocpp -d /app -s /sbin/nologin ocpp

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/ocpp-proxy/target/release/ocpp-proxy /app/ocpp-proxy

# Copy a default config file (can be overridden via volume mount or env vars)
COPY config.yaml.example /app/config.yaml.example

# Set ownership
RUN chown -R ocpp:ocpp /app

USER ocpp

# Expose configurable ports:
# - 9000: WebSocket server for charger connections (configurable via OCPP_PROXY_LISTEN_PORT)
# - 8080: Health check HTTP endpoint (configurable via OCPP_PROXY_HEALTH_PORT)
EXPOSE 9000
EXPOSE 8080

# Health check using the /health endpoint
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Set the entrypoint to the proxy binary
ENTRYPOINT ["/app/ocpp-proxy"]
