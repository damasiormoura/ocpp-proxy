# Product Summary

OCPP Proxy is a WebSocket proxy for OCPP 1.6J (Open Charge Point Protocol) that sits between EV chargers and a Central System (Mobi.e). It transparently forwards OCPP messages while publishing them to an MQTT broker for Home Assistant integration.

## Core Responsibilities

- Accept WebSocket connections from EV chargers on the downstream side
- Maintain a WebSocket connection to the Mobi.e Central System on the upstream side
- Forward OCPP messages byte-for-byte between charger and central system (priority path)
- Publish OCPP events asynchronously to MQTT for Home Assistant consumption
- Expose a health check endpoint for AWS ECS/NLB monitoring
- Handle graceful shutdown with in-flight message completion

## Key Invariants

- Messages are forwarded byte-for-byte — no modification, no re-serialization
- FIFO ordering is maintained per direction
- MQTT publishing never blocks the forwarding path
- Buffers use FIFO eviction when full (oldest messages discarded first)
- Connections reconnect with exponential backoff (upstream: 2s–60s, MQTT: 1s–30s)
- Upstream reconnection fails after 5 minutes, triggering downstream close (code 1001)

## Deployment

- Runs as a Docker container on AWS ECS behind an NLB
- Connects to a Mosquitto MQTT broker on a Raspberry Pi via mutual TLS
- Designed for a single charger setup (but supports Charge Point ID routing)
