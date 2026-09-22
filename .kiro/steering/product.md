# Product Summary

OCPP Proxy is a WebSocket proxy for OCPP 1.6J (Open Charge Point Protocol) that sits between EV chargers and a Central System (Mobi.e). It transparently forwards OCPP messages while publishing them to an MQTT broker for Home Assistant integration.

## Core Responsibilities

- Accept WebSocket connections from EV chargers on the downstream side
- Maintain a WebSocket connection to the Mobi.e Central System on the upstream side
- Forward OCPP messages byte-for-byte between charger and central system (priority path)
- Publish OCPP events asynchronously to MQTT for Home Assistant consumption
- Expose a health check endpoint for Home Assistant monitoring and manual diagnosis
- On request from Home Assistant (opt-in, `charging.enabled`), send SetChargingProfile and a few companion Calls to the charger, and report the outcome over MQTT
- Handle graceful shutdown with in-flight message completion

## Key Invariants

- Messages are forwarded byte-for-byte — no modification, no re-serialization
- The proxy originates OCPP only when charger control is enabled, only towards the charger, one Call in flight at a time and never while a Central System Call is outstanding; replies to proxy Calls (ids prefixed `proxy-`) are consumed, never forwarded
- FIFO ordering is maintained per direction
- MQTT publishing never blocks the forwarding path
- Buffers use FIFO eviction when full (oldest messages discarded first)
- Connections reconnect with exponential backoff (upstream: 2s–60s, MQTT: 1s–30s)
- Upstream reconnection fails after 5 minutes, triggering downstream close (code 1001)

## Deployment

- Runs as a native binary under systemd in an unprivileged LXC (113) on the Proxmox host `mouraishikawa`
- Connects to the EMQX broker on VM 110 (Home Assistant) over one LAN hop; TLS optional
- Reaches Mobi.e over a mobile APN on a ZTE 4G USB dongle owned by the Proxmox host
- Is the charger's only path to Mobi.e: no failover, so host downtime stops charging and billing
- Designed for a single charger setup (but supports Charge Point ID routing)
