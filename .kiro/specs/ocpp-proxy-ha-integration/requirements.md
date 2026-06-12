# Requirements Document

## Introduction

This document specifies requirements for an OCPP Proxy hosted on AWS that sits between an Autel EV charger and the Mobi.e Central System in Portugal. The proxy transparently forwards all OCPP 1.6J WebSocket messages between the charger and Mobi.e while capturing OCPP events and publishing them to MQTT for Home Assistant integration running locally on a Raspberry Pi 4. Hosting the proxy on AWS provides higher availability and resilience — the charger maintains connectivity to Mobi.e even when the local Home Assistant instance is unavailable. The primary constraint is that Mobi.e communication must never be disrupted — the proxy must be invisible to both endpoints and prioritize upstream connectivity above all else.

## Glossary

- **Proxy**: The OCPP Proxy application that relays WebSocket messages between the Charger and the Central_System
- **Charger**: The Autel EV charger acting as an OCPP 1.6J Charge Point client
- **Central_System**: The Mobi.e OCPP Central System server that manages charging sessions and billing
- **MQTT_Broker**: The Mosquitto MQTT broker running as a Home Assistant add-on on the local Raspberry Pi 4
- **Home_Assistant**: The home automation platform running on HAOS on a local Raspberry Pi 4
- **AWS_Host**: The AWS infrastructure (ECS Fargate or EC2) hosting the Proxy container for high availability
- **OCPP_Message**: A JSON-formatted message conforming to the OCPP 1.6J specification transmitted over WebSocket
- **Upstream_Connection**: The WebSocket connection from the Proxy to the Central_System
- **Downstream_Connection**: The WebSocket connection from the Charger to the Proxy
- **Charge_Point_ID**: The unique identifier of the Charger as registered with Mobi.e
- **MQTT_Topic**: A hierarchical string used to route messages within the MQTT_Broker

## Requirements

### Requirement 1: WebSocket Server for Charger Connection

**User Story:** As a charger owner, I want the proxy to accept my charger's OCPP WebSocket connection, so that the charger can communicate through the proxy instead of directly to Mobi.e.

#### Acceptance Criteria

1. THE Proxy SHALL accept incoming WebSocket connections from the Charger on a configurable port using the OCPP 1.6J subprotocol
2. WHEN the Charger initiates a WebSocket connection, THE Proxy SHALL complete the WebSocket handshake within 5 seconds
3. THE Proxy SHALL support the `ocpp1.6` WebSocket subprotocol as defined in the OCPP 1.6J specification
4. WHEN multiple connection attempts arrive from the Charger, THE Proxy SHALL accept only one active Downstream_Connection per Charge_Point_ID
5. IF the Charger connection fails the WebSocket handshake, THEN THE Proxy SHALL log the failure reason and close the connection gracefully

### Requirement 2: WebSocket Client for Central System Connection

**User Story:** As a charger owner, I want the proxy to connect to Mobi.e on behalf of my charger, so that billing and session management continue to work without interruption.

#### Acceptance Criteria

1. WHEN the Charger establishes a Downstream_Connection, THE Proxy SHALL initiate an Upstream_Connection to the Central_System using the configured Mobi.e WebSocket URL
2. THE Proxy SHALL use the same Charge_Point_ID in the Upstream_Connection URL path as received from the Charger
3. THE Proxy SHALL forward the same WebSocket subprotocol header to the Central_System as received from the Charger
4. IF the Upstream_Connection cannot be established within 10 seconds, THEN THE Proxy SHALL retry with exponential backoff starting at 2 seconds up to a maximum of 60 seconds
5. IF the Upstream_Connection is lost, THEN THE Proxy SHALL attempt reconnection using exponential backoff while keeping the Downstream_Connection open

### Requirement 3: Transparent Message Forwarding

**User Story:** As a charger owner, I want all OCPP messages forwarded transparently between my charger and Mobi.e, so that charging sessions, billing, and remote operations work exactly as if the proxy were not present.

#### Acceptance Criteria

1. WHEN the Proxy receives an OCPP_Message from the Charger, THE Proxy SHALL forward the message to the Central_System without modification within 100 milliseconds
2. WHEN the Proxy receives an OCPP_Message from the Central_System, THE Proxy SHALL forward the message to the Charger without modification within 100 milliseconds
3. THE Proxy SHALL preserve the exact JSON payload of each OCPP_Message during forwarding, including field order and whitespace
4. THE Proxy SHALL forward OCPP_Messages in the same order they were received on each direction of the connection
5. IF the destination connection is temporarily unavailable, THEN THE Proxy SHALL buffer up to 100 messages for a maximum of 30 seconds before discarding the oldest messages
6. THE Proxy SHALL support all OCPP 1.6J message types including Call, CallResult, and CallError frames

### Requirement 4: Mobi.e Communication Priority

**User Story:** As a charger owner, I want Mobi.e communication to always take priority, so that my billing and charging session management are never disrupted by proxy features.

#### Acceptance Criteria

1. THE Proxy SHALL forward OCPP_Messages between the Charger and Central_System before performing any MQTT publishing or internal processing
2. IF the MQTT_Broker is unreachable, THEN THE Proxy SHALL continue forwarding OCPP_Messages between the Charger and Central_System without interruption
3. IF internal processing of an OCPP_Message causes an error, THEN THE Proxy SHALL forward the original message to the destination and log the processing error separately
4. THE Proxy SHALL allocate separate processing paths for message forwarding and MQTT publishing so that MQTT operations do not block OCPP message flow
5. WHILE the Upstream_Connection is being re-established, THE Proxy SHALL buffer Charger messages and deliver them to the Central_System once the connection is restored

### Requirement 5: OCPP Event Publishing to MQTT

**User Story:** As a Home Assistant user, I want all OCPP events published to MQTT, so that I can monitor charger status, energy consumption, and charging sessions in my dashboard.

#### Acceptance Criteria

1. WHEN the Proxy forwards an OCPP_Message, THE Proxy SHALL publish the message content to the MQTT_Broker on a structured MQTT_Topic
2. THE Proxy SHALL publish MQTT messages using the topic format `ocpp/{Charge_Point_ID}/{direction}/{message_type}` where direction is `charger` or `central_system`
3. THE Proxy SHALL publish the full JSON payload of each OCPP_Message as the MQTT message body
4. THE Proxy SHALL include a timestamp in ISO 8601 format in the MQTT message metadata
5. WHEN the Proxy publishes to the MQTT_Broker, THE Proxy SHALL use QoS level 1 to ensure at-least-once delivery
6. IF the MQTT_Broker is unreachable, THEN THE Proxy SHALL buffer up to 500 MQTT messages and publish them when the connection is restored
7. THE Proxy SHALL publish a status message to `ocpp/{Charge_Point_ID}/status` containing the connection state of both Upstream_Connection and Downstream_Connection whenever either state changes

### Requirement 6: MQTT Connection Management

**User Story:** As a Home Assistant user, I want the proxy to maintain a reliable connection to my MQTT broker over the internet, so that I receive charger events consistently in my local Home Assistant.

#### Acceptance Criteria

1. THE Proxy SHALL connect to the MQTT_Broker using configurable host, port, username, and password parameters
2. WHEN the Proxy starts, THE Proxy SHALL establish a connection to the MQTT_Broker within 10 seconds
3. IF the MQTT_Broker connection is lost, THEN THE Proxy SHALL attempt reconnection using exponential backoff starting at 1 second up to a maximum of 30 seconds
4. THE Proxy SHALL publish an MQTT Last Will and Testament message to `ocpp/{Charge_Point_ID}/availability` with payload `offline` upon unexpected disconnection
5. WHEN the Proxy connects to the MQTT_Broker, THE Proxy SHALL publish a retained message to `ocpp/{Charge_Point_ID}/availability` with payload `online`
6. THE Proxy SHALL connect to the MQTT_Broker using TLS encryption to secure communication over the internet

### Requirement 7: Configuration Management

**User Story:** As a system administrator, I want to configure the proxy through environment variables or a configuration file, so that I can deploy and adjust settings without modifying code.

#### Acceptance Criteria

1. THE Proxy SHALL read configuration from environment variables with fallback to a YAML configuration file
2. THE Proxy SHALL require the following configuration parameters: Central_System WebSocket URL, Proxy listen port, MQTT_Broker host, MQTT_Broker port, MQTT username, MQTT password, and TLS settings for MQTT
3. IF a required configuration parameter is missing, THEN THE Proxy SHALL fail to start and log which parameter is missing
4. THE Proxy SHALL support optional configuration parameters for: TLS certificate paths, log level, message buffer sizes, and reconnection timing
5. THE Proxy SHALL validate all configuration parameters at startup before accepting connections
6. THE Proxy SHALL support loading secrets from AWS Secrets Manager or environment variables for MQTT credentials and TLS certificates

### Requirement 8: Logging and Observability

**User Story:** As a system administrator, I want comprehensive logging from the proxy, so that I can diagnose connection issues and monitor proxy health.

#### Acceptance Criteria

1. THE Proxy SHALL log all connection state changes for both Upstream_Connection and Downstream_Connection with timestamps
2. THE Proxy SHALL log OCPP_Message summaries at DEBUG level including message type, action, and unique ID without logging full message payloads at INFO level
3. THE Proxy SHALL log errors with sufficient context to identify the source, including connection identifiers and message references
4. THE Proxy SHALL support configurable log levels: DEBUG, INFO, WARNING, and ERROR
5. THE Proxy SHALL output logs in structured JSON format to stdout for integration with container logging
6. IF message forwarding latency exceeds 500 milliseconds, THEN THE Proxy SHALL log a warning with the measured latency and message identifier

### Requirement 9: Graceful Startup and Shutdown

**User Story:** As a system administrator, I want the proxy to start and stop gracefully, so that no messages are lost during maintenance operations.

#### Acceptance Criteria

1. WHEN the Proxy receives a termination signal (SIGTERM or SIGINT), THE Proxy SHALL stop accepting new connections and complete forwarding of in-flight messages within 10 seconds before shutting down
2. WHEN the Proxy starts, THE Proxy SHALL first establish the MQTT_Broker connection, then begin listening for Charger connections
3. IF the Proxy cannot establish the MQTT_Broker connection at startup, THEN THE Proxy SHALL proceed to accept Charger connections and retry the MQTT connection in the background
4. WHEN the Proxy shuts down, THE Proxy SHALL close the Upstream_Connection and Downstream_Connection with proper WebSocket close frames
5. THE Proxy SHALL log the startup sequence completion and the total time taken to become ready

### Requirement 10: Health Monitoring

**User Story:** As a Home Assistant user, I want to know the proxy's health status, so that I can set up alerts when something goes wrong.

#### Acceptance Criteria

1. THE Proxy SHALL expose a health check HTTP endpoint on a configurable port that returns the current status of all connections
2. WHEN queried, THE health check endpoint SHALL return a JSON response containing: Upstream_Connection state, Downstream_Connection state, MQTT_Broker connection state, uptime, and message counters
3. THE Proxy SHALL report health status as `healthy` when the Upstream_Connection and Downstream_Connection are both active
4. THE Proxy SHALL report health status as `degraded` when the MQTT_Broker connection is lost but OCPP forwarding continues
5. IF both the Upstream_Connection and the MQTT_Broker connection are lost, THEN THE Proxy SHALL report health status as `unhealthy`

### Requirement 11: AWS Deployment and High Availability

**User Story:** As a charger owner, I want the proxy hosted on AWS with high availability, so that the charger maintains connectivity to Mobi.e even when my home network or Raspberry Pi is unavailable.

#### Acceptance Criteria

1. THE Proxy SHALL be packaged as a Docker container image suitable for deployment on AWS ECS Fargate or EC2
2. THE Proxy SHALL expose a single port for Charger WebSocket connections and a separate port for the health check endpoint
3. THE Proxy SHALL be stateless so that container restarts do not lose persistent configuration
4. THE Proxy SHALL start and become ready to accept connections within 30 seconds of container launch
5. IF the Proxy container crashes, THEN THE AWS_Host SHALL restart the container automatically within 60 seconds
6. THE Proxy SHALL use a stable DNS endpoint or Elastic IP so that the Charger connection URL does not change across deployments
7. THE Proxy SHALL support TLS termination for the Charger WebSocket connection using AWS Certificate Manager or a provided certificate
