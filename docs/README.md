# Documentation Index

This directory collects the important docs for the notification control plane in one place.

## Start Here

- [Top-level README](/Users/Shaik/notifications/notification-control-plane/README.md)
  Product overview, quick start, local URLs, testing commands, and examples.
- [Architecture](/Users/Shaik/notifications/notification-control-plane/docs/architecture/v1.md)
  Current runtime design, data model, delivery flow, and system boundaries.
- [OpenAPI Contract](/Users/Shaik/notifications/notification-control-plane/docs/api/openapi.yaml)
  Current API surface and resource schemas.
- [Project Status](/Users/Shaik/notifications/notification-control-plane/docs/project-status.md)
  What is already built and what is still pending for production readiness.

## Operating The Platform

- [Operator Guide](/Users/Shaik/notifications/notification-control-plane/docs/operator-guide.md)
  Local startup, runtime URLs, dead-letter replay, provider health reset, observability, and troubleshooting.
- [Roadmap](/Users/Shaik/notifications/notification-control-plane/docs/roadmap.md)
  Planned platform directions beyond the current implementation.

## Extending The Platform

- [Connector Integration Guide](/Users/Shaik/notifications/notification-control-plane/docs/connector-sdk.md)
  How to add a new connector, expected request/response shape, and operational guard rails.
- [Example Client Integrations](/Users/Shaik/notifications/notification-control-plane/examples)
  Northbound usage examples for transactional email, OTP SMS, and operational webhook alerts.

## Architecture Decisions

- [ADR 0001: Control Plane Boundary](/Users/Shaik/notifications/notification-control-plane/docs/adr/0001-control-plane-boundary.md)
- [ADR 0002: Postgres + Kafka Runtime Model](/Users/Shaik/notifications/notification-control-plane/docs/adr/0002-postgres-kafka-runtime-model.md)
- [ADR 0003: Binding Sets And Provider Routing](/Users/Shaik/notifications/notification-control-plane/docs/adr/0003-binding-sets-and-provider-routing.md)
- [ADR 0004: Durable Retries And Dead Letters](/Users/Shaik/notifications/notification-control-plane/docs/adr/0004-durable-retries-and-dead-letters.md)
- [ADR 0005: Provider Health And Circuit Breakers](/Users/Shaik/notifications/notification-control-plane/docs/adr/0005-provider-health-and-circuit-breakers.md)

## Suggested Reading Order

1. Read the [README](/Users/Shaik/notifications/notification-control-plane/README.md).
2. Read the [Architecture](/Users/Shaik/notifications/notification-control-plane/docs/architecture/v1.md).
3. Use the [Operator Guide](/Users/Shaik/notifications/notification-control-plane/docs/operator-guide.md) if you are running the stack.
4. Use the [Connector Integration Guide](/Users/Shaik/notifications/notification-control-plane/docs/connector-sdk.md) if you want to add a provider.
5. Use the [Roadmap](/Users/Shaik/notifications/notification-control-plane/docs/roadmap.md) and [Project Status](/Users/Shaik/notifications/notification-control-plane/docs/project-status.md) to understand what is next.
