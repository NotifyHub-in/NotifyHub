# Documentation Index

This directory collects the important docs for the notification control plane in one place.

## Start Here

- [Top-level README](/Users/Shaik/notifications/notification-control-plane/README.md)
  Product overview, quick start, local URLs, testing commands, and examples.
- [Architecture](/Users/Shaik/notifications/notification-control-plane/docs/architecture/v1.md)
  Current runtime design, data model, delivery flow, and system boundaries.
- [System Dock](/Users/Shaik/notifications/notification-control-plane/docs/architecture/system-dock.md)
  Big-picture architecture map, module placement, and end-to-end data flow.
- [Managed Provider Platform Design](/Users/Shaik/notifications/notification-control-plane/docs/architecture/managed-provider-platform.md)
  Target architecture for first-party connectors, provider accounts, secure secret references, and runtime secret resolution.
- [Managed Provider Platform Implementation Plan](/Users/Shaik/notifications/notification-control-plane/docs/architecture/managed-provider-platform-implementation-plan.md)
  Phased rollout plan covering storage, API, worker, connectors, callbacks, security, and migration milestones.
- [OpenAPI Contract](/Users/Shaik/notifications/notification-control-plane/docs/api/openapi.yaml)
  Current API surface and resource schemas.
- [Project Status](/Users/Shaik/notifications/notification-control-plane/docs/project-status.md)
  What is already built and what is still pending for production readiness.
- [Production Release Plan](/Users/Shaik/notifications/notification-control-plane/docs/production-release-plan.md)
  Detailed production-readiness plan with blockers, workstreams, release gates, and go-live checks.
- [Deploy Control Plane To Kubernetes](/Users/Shaik/notifications/notification-control-plane/docs/guides/deploy-control-plane-to-kubernetes.md)
  Step-by-step Kubernetes deployment, secret wiring, Helm install flow, validation checks, and rollback guidance.
- [Deploy Observability Stack](/Users/Shaik/notifications/notification-control-plane/docs/guides/deploy-observability-stack.md)
  Separate Prometheus, Grafana, cAdvisor, exporters, and alerting dashboard deployment for the notification control plane.
- [Channel Setup Checklist](/Users/Shaik/notifications/notification-control-plane/docs/guides/channel-setup-checklist.md)
  Channel-by-channel setup steps for email, SMS, WhatsApp, push, and webhook without exposing real secret values.
- [Deploy To Production](/Users/Shaik/notifications/notification-control-plane/docs/guides/deploy-to-production.md)
  Step-by-step production packaging, Helm install flow, validation checks, and rollback guidance.

## Operating The Platform

- [User And Operator Guides](/Users/Shaik/notifications/notification-control-plane/docs/guides/README.md)
  Step-by-step onboarding and usage flow for running the platform, onboarding services, wiring providers, sending notifications, and tracking callbacks.
- [Operator Guide](/Users/Shaik/notifications/notification-control-plane/docs/operator-guide.md)
  Local startup, runtime URLs, dead-letter replay, provider health reset, observability, and troubleshooting.
- [Roadmap](/Users/Shaik/notifications/notification-control-plane/docs/roadmap.md)
  Planned platform directions beyond the current implementation.

## Extending The Platform

- [Connector Integration Guide](/Users/Shaik/notifications/notification-control-plane/docs/connector-sdk.md)
  How to add a new connector, expected request/response shape, and operational guard rails.
- [Example Client Integrations](/Users/Shaik/notifications/notification-control-plane/examples)
  Northbound usage examples for transactional email, OTP SMS, and operational webhook alerts.
- [Integration Guides](/Users/Shaik/notifications/notification-control-plane/docs/integrations/README.md)
  End-to-end service integration notes, including the communication-engine multi-channel mapping and the AFS-admin push integration flow.

## Engineering Deep Dives

- [Engineering Docs](/Users/Shaik/notifications/notification-control-plane/docs/engineering/README.md)
  Detailed explanation of the managed-provider changes, runtime model interactions, and sequence diagrams for the current implementation.

## Architecture Decisions

- [ADR 0001: Control Plane Boundary](/Users/Shaik/notifications/notification-control-plane/docs/adr/0001-control-plane-boundary.md)
- [ADR 0002: Postgres + Kafka Runtime Model](/Users/Shaik/notifications/notification-control-plane/docs/adr/0002-postgres-kafka-runtime-model.md)
- [ADR 0003: Binding Sets And Provider Routing](/Users/Shaik/notifications/notification-control-plane/docs/adr/0003-binding-sets-and-provider-routing.md)
- [ADR 0004: Durable Retries And Dead Letters](/Users/Shaik/notifications/notification-control-plane/docs/adr/0004-durable-retries-and-dead-letters.md)
- [ADR 0005: Provider Health And Circuit Breakers](/Users/Shaik/notifications/notification-control-plane/docs/adr/0005-provider-health-and-circuit-breakers.md)

## Suggested Reading Order

1. Read the [README](/Users/Shaik/notifications/notification-control-plane/README.md).
2. Read the [Architecture](/Users/Shaik/notifications/notification-control-plane/docs/architecture/v1.md).
3. Use the [User And Operator Guides](/Users/Shaik/notifications/notification-control-plane/docs/guides/README.md) if you are onboarding or operating the platform.
4. Use the [Engineering Docs](/Users/Shaik/notifications/notification-control-plane/docs/engineering/README.md) if you want the implementation details and model interactions.
5. Use the [Connector Integration Guide](/Users/Shaik/notifications/notification-control-plane/docs/connector-sdk.md) if you want to add a provider.
6. Use the [Deploy To Production](/Users/Shaik/notifications/notification-control-plane/docs/guides/deploy-to-production.md) guide if you are preparing a release.
7. Use the [Roadmap](/Users/Shaik/notifications/notification-control-plane/docs/roadmap.md) and [Project Status](/Users/Shaik/notifications/notification-control-plane/docs/project-status.md) to understand what is next.
