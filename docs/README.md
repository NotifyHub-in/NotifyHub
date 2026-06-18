# Documentation Index

This directory collects the important docs for the NotifyHub in one place.

## Start Here

- [Top-level README](/README.md)
  Product overview, quick start, local URLs, testing commands, and examples.
- [Architecture](/docs/architecture/v1.md)
  Current runtime design, data model, delivery flow, and system boundaries.
- [System Dock](/docs/architecture/system-dock.md)
  Big-picture architecture map, module placement, and end-to-end data flow.
- [Managed Provider Platform Design](/docs/architecture/managed-provider-platform.md)
  Target architecture for first-party connectors, provider accounts, secure secret references, and runtime secret resolution.
- [Managed Provider Platform Implementation Plan](/docs/architecture/managed-provider-platform-implementation-plan.md)
  Phased rollout plan covering storage, API, worker, connectors, callbacks, security, and migration milestones.
- [OpenAPI Contract](/docs/api/openapi.yaml)
  Current API surface and resource schemas.
- [Project Status](/docs/project-status.md)
  What is already built and what is still pending for production readiness.
- [Production Release Plan](/docs/production-release-plan.md)
  Detailed production-readiness plan with blockers, workstreams, release gates, and go-live checks.
- [Deploy Control Plane To Kubernetes](/docs/guides/deploy-control-plane-to-kubernetes.md)
  Step-by-step Kubernetes deployment, secret wiring, Helm install flow, validation checks, and rollback guidance.
- [Deploy To AWS EKS](/docs/guides/deploy-to-aws-eks.md)
  Step-by-step AWS EKS deployment path, ECR image publishing, namespace layout, and production-shaped rollout notes.
- [Postgres Backup And Restore Verification](/docs/guides/postgres-backup-restore.md)
  How to verify logical backups and AWS RDS snapshot restores before you trust the platform in production.
- [AWS Operations Runbook](/docs/guides/aws-operations-runbook.md)
  Day-2 AWS commands for resuming, stopping, and tearing down the platform after deployment.
- [AWS Shutdown Runbook](/docs/guides/aws-shutdown-runbook.md)
  Step-by-step commands for deleting the AWS environment fully or partially.
- [Deploy Observability Stack](/docs/guides/deploy-observability-stack.md)
  Separate Prometheus, Grafana, cAdvisor, exporters, and alerting dashboard deployment for the NotifyHub.
- [Channel Setup Checklist](/docs/guides/channel-setup-checklist.md)
  Channel-by-channel setup steps for email, SMS, WhatsApp, push, and webhook without exposing real secret values.
- [Deploy To Production](/docs/guides/deploy-to-production.md)
  Step-by-step production packaging, Helm install flow, validation checks, and rollback guidance.
- [Release Images And ECR](/docs/guides/release-images-and-ecr.md)
  How image publishing works, how to release to custom registries, and how to publish to ECR with GitHub OIDC.

## Operating The Platform

- [User And Operator Guides](/docs/guides/README.md)
  Step-by-step onboarding and usage flow for running the platform, onboarding services, wiring providers, sending notifications, and tracking callbacks.
- [Operator Guide](/docs/operator-guide.md)
  Local startup, runtime URLs, dead-letter replay, provider health reset, observability, and troubleshooting.
- [Roadmap](/docs/roadmap.md)
  Planned platform directions beyond the current implementation.

## Extending The Platform

- [Connector Integration Guide](/docs/connector-sdk.md)
  How to add a new connector, expected request/response shape, and operational guard rails.
- [Example Integrations](/examples)
  Northbound usage examples for transactional email, OTP SMS, and operational webhook alerts.
- [Integration Guides](/docs/integrations/README.md)
  End-to-end service integration notes, including the upstream service multi-channel mapping and the example push integration flow.
- [WhatsApp Inbound Replies](/docs/guides/whatsapp-inbound-replies.md)
  How WhatsApp reply webhooks are normalized, persisted as channel events, and forwarded to subscribed services.

## Engineering Deep Dives

- [Engineering Docs](/docs/engineering/README.md)
  Detailed explanation of the managed-provider changes, runtime model interactions, and sequence diagrams for the current implementation.

## Architecture Decisions

- [ADR 0001: Control Plane Boundary](/docs/adr/0001-control-plane-boundary.md)
- [ADR 0002: Postgres + Kafka Runtime Model](/docs/adr/0002-postgres-kafka-runtime-model.md)
- [ADR 0003: Binding Sets And Provider Routing](/docs/adr/0003-binding-sets-and-provider-routing.md)
- [ADR 0004: Durable Retries And Dead Letters](/docs/adr/0004-durable-retries-and-dead-letters.md)
- [ADR 0005: Provider Health And Circuit Breakers](/docs/adr/0005-provider-health-and-circuit-breakers.md)

## Suggested Reading Order

1. Read the [README](/README.md).
2. Read the [Architecture](/docs/architecture/v1.md).
3. Use the [User And Operator Guides](/docs/guides/README.md) if you are onboarding or operating the platform.
4. Use the [Engineering Docs](/docs/engineering/README.md) if you want the implementation details and model interactions.
5. Use the [Connector Integration Guide](/docs/connector-sdk.md) if you want to add a provider.
6. Use the [AWS Operations Runbook](/docs/guides/aws-operations-runbook.md) if you need to resume, stop, or tear down the AWS environment.
7. Use the [AWS Shutdown Runbook](/docs/guides/aws-shutdown-runbook.md) if you need to fully delete the AWS environment.
8. Use the [Deploy To Production](/docs/guides/deploy-to-production.md) guide if you are preparing a release.
9. Use the [Roadmap](/docs/roadmap.md) and [Project Status](/docs/project-status.md) to understand what is next.
