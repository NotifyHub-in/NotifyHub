# Notification Control Plane

An open-source, Kubernetes-inspired notification control plane for modern products.

This repository contains the v1 scaffold for a Dockerized, Go-based platform that accepts notification intents, evaluates policies, routes delivery through pluggable connectors, and tracks delivery lifecycle state.

## Current status

This is the initial implementation scaffold. It includes:

- a monorepo layout for control-plane services and connectors
- runnable HTTP services for `api`, `worker`, and `callback-gateway`
- runnable connector stubs for `email`, `sms`, and `webhook`
- Dockerfiles and a local `docker compose` stack
- a shared Go module and common packages for config, HTTP helpers, and service metadata

For the current build log and production-readiness checklist, see [docs/project-status.md](/Users/Shaik/notifications/notification-control-plane/docs/project-status.md).

## Repo layout

```text
apps/
  api/
  worker/
  callback-gateway/
connectors/
  email/
  sms/
  webhook/
libs/
  contracts/
  core/
  observability/
deployments/docker/
build/docker/
migrations/
examples/
```

## Local development

Start the full local stack:

```bash
make up
```

Useful endpoints:

- API: `http://localhost:8080/healthz`
- Worker: `http://localhost:8081/healthz`
- Callback gateway: `http://localhost:8082/healthz`
- Email connector: `http://localhost:8091/healthz`
- SMS connector: `http://localhost:8092/healthz`
- Webhook connector: `http://localhost:8093/healthz`
- Kafka UI: `http://localhost:8085`

Run tests locally:

```bash
make test
```

## Next steps

- replace in-memory stubs with PostgreSQL-backed repositories
- add Kafka producers and consumers for request ingestion and worker dispatch
- define canonical persistence schemas for notification requests and delivery attempts
- add policy, template, and provider management APIs
- implement connector contract authentication and callback signatures
