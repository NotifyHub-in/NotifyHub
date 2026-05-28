# Notification Control Plane

An open-source, Kubernetes-inspired notification control plane for modern products.

This repository contains an early working version of a Dockerized, Go-based platform that accepts notification intents, evaluates policies, routes delivery through pluggable connectors, and tracks delivery lifecycle state.

## Current status

The current build includes:

- control-plane services for `api`, `worker`, and `callback-gateway`
- runnable connector stubs for `email`, `sms`, and `webhook`
- PostgreSQL + Kafka backed request processing
- routing, preferences, templates, retries, failover, callbacks, and lifecycle webhooks
- Prometheus, Grafana, Kafka UI, and Adminer for local visibility
- versioned SQL migrations applied through a dedicated migration runner

For the current build log and production-readiness checklist, see [docs/project-status.md](/Users/Shaik/notifications/notification-control-plane/docs/project-status.md).

## Repo layout

```text
apps/
  api/
  migrate/
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

Run migrations directly against the configured database:

```bash
make migrate
```

Useful endpoints:

- API: `http://localhost:8080/healthz`
- Worker: `http://localhost:8081/healthz`
- Callback gateway: `http://localhost:8082/healthz`
- Email connector: `http://localhost:8091/healthz`
- SMS connector: `http://localhost:8092/healthz`
- Webhook connector: `http://localhost:8093/healthz`
- Kafka UI: `http://localhost:8085`
- Adminer: `http://localhost:8086`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

Run tests locally:

```bash
make test
```

The local Docker stack now runs migrations through a one-shot `migrate` service before `api`, `worker`, and `callback-gateway` start. Future schema changes should be added as new numbered `.sql` files in [migrations](/Users/Shaik/notifications/notification-control-plane/migrations), not by editing older versions in place.
