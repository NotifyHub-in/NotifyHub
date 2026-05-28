# Notification Control Plane

An open-source, Go-based notification control plane for event-driven products.

This project is building toward a "Kubernetes for notifications" model: business systems publish notification intent, while the platform owns delivery orchestration, policy evaluation, retries, provider failover, callbacks, auditability, and observability.

## What It Does Today

The current platform already supports:

- canonical notification intake through a REST API
- asynchronous worker processing backed by Kafka
- PostgreSQL-backed request, attempt, policy, and audit state
- templates, routing policies, preference policies, and delivery policies
- provider bindings with binding-set aware routing
- pluggable `email`, `sms`, and `webhook` connectors
- retry classification, dead-lettering, replay, and provider failover
- provider health tracking and `gobreaker`-backed circuit breakers
- provider callback normalization and outbound lifecycle webhooks
- Prometheus/Grafana metrics, Kafka UI, and Adminer
- unit tests, live integration tests, and a basic CI workflow

For the build log and production-readiness checklist, see [docs/project-status.md](/Users/Shaik/notifications/notification-control-plane/docs/project-status.md).

## Mental Model

Your application decides:

- why a notification should happen
- who should receive it
- what business event or template key it maps to

The control plane decides:

- which channels should be used
- which provider binding set should handle them
- how templates are rendered
- whether preferences suppress delivery
- how retries, failover, DLQ, replay, and callbacks are handled

## Core Components

- `api`
  Control-plane API for requests, policies, templates, provider bindings, dead letters, and status queries.
- `worker`
  Async reconciliation engine that evaluates policy, renders content, dispatches connectors, schedules retries, and manages circuit-breaker state.
- `callback-gateway`
  Normalizes provider callbacks into delivery-attempt and request-status updates.
- `connector-email`, `connector-sms`, `connector-webhook`
  Reference out-of-process provider adapters.
- `migrate`
  Versioned SQL migration runner used by Docker and local commands.

## Repo Layout

```text
apps/
  api/
  callback-gateway/
  migrate/
  worker/
connectors/
  email/
  sms/
  webhook/
libs/
  contracts/
  core/
  messaging/
  observability/
  storage/
deployments/docker/
docs/
  api/
  architecture/
  project-status.md
migrations/
tests/
  integration/
  load/
```

## Quick Start

Start the local stack:

```bash
make up
```

Run migrations directly:

```bash
make migrate
```

Inspect the stack:

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

Grafana default login:

- user: `admin`
- password: `admin`

Adminer default login:

- system: `PostgreSQL`
- server: `postgres`
- username: `postgres`
- password: `postgres`
- database: `notification_control_plane`

## Example Request

```json
{
  "idempotency_key": "order-123-delayed",
  "event_name": "order.delayed",
  "template_key": "order-delayed-v1",
  "channels": ["email"],
  "recipient": {
    "user_id": "user-123",
    "email": "alice@example.com"
  },
  "variables": {
    "order_id": "ORD-123",
    "reason": "carrier_delay"
  },
  "priority": "high"
}
```

Submit it:

```bash
curl -s -X POST http://localhost:8080/v1/notification-requests \
  -H 'Content-Type: application/json' \
  -d @request.json
```

## Testing

Run the normal Go suite:

```bash
make test
```

Check formatting:

```bash
make fmt-check
```

Run the live integration suite against the local stack:

```bash
make integration-test
```

Run the load test:

```bash
make load-test
```

## API Docs

- Documentation index: [docs/README.md](/Users/Shaik/notifications/notification-control-plane/docs/README.md)
- OpenAPI-style contract: [docs/api/openapi.yaml](/Users/Shaik/notifications/notification-control-plane/docs/api/openapi.yaml)
- Architecture notes: [docs/architecture/v1.md](/Users/Shaik/notifications/notification-control-plane/docs/architecture/v1.md)
- Current project status: [docs/project-status.md](/Users/Shaik/notifications/notification-control-plane/docs/project-status.md)
- Operator guide: [docs/operator-guide.md](/Users/Shaik/notifications/notification-control-plane/docs/operator-guide.md)
- Connector extension guide: [docs/connector-sdk.md](/Users/Shaik/notifications/notification-control-plane/docs/connector-sdk.md)
- Roadmap: [docs/roadmap.md](/Users/Shaik/notifications/notification-control-plane/docs/roadmap.md)

## Example Client Integrations

- Transactional email: [examples/transactional-order-delay/README.md](/Users/Shaik/notifications/notification-control-plane/examples/transactional-order-delay/README.md)
- OTP SMS: [examples/otp-sms/README.md](/Users/Shaik/notifications/notification-control-plane/examples/otp-sms/README.md)
- Operational webhook alert: [examples/ops-webhook-alert/README.md](/Users/Shaik/notifications/notification-control-plane/examples/ops-webhook-alert/README.md)

## Current Public API Surface

Key endpoints currently exposed by `api`:

- `POST /v1/notification-requests`
- `GET /v1/notification-requests/{requestID}`
- `GET /v1/provider-bindings`
- `GET /v1/provider-bindings/{channel}`
- `POST /v1/provider-bindings`
- `GET /v1/provider-binding-health`
- `GET /v1/provider-binding-health/{bindingID}`
- `POST /v1/provider-binding-health/{bindingID}/reset`
- `GET /v1/routing-policies`
- `GET /v1/preference-policies`
- `GET /v1/templates`
- `GET /v1/delivery-policies`
- `GET /v1/webhook-subscriptions`
- `GET /v1/dead-letters`
- `POST /v1/dead-letters/{deadLetterID}/replay`

Provider callbacks are received through `callback-gateway`:

- `POST /v1/providers/{provider}/callbacks`

## Notes For Contributors

- Add new schema changes as numbered files in [migrations](/Users/Shaik/notifications/notification-control-plane/migrations).
- Keep connector-specific behavior behind connector processes rather than embedding provider logic into `api` or `worker`.
- Prefer extending the unit and integration suites when changing worker decision logic, retries, or provider behavior.
- Review architecture decisions in [docs/adr](/Users/Shaik/notifications/notification-control-plane/docs/adr) before making large design changes.

## Production Readiness

This is already a strong working platform foundation, but it is not yet finished as a production-ready OSS control plane. Remaining work includes broader operator docs, stronger multi-channel semantics, auth/RBAC, secret-manager integration, more CI/build automation, and deployment packaging beyond local Docker.
