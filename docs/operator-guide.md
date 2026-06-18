# Operator Guide

This guide is for running and debugging the NotifyHub as it exists today.

If you want the full step-by-step onboarding flow for using the platform, start with [User And Operator Guides](/docs/guides/README.md). This operator guide stays focused on runtime operations, inspection, and troubleshooting.

## Local Stack

Start the stack:

```bash
make up
```

Stop it:

```bash
make down
```

Apply migrations manually:

```bash
make migrate
```

## Local Service URLs

- API: `http://localhost:8080`
- Worker health: `http://localhost:8081/healthz`
- Callback gateway: `http://localhost:8082`
- Email connector: `http://localhost:8091`
- SMS connector: `http://localhost:8092`
- Webhook connector: `http://localhost:8093`
- Kafka UI: `http://localhost:8085`
- Adminer: `http://localhost:8086`
- cAdvisor: `http://localhost:8088`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Postgres on host: `localhost:5433`

## Default Local Credentials

### PostgreSQL

- host in Docker network: `postgres`
- host from your machine: `localhost`
- port from your machine: `5433`
- username: `postgres`
- password: `postgres`
- database: `notification_control_plane`

### Grafana

- user: `admin`
- password: `admin`

## Normal Startup Expectations

After the stack starts:

- `migrate` should complete before `api`, `worker`, and `callback-gateway`
- `api` should accept `POST /v1/notification-requests`
- `worker` should report healthy and consume from Kafka
- Grafana and Prometheus should show service targets as up

Admin/config write calls must include `X-Notification-Admin-Token`. In the default local stack, the token is `<admin-token>`.

Read-only config and status calls can use `X-Notification-Read-Token` or the admin token. In the default local stack, the read token is `<read-token>`.

For production packaging and rollback flow, use [Deploy To Production](/docs/guides/deploy-to-production.md).

## Push Setup For An Upstream Service

This control plane already supports push generically through the `fcm-push` provider. Keep the setup on the same managed-provider pattern as the upstream push integration: mount the control-plane secret directory into `connector-push`, register a file-backed provider account, and wire routing through the API.

The connector should expose the secret directory at `/run/notification-secrets`, for example:

```yaml
volumes:
  - ${NOTIFICATION_SECRETS_DIR:-/tmp/notification-control-plane-secrets}:/run/notification-secrets:ro
```

Defaults used by the generic push integration:

- source secret file: `<upstream-service-repo>/src/main/java/<app>/resource/firebase_config/firebase_service_account.json`
- mounted secret path in connector: `file:///run/notification-secrets/firebase_service_account.json`
- provider account provider key: `fcm-push`
- binding set: `upstream-service-push`
- routing event: `service.alert`

If your checkout lives somewhere else, copy the Firebase JSON into `NOTIFICATION_SECRETS_DIR` and keep the bind mount aligned with that directory.

Then follow the API-based setup flow in [Upstream Push Integration](/docs/integrations/upstream-push-integration.md). The device token belongs in `recipient.push_token`, not in provider configuration.

Useful checks:

```bash
docker compose -f deployments/docker/compose.yml ps
```

```bash
curl -s http://localhost:8080/healthz
```

```bash
curl -s http://localhost:8081/metrics
```

## Day-To-Day Operator Tasks

### Register A Sending Client

Any upstream service can send notifications once it has a client API key.

Create a client:

```bash
curl -s -X POST http://localhost:8080/v1/clients \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "tenant-a",
    "client_name": "billing-service",
    "allowed_channels": ["email", "sms", "whatsapp", "push", "webhook"],
    "enabled": true
  }'
```

Use the returned `api_key` as:

```bash
Authorization: Bearer <api_key>
```

List clients:

```bash
curl -s http://localhost:8080/v1/clients?tenant_id=tenant-a
```

### Inspect A Request

```bash
curl -s http://localhost:8080/v1/notification-requests/<request_id>
```

That response is the best single place to inspect:

- request state
- delivery attempts
- scheduled retries
- dead letters
- webhook delivery attempts

### Inspect Dead Letters

```bash
curl -s http://localhost:8080/v1/dead-letters
```

Inspect a single dead letter:

```bash
curl -s http://localhost:8080/v1/dead-letters/<dead_letter_id>
```

Replay it:

```bash
curl -s -X POST http://localhost:8080/v1/dead-letters/<dead_letter_id>/replay
```

### Inspect Provider Health

List provider binding health:

```bash
curl -s http://localhost:8080/v1/provider-binding-health
```

Reset a circuit manually:

```bash
curl -s -X POST http://localhost:8080/v1/provider-binding-health/<binding_id>/reset
```

Use this only after the underlying provider problem has been addressed.

### Inspect Bindings And Routing

Bindings:

```bash
curl -s http://localhost:8080/v1/provider-bindings
```

Routing policies:

```bash
curl -s http://localhost:8080/v1/routing-policies
```

Templates:

```bash
curl -s http://localhost:8080/v1/templates
```

## Observability

### Grafana

Use Grafana for:

- API latency
- worker end-to-end latency
- callback throughput
- retry and dead-letter backlog
- provider failure classifications
- provider circuit state
- CPU, memory, goroutines, and Kafka lag

### Prometheus

Prometheus is useful for raw query inspection and confirming metric series.

### Kafka UI

Use Kafka UI when you want to confirm:

- topic availability
- consumer group lag
- message backlog during worker outages or load tests

### Adminer

Use Adminer when you need direct table-level inspection for:

- `notification_requests`
- `delivery_attempts`
- `scheduled_retries`
- `dead_letter_notifications`
- `provider_bindings`
- `provider_binding_health`

## Common Operational Flows

### A Provider Starts Failing

What you should expect:

- connector failure metrics rise
- retryable failures create scheduled retries
- repeated retryable failures open the provider circuit
- later requests skip the unhealthy binding during cooldown

What to inspect:

- Grafana retry and circuit panels
- `GET /v1/provider-binding-health`
- request detail for recent failed or delayed requests

### Dead Letters Are Growing

What it usually means:

- requests are exhausting retry policy
- failures are non-retryable
- or a provider or configuration issue has lasted longer than retry budget

What to inspect:

- `GET /v1/dead-letters`
- dead-letter reasons
- connector failure classification metrics
- provider binding config and secret refs

### Kafka Lag Is Growing

What it usually means:

- worker is down
- worker is healthy but not draining fast enough
- connector calls are too slow
- retries are dominating throughput

What to inspect:

- worker logs
- Kafka consumer lag panel
- end-to-end latency panels
- provider circuit state

## Troubleshooting Quick Guide

### Requests Stay In `accepted`

Check:

- worker is running
- Kafka topic exists
- consumer lag is increasing
- worker metrics and logs show consumption

### Requests Fail Immediately

Check:

- template variables are complete
- provider binding exists for the channel and binding set
- required env-backed secret refs exist in the worker container
- connector returned a non-retryable classification

### Retries Are Not Draining

Check:

- scheduled retry backlog and oldest age metrics
- worker retry poller health
- Postgres availability
- whether the failing provider circuit is still open

### Callback Updates Never Arrive

Check:

- provider callback endpoint configuration
- callback gateway logs
- callback-related metrics
- `provider_message_id` on the original attempt

## Production Notes

This repo is still optimized for local and early-stage internal usage. Before wider deployment, prioritize:

- API authentication and authorization
- secret-manager integration
- signed lifecycle webhooks and callback verification
- backup and restore process for Postgres
- environment-specific configuration management
- production deployment packaging
- alert rules and runbooks
