# Deploying To Production

This guide covers the production packaging and release path for the Notification Control Plane.

## What the production package contains

- `api`
- `worker`
- `callback-gateway`
- provider connectors for email, SMS, webhook, push, and WhatsApp
- a Helm-managed migration job

## Prerequisites

1. A Postgres database reachable from the cluster.
2. A Kafka cluster reachable from the cluster.
3. A Kubernetes Secret or external-secret resource containing:
   - the admin API token
   - the read-only API token
4. A Kubernetes Secret mounted at `/run/notification-secrets` containing provider and callback verification secrets.
5. Published container images for every service.

## Release order

1. Build and publish images with an immutable tag.
2. Update Helm values for the new tag.
3. Run the Helm upgrade.
4. Let the pre-upgrade migration job complete.
5. Verify health endpoints.
6. Run a smoke notification through a supported channel.

## Example values

Create a `values.prod.yaml` that points at your real infrastructure:

```yaml
global:
  appEnv: production
  appVersion: "1.0.0"
  secretVolume:
    name: notification-control-plane-secrets

auth:
  existingSecret: notification-control-plane-auth

database:
  url: postgres://...

kafka:
  brokers: kafka-1:9092,kafka-2:9092

images:
  api:
    repository: ghcr.io/your-org/notification-control-plane/api
    tag: "1.0.0"
  worker:
    repository: ghcr.io/your-org/notification-control-plane/worker
    tag: "1.0.0"
```

## Install

```bash
helm upgrade --install notification-control-plane deployments/helm/notification-control-plane \
  --namespace notification-control-plane \
  --create-namespace \
  -f values.prod.yaml
```

## Validate

After the upgrade, confirm:

1. The migration job succeeded.
2. All pods are ready.
3. `/v1/status` returns OK on the API.
4. A real provider-backed notification request is accepted.
5. The connector logs show a successful send.

## Roll back

Use Helm rollback for app regressions:

```bash
helm rollback notification-control-plane <REVISION>
```

If the release added a schema migration, treat rollback as a code rollback only. The database should be moved forward with a follow-up release rather than trying to undo schema in place.
