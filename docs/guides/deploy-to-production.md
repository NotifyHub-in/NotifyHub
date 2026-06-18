# Deploying To Production

This guide covers the production packaging and release path for the NotifyHub.

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

## Image publishing

The repository now includes a release workflow that publishes every shipped image when you push a version tag such as `v1.2.3`.

By default, tag pushes publish to GHCR under the repository namespace. If a team wants a different registry, they can run the same workflow manually and override the registry host and namespace with workflow inputs.

The workflow publishes these images:

- `api`
- `worker`
- `callback-gateway`
- `migrate`
- `connector-email`
- `connector-sms`
- `connector-webhook`
- `connector-push`
- `connector-whatsapp`

For a normal release, the sequence is:

```bash
git tag v1.2.3
git push origin v1.2.3
```

That will publish immutable tags like:

- `ghcr.io/<org>/notification-control-plane/api:1.2.3`
- `ghcr.io/<org>/notification-control-plane/worker:1.2.3`
- `ghcr.io/<org>/notification-control-plane/callback-gateway:1.2.3`

The chart values should point at the same version for every image so the release stays internally consistent.

### Custom registry release

If you need to publish to another registry, run the workflow manually and provide:

- `version`
- `registry`
- `namespace`

For authentication, store these repository or environment secrets:

- `REGISTRY_USERNAME`
- `REGISTRY_PASSWORD`

GHCR uses the built-in `GITHUB_TOKEN`, so those custom registry secrets are only needed when you override the registry host.

If the target registry is AWS ECR, you can avoid storing registry passwords by supplying:

- `aws-region`
- `aws-role-arn`

In that mode, the workflow uses GitHub OIDC to assume the role and logs in to ECR with temporary AWS credentials instead of long-lived registry secrets.

Example manual dispatch values:

- `version = v1.2.3`
- `registry = 123456789012.dkr.ecr.ap-south-1.amazonaws.com`
- `namespace = platform/notification-control-plane`
- `aws-region = ap-south-1`
- `aws-role-arn = arn:aws:iam::123456789012:role/github-actions-ecr-publish`

The workflow will then publish images such as:

- `123456789012.dkr.ecr.ap-south-1.amazonaws.com/platform/notification-control-plane/api:1.2.3`
- `123456789012.dkr.ecr.ap-south-1.amazonaws.com/platform/notification-control-plane/worker:1.2.3`

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
  callback-gateway:
    repository: ghcr.io/your-org/notification-control-plane/callback-gateway
    tag: "1.0.0"
  migrate:
    repository: ghcr.io/your-org/notification-control-plane/migrate
    tag: "1.0.0"
  connector-email:
    repository: ghcr.io/your-org/notification-control-plane/connector-email
    tag: "1.0.0"
  connector-sms:
    repository: ghcr.io/your-org/notification-control-plane/connector-sms
    tag: "1.0.0"
  connector-webhook:
    repository: ghcr.io/your-org/notification-control-plane/connector-webhook
    tag: "1.0.0"
  connector-push:
    repository: ghcr.io/your-org/notification-control-plane/connector-push
    tag: "1.0.0"
  connector-whatsapp:
    repository: ghcr.io/your-org/notification-control-plane/connector-whatsapp
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

## Release checklist

Before you cut a production release, verify:

1. `go test ./...` passes.
2. The image build validation job is green.
3. The release workflow published all service images for the version tag.
4. Helm values reference the same tag across the API, worker, callback gateway, migrate job, and connector images.
5. A smoke notification works against the target environment.
