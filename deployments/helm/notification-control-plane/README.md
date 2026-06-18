# NotifyHub Helm Chart

This chart packages the production deployment surface for the NotifyHub:

- `api`
- `worker`
- `callback-gateway`
- `connector-email`
- `connector-sms`
- `connector-webhook`
- `connector-push`
- `connector-whatsapp`
- a pre-install / pre-upgrade migration job

## What the chart expects

- An external Postgres database
- An external Kafka cluster
- A Kubernetes Secret containing the admin and read-only API tokens, unless you enable `auth.createSecret`
- A Kubernetes Secret mounted at `/run/notification-secrets` for provider secrets and callback verification secrets

## Install

```bash
helm upgrade --install notification-control-plane deployments/helm/notification-control-plane \
  --namespace notification-control-plane \
  --create-namespace \
  -f values.prod.yaml
```

## Rollback

```bash
helm rollback notification-control-plane <REVISION>
```

Schema changes are forward-only. If a migration has already landed, rollbacks that revert the code but not the schema should be treated as temporary and followed by a forward-fix release.
