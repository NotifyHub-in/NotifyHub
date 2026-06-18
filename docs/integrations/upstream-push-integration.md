# Upstream Push Integration

This guide shows how an upstream service can integrate push delivery with NotifyHub without introducing any service-specific runtime code.

The control plane already has a generic `fcm-push` provider path. For an upstream service, we only need to:

1. mount the Firebase admin SDK JSON into the `connector-push` container through the control-plane secret mount,
2. register a `fcm-push` provider account that points at that mounted file,
3. bind that provider account to the `connector-push` service,
4. add a routing policy and preference policy, and
5. send a push request with the recipient device token.

## Default paths and names

- source Firebase JSON: `<upstream-service-repo>/src/main/java/<app>/resource/firebase_config/firebase_service_account.json`
- host secret directory: `${NOTIFICATION_SECRETS_DIR:-/tmp/notification-control-plane-secrets}`
- mounted secret path in connector: `file:///run/notification-secrets/firebase_service_account.json`
- provider key: `fcm-push`
- binding set: `upstream-service-push`
- routing event: `service.alert`
- template key: `upstream-alert-v1`

If your checkout is in a different location, copy the Firebase JSON into `NOTIFICATION_SECRETS_DIR` before starting the stack.

## Integration flow

Make sure the secret directory is mounted into `connector-push`:

```yaml
volumes:
  - ${NOTIFICATION_SECRETS_DIR:-/tmp/notification-control-plane-secrets}:/run/notification-secrets:ro
```

Then register the managed-provider resources through the API.

### 1. Create a sending client

```bash
curl -s -X POST http://localhost:8080/v1/clients \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"upstream-service","client_name":"upstream-service","allowed_channels":["push"],"enabled":true}'
```

### 2. Create the FCM provider account

```bash
curl -s -X POST http://localhost:8080/v1/provider-accounts \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "upstream-service",
    "provider_key": "fcm-push",
    "display_name": "Upstream FCM",
    "channel": "push",
    "enabled": true,
    "config": {"project_id": "upstream-service"},
    "secret_refs": {
      "service_account_json": {
        "ref": "file:///run/notification-secrets/firebase_service_account.json",
        "material_type": "secret_json",
        "source": "file"
      }
    }
  }'
```

### 3. Bind the provider account to the push connector

```bash
curl -s -X POST http://localhost:8080/v1/provider-bindings \
  -H 'Content-Type: application/json' \
  -d '{
    "channel": "push",
    "binding_set": "upstream-service-push",
    "provider_account_id": "<provider_account_id>",
    "endpoint_url": "http://connector-push:8094",
    "enabled": true,
    "priority": 10
  }'
```

### 4. Create routing

```bash
curl -s -X POST http://localhost:8080/v1/routing-policies \
  -H 'Content-Type: application/json' \
  -d '{
    "event_name": "service.alert",
    "channels": ["push"],
    "binding_set": "upstream-service-push",
    "enabled": true,
    "priority": 10
  }'
```

### 5. Enable push for the target user

```bash
curl -s -X POST http://localhost:8080/v1/preference-policies \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "upstream-service",
    "channel": "push",
    "is_enabled": true
  }'
```

### 6. Create the push template

```bash
curl -s -X POST http://localhost:8080/v1/templates \
  -H 'Content-Type: application/json' \
  -d '{
    "template_key": "upstream-alert-v1",
    "channel": "push",
    "subject_template": "{{title}}",
    "body_template": "{{body}}",
    "metadata": {"app": "upstream-service"},
    "enabled": true
  }'
```

### 7. Send a push request

Use the returned client `api_key` when posting a notification request. The recipient token is the FCM device token you pass as `recipient.push_token`.

```bash
curl -s -X POST http://localhost:8080/v1/notification-requests \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <api_key>' \
  -d '{
    "idempotency_key": "upstream-service-push-001",
    "event_name": "service.alert",
    "template_key": "upstream-alert-v1",
    "channels": ["push"],
    "binding_set": "upstream-service-push",
    "recipient": {
      "user_id": "upstream-service",
      "push_token": "<device_token>"
    },
    "variables": {
      "title": "upstream alert",
      "body": "upstream sync failed for batch 42"
    },
    "metadata": {
      "app": "upstream-service"
    },
    "priority": "high"
  }'
```

## Operational note

If earlier local testing created duplicate `fcm-push` provider accounts, keep one managed file-backed account enabled and disable older env-backed duplicates.

## Security notes

- The Firebase JSON stays outside git and is mounted read-only into `connector-push`.
- Only the mounted file reference is stored in the control plane.
- The connector resolves the file at runtime through the shared secret resolver.
