# AFS-admin Push Integration

This guide adds AFS-admin push to the Notification Control Plane without introducing any AFS-admin-specific runtime code.

The control plane already has a generic `fcm-push` provider path. For AFS-admin, we only need to:

1. mount the Firebase admin SDK JSON into the `connector-push` container through the control plane secret mount,
2. register a `fcm-push` provider account that points at that mounted file,
3. bind that provider account to the `connector-push` service,
4. add a routing policy and preference policy, and
5. send a push request with the recipient device token.

## Default paths and names

- source Firebase JSON: `/Users/Shaik/notifications/communication-engine/src/main/java/farm/nurture/communication/engine/resource/firebase_config/afs_admin_fcm_content_adminsdk.json`
- host secret directory: `${NOTIFICATION_SECRETS_DIR:-/tmp/notification-control-plane-secrets}`
- mounted secret path in connector: `file:///run/notification-secrets/afs_admin_fcm_content_adminsdk.json`
- provider key: `fcm-push`
- binding set: `afs-admin-push`
- routing event: `afs.admin.alert`
- template key: `afs-admin-alert-v1`

If your CE checkout is in a different location, copy the Firebase JSON into `NOTIFICATION_SECRETS_DIR` before starting the stack.

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
  -d '{"tenant_id":"afs-admin","client_name":"afs-admin-service","allowed_channels":["push"],"enabled":true}'
```

### 2. Create the FCM provider account

```bash
curl -s -X POST http://localhost:8080/v1/provider-accounts \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "afs-admin",
    "provider_key": "fcm-push",
    "display_name": "AFS Admin FCM",
    "channel": "push",
    "enabled": true,
    "config": {"project_id": "afs-admin"},
    "secret_refs": {
      "service_account_json": {
        "ref": "file:///run/notification-secrets/afs_admin_fcm_content_adminsdk.json",
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
    "binding_set": "afs-admin-push",
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
    "event_name": "afs.admin.alert",
    "channels": ["push"],
    "binding_set": "afs-admin-push",
    "enabled": true,
    "priority": 10
  }'
```

### 5. Enable push for the target user

```bash
curl -s -X POST http://localhost:8080/v1/preference-policies \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "afs-admin",
    "channel": "push",
    "is_enabled": true
  }'
```

### 6. Create the push template

```bash
curl -s -X POST http://localhost:8080/v1/templates \
  -H 'Content-Type: application/json' \
  -d '{
    "template_key": "afs-admin-alert-v1",
    "channel": "push",
    "subject_template": "{{title}}",
    "body_template": "{{body}}",
    "metadata": {"app": "afs-admin"},
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
    "idempotency_key": "afs-admin-push-001",
    "event_name": "afs.admin.alert",
    "template_key": "afs-admin-alert-v1",
    "channels": ["push"],
    "binding_set": "afs-admin-push",
    "recipient": {
      "user_id": "afs-admin",
      "push_token": "<device_token>"
    },
    "variables": {
      "title": "AFS-admin alert",
      "body": "AFS-admin sync failed for batch 42"
    },
    "metadata": {
      "app": "afs-admin"
    },
    "priority": "high"
  }'
```

## Operational note

If earlier local testing created duplicate AFS-admin `fcm-push` provider accounts, keep one managed file-backed account enabled and disable older env-backed duplicates.

## Security notes

- The Firebase JSON stays outside git and is mounted read-only into `connector-push`.
- Only the mounted file reference is stored in the control plane.
- The connector resolves the file at runtime through the shared secret resolver.
