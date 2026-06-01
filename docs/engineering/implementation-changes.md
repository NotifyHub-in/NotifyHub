# Implementation Changes

This document records the major changes we made to evolve the Notification Control Plane into a managed-provider platform with production-shaped connector behavior.

## Summary Of The Changes

We changed the platform in eight big ways:

1. introduced managed provider accounts and typed secret references
2. introduced notification clients so any upstream service can integrate
3. moved provider-specific behavior into first-party connectors and adapters
4. added callback routes and provider-specific callback normalization
5. moved secret resolution to the connector boundary
6. added first-party push and WhatsApp support
7. added multilingual template variants with English fallback
8. removed the legacy binding configuration path

## 1. Managed Provider Accounts

### What changed

We added provider account models and persistence so provider credentials are registered once during onboarding rather than passed through requests.

Key files:

- [libs/contracts/notification/provider_accounts.go](/Users/Shaik/notifications/notification-control-plane/libs/contracts/notification/provider_accounts.go)
- [libs/storage/postgres/provider_accounts.go](/Users/Shaik/notifications/notification-control-plane/libs/storage/postgres/provider_accounts.go)
- [migrations/0004_provider_accounts.sql](/Users/Shaik/notifications/notification-control-plane/migrations/0004_provider_accounts.sql)
- [apps/api/cmd/api/main.go](/Users/Shaik/notifications/notification-control-plane/apps/api/cmd/api/main.go)

### Why it was needed

The old model was too loose:

- bindings pointed directly at endpoints
- secret references were ad hoc
- the platform could not cleanly represent a tenant-owned provider account

### What the new model gives us

- typed provider definitions
- typed secret references
- tenant-specific provider accounts
- cleaner provider validation
- cleaner callback setup

## 2. Notification Clients

### What changed

We added client identities and API-key auth for upstream services.

Key files:

- [libs/storage/postgres/notification_clients.go](/Users/Shaik/notifications/notification-control-plane/libs/storage/postgres/notification_clients.go)
- [migrations/0008_notification_clients.sql](/Users/Shaik/notifications/notification-control-plane/migrations/0008_notification_clients.sql)
- [apps/api/cmd/api/main.go](/Users/Shaik/notifications/notification-control-plane/apps/api/cmd/api/main.go)

### Why it matters

This made the control plane generic:

- `communication-engine` is only one client
- any other service can now register and send
- the request model is no longer tied to one upstream application

## 3. First-Party Connectors And Provider Adapters

### What changed

We expanded connectors into first-party managed channel services with provider-specific adapters.

Key files:

- [connectors/email/cmd/connector-email/main.go](/Users/Shaik/notifications/notification-control-plane/connectors/email/cmd/connector-email/main.go)
- [connectors/sms/cmd/connector-sms/main.go](/Users/Shaik/notifications/notification-control-plane/connectors/sms/cmd/connector-sms/main.go)
- [connectors/whatsapp/cmd/connector-whatsapp/main.go](/Users/Shaik/notifications/notification-control-plane/connectors/whatsapp/cmd/connector-whatsapp/main.go)
- [connectors/push/cmd/connector-push/main.go](/Users/Shaik/notifications/notification-control-plane/connectors/push/cmd/connector-push/main.go)
- [connectors/webhook/cmd/connector-webhook/main.go](/Users/Shaik/notifications/notification-control-plane/connectors/webhook/cmd/connector-webhook/main.go)

### Providers supported in code

- email: SMTP, SendGrid
- SMS: Twilio, Gupshup, Karix
- WhatsApp: Gupshup, Karix
- push: FCM
- webhook: outbound HTTP target

### Why it matters

Clients no longer need to write the standard connectors themselves.

## 4. Callback Routes And Callback Normalization

### What changed

We added callback-route storage and callback verification, then taught the callback gateway to parse provider-specific callback payloads.

Key files:

- [migrations/0006_callback_routes.sql](/Users/Shaik/notifications/notification-control-plane/migrations/0006_callback_routes.sql)
- [libs/storage/postgres/callback_routes.go](/Users/Shaik/notifications/notification-control-plane/libs/storage/postgres/callback_routes.go)
- [libs/contracts/notification/provider_callbacks.go](/Users/Shaik/notifications/notification-control-plane/libs/contracts/notification/provider_callbacks.go)
- [apps/callback-gateway/cmd/callback-gateway/main.go](/Users/Shaik/notifications/notification-control-plane/apps/callback-gateway/cmd/callback-gateway/main.go)

### Current callback coverage

- SMS: Gupshup, Karix
- WhatsApp: Gupshup, Karix
- Push: no callback reconciliation yet
- Email: partial, not fully complete for all providers

## 5. Secret Resolution At The Connector Boundary

### What changed

We moved secret resolution out of the worker and into the connectors.

Key files:

- [libs/core/secrets/resolver.go](/Users/Shaik/notifications/notification-control-plane/libs/core/secrets/resolver.go)
- [apps/worker/cmd/worker/main.go](/Users/Shaik/notifications/notification-control-plane/apps/worker/cmd/worker/main.go)
- connector main files for all channels
- [deployments/docker/compose.yml](/Users/Shaik/notifications/notification-control-plane/deployments/docker/compose.yml)

### Why it matters

This makes the runtime more production-shaped:

- connectors are now the provider secret boundary
- worker stays orchestration-focused
- file-backed secrets are mounted where they are consumed

## 6. Push And WhatsApp Expansion

### What changed

We added and hardened:

- `connector-push`
- `connector-whatsapp`
- template metadata support for provider-specific WhatsApp template behavior
- `recipient.push_token` support in the northbound API

Key files:

- [libs/contracts/notification/types.go](/Users/Shaik/notifications/notification-control-plane/libs/contracts/notification/types.go)
- [libs/core/render/render.go](/Users/Shaik/notifications/notification-control-plane/libs/core/render/render.go)
- [connectors/push/cmd/connector-push/main.go](/Users/Shaik/notifications/notification-control-plane/connectors/push/cmd/connector-push/main.go)
- [connectors/whatsapp/cmd/connector-whatsapp/main.go](/Users/Shaik/notifications/notification-control-plane/connectors/whatsapp/cmd/connector-whatsapp/main.go)
- [migrations/0007_template_metadata.sql](/Users/Shaik/notifications/notification-control-plane/migrations/0007_template_metadata.sql)

### Why it matters

The system can now represent:

- app-specific FCM projects as managed provider accounts
- WhatsApp template metadata that must match provider template behavior

## 7. Multilingual Templates

### What changed

We added `language_code` to the request and template models, then taught the API, storage layer, and worker to select the right template variant with English fallback.

Key files:

- [libs/contracts/notification/types.go](/Users/Shaik/notifications/notification-control-plane/libs/contracts/notification/types.go)
- [libs/storage/postgres/postgres.go](/Users/Shaik/notifications/notification-control-plane/libs/storage/postgres/postgres.go)
- [apps/api/cmd/api/main.go](/Users/Shaik/notifications/notification-control-plane/apps/api/cmd/api/main.go)
- [apps/worker/cmd/worker/main.go](/Users/Shaik/notifications/notification-control-plane/apps/worker/cmd/worker/main.go)
- [migrations/0010_template_language_codes.sql](/Users/Shaik/notifications/notification-control-plane/migrations/0010_template_language_codes.sql)

### Why it matters

This makes the platform behave more like CE and more like a real multi-language notification system:

- the upstream service can request a language explicitly
- templates can be stored per language
- if a request omits the language, English is used by default
- if a requested translation is missing, the worker falls back to English

## 7. Legacy Removal

### What changed

We removed the older `provider_bindings.config_refs` path and tightened the schema to require `provider_account_id`.

Key files:

- [migrations/0009_remove_legacy_provider_binding_config_refs.sql](/Users/Shaik/notifications/notification-control-plane/migrations/0009_remove_legacy_provider_binding_config_refs.sql)
- [libs/contracts/notification/types.go](/Users/Shaik/notifications/notification-control-plane/libs/contracts/notification/types.go)
- [libs/storage/postgres/postgres.go](/Users/Shaik/notifications/notification-control-plane/libs/storage/postgres/postgres.go)
- [apps/api/cmd/api/main.go](/Users/Shaik/notifications/notification-control-plane/apps/api/cmd/api/main.go)
- [apps/worker/cmd/worker/main.go](/Users/Shaik/notifications/notification-control-plane/apps/worker/cmd/worker/main.go)

### Why it matters

There is now one active provider-binding model:

- managed binding
- managed provider account
- connector-resolved secrets

## 8. Observability Expansion

### What changed

We added metrics around admin APIs and connector behavior, then added them to Grafana dashboards.

Key files:

- [apps/api/cmd/api/main.go](/Users/Shaik/notifications/notification-control-plane/apps/api/cmd/api/main.go)
- connector main files
- [deployments/docker/grafana/provisioning/dashboards/json/notification-control-plane-overview.json](/Users/Shaik/notifications/notification-control-plane/deployments/docker/grafana/provisioning/dashboards/json/notification-control-plane-overview.json)
- [deployments/docker/grafana/provisioning/dashboards/json/notification-control-plane-load-test.json](/Users/Shaik/notifications/notification-control-plane/deployments/docker/grafana/provisioning/dashboards/json/notification-control-plane-load-test.json)

### Important new metrics

- `admin_api_events_total`
- `connector_send_requests_total`
- `connector_provider_request_duration_seconds`

## Change Map By Migration

| Migration | Purpose |
|---|---|
| `0004_provider_accounts` | managed provider accounts and secret refs |
| `0005_provider_binding_provider_accounts` | provider bindings linked to provider accounts |
| `0006_callback_routes` | callback route registration and verification |
| `0007_template_metadata` | provider-specific template metadata |
| `0008_notification_clients` | generic upstream client onboarding |
| `0009_remove_legacy_provider_binding_config_refs` | remove old binding config path |
| `0010_template_language_codes` | multilingual templates with English fallback |

## Net Result

The platform now behaves like a real managed notification control plane:

- upstream services send canonical requests
- managed provider accounts hold provider setup
- connectors own provider execution and secret resolution
- callbacks reconcile back into durable request state
