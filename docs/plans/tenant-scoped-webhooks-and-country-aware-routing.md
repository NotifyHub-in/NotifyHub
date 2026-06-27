# Tenant-Scoped Webhooks And Country-Aware Routing

## Problem Frame

The current notification platform has two related gaps:

1. Lifecycle webhooks are broadcast to every enabled webhook subscription, which means one tenant or client can observe status updates for requests that belong to another tenant/client.
2. SMS and WhatsApp provider selection does not yet use recipient country information, so a binding set cannot prefer the right provider for a destination country.

Both issues matter operationally, but the webhook broadcast problem is the urgent one because it creates a cross-tenant data exposure risk.

## Goals

- Deliver lifecycle webhooks only to the subscriptions that belong to the request owner.
- Preserve the current notification request flow and request payload shape.
- Add a deterministic way to choose an SMS or WhatsApp provider binding based on recipient country, while keeping the existing binding-set fallback behavior.
- Keep the implementation compatible with the current Postgres-backed storage model and worker/connector split.

## Scope

### In scope

- Webhook subscription ownership and dispatch filtering.
- Storage and API changes needed to scope webhook subscriptions to tenant/client ownership.
- Country-aware provider binding selection for SMS and WhatsApp.
- Request and binding validation updates needed to make the new routing behavior predictable.
- Integration and unit tests for the new behavior.

### Out of scope

- A full redesign of the notification request contract.
- Provider adapter changes inside `connector-sms` or `connector-whatsapp`.
- Inbound callback normalization changes in `callback-gateway`.
- Signed lifecycle webhooks, unless we decide to fold them into this follow-up.

## Current Behavior To Change

### Lifecycle webhook fanout

`libs/core/webhooks/notifier.go` currently loads all enabled webhook subscriptions and delivers every lifecycle event to all of them. The storage layer method used here is `ListEnabledWebhookSubscriptions`, which has no tenant or client scoping.

### Provider selection

`apps/worker/cmd/worker/main.go` currently resolves provider bindings by `channel` and `binding_set` only. For SMS and WhatsApp, the destination is already a phone number, but the worker does not yet infer destination country and does not use country metadata to choose among multiple bindings in the same set.

## Proposed Design

### 1. Scope lifecycle webhook delivery to the owning tenant/client

Use the source identity already stamped onto the request:

- `source_tenant_id`
- `source_client_id`

The webhook subscription model should gain ownership fields so subscriptions can be tied to a tenant and, more importantly, a listener client allowlist. Dispatch should only consider subscriptions whose listener client list includes the request’s owning client, even if the subscription lives in a different tenant.

Recommended behavior:

- A webhook subscription can still store tenant ownership for admin visibility and lifecycle management.
- A subscription stores an allowlist of listener client IDs.
- When a subscription is created, the registering client ID is automatically included in that allowlist.
- A request should fan out to every subscription whose allowlist contains the request’s `source_client_id`, regardless of tenant.
- If a request has no source client identity, the webhook should not be delivered unless we later define a safe anonymous listener mode.

This makes callback sharing explicit and client-driven, while still preserving tenant metadata for administration and auditing.

### 2. Add country-aware binding selection for SMS and WhatsApp

Keep the request payload unchanged and infer destination country from `recipient.phone` for SMS and WhatsApp requests.

Recommended behavior:

- Normalize the phone number to an E.164-like form before routing.
- Derive a destination country code from the phone number.
- Use a fixed canonical country value format on provider bindings, not free-form text.
- Add country metadata to `ProviderBinding` so a binding set can declare where that provider should be used.
- When a request supplies a `binding_set`, prefer the bindings in that set first.
- When no `binding_set` is supplied, derive the route from the destination country and use the matching country-scoped binding set or binding group.
- When multiple enabled bindings exist in the selected set or group:
  - prefer bindings that match the destination country
  - then prefer bindings with no country restriction as a fallback
  - then preserve the existing priority order inside each group

Fallback behavior:

- If the destination phone number cannot be parsed, keep the current binding-set behavior.
- If a channel does not use a phone destination, country routing should not apply.
- If no binding matches the country, fall back to the existing priority-based binding selection instead of failing the request immediately.
- If a request explicitly supplies a binding set, that explicit binding set overrides the derived country route.

This keeps the new behavior additive and avoids surprising breakage for existing tenants.

### 3. Persist provider-account provenance for each delivery attempt

Today, the request record does not store the provider account that actually handled delivery, and the delivery-attempt record only stores the connector name plus callback correlation fields. That means we can inspect delivery attempts, but we cannot reliably answer:

- which provider account was used for the winning attempt
- which provider account failed when a channel retries or fails over
- which provider account was selected if bindings change later

Recommended behavior:

- Record the resolved `provider_account_id` on each delivery attempt.
- Also record the winning `binding_id` or a stable binding reference so the historical path is not lost if the binding set is edited later.
- Keep the existing `connector_name` and `provider_message_id` fields so callback correlation and operational dashboards continue to work.
- Optionally surface the selected provider account on request detail APIs as a convenience read model, but treat the delivery attempt as the source of truth.

Fallback behavior:

- If the provider account cannot be resolved before send, the attempt should still record the failure path and the reason.
- If a request fails over across multiple bindings, every attempt should keep its own provider-account provenance.

This is the cleanest way to make provider usage auditable without trying to reconstruct it from mutable binding tables after the fact.

## Implementation Units

### Unit 1: Webhook subscription ownership and dispatch filtering

Files likely involved:

- `libs/contracts/notification/types.go`
- `migrations/0001_baseline.sql`
- `libs/storage/postgres/postgres.go`
- `apps/api/cmd/api/main.go`
- `libs/core/webhooks/notifier.go`
- `tests/integration/api_flow_test.go`

Planned changes:

- Extend `WebhookSubscription` with ownership fields.
- Add an allowlist field for listener client IDs.
- Add or update the subscription create/update API so subscriptions are stored with tenant ownership and optional client scoping.
- Replace the global enabled-subscription query with a scoped lookup that accepts the request owner.
- Make `NotifyRequestUpdated` use the scoped query before it builds the webhook payload list.
- Keep the payload shape unchanged so downstream consumers do not need a new contract.

Test scenarios:

- A request from tenant A only reaches webhook subscriptions owned by tenant A.
- A subscription delivers only to listener client IDs that are explicitly allowed.
- The registering client is included by default when a subscription is created.
- The notifier sends nothing when no matching subscriptions exist.
- Existing enabled subscriptions do not leak across tenants after the change.

### Unit 2: Country-aware provider selection

Files likely involved:

- `libs/contracts/notification/types.go`
- `migrations/0001_baseline.sql`
- `libs/storage/postgres/postgres.go`
- `apps/worker/cmd/worker/main.go`
- `tests/integration/api_flow_test.go`
- `apps/worker/cmd/worker/main_test.go`

Planned changes:

- Extend `ProviderBinding` with country metadata for route selection.
- Use a fixed country code enum or constrained code list on the binding model.
- Update the provider-binding CRUD surface so operators can declare the supported country or countries for a binding.
- Add a small phone-country resolver in the worker path that is used only for SMS and WhatsApp destinations.
- Teach binding selection to resolve explicit `binding_set` first, then country-derived routing, then existing priority ordering.
- Preserve the current behavior for channels and destinations that do not carry phone numbers.

Test scenarios:

- SMS with a phone in country X picks the binding in the same binding set marked for country X.
- WhatsApp with a phone in country X uses the same country-aware preference logic.
- A binding set with both country-specific and global bindings prefers the country-specific binding.
- A phone number that cannot be parsed falls back to the existing priority-based selection.
- Non-phone channels continue to ignore country routing.
- A request with an explicit `binding_set` uses that set even if the phone country points elsewhere.

### Country Code Format

- Use a canonical ISO-style country code format, such as `US` or `IN`, as the only accepted binding value.
- Normalize any derived country lookup to the same canonical format before matching bindings.
- Reject or normalize any admin payload that tries to use a non-canonical value.

### Unit 3: Provider-account provenance on delivery attempts

Files likely involved:

- `libs/contracts/notification/types.go`
- `libs/storage/postgres/postgres.go`
- `migrations/0001_baseline.sql`
- `apps/worker/cmd/worker/main.go`
- `apps/api/cmd/api/main.go`
- `tests/integration/api_flow_test.go`
- `apps/worker/cmd/worker/main_test.go`

Planned changes:

- Add provider-account provenance fields to the delivery-attempt contract and persistence layer.
- Populate those fields at the point where the worker resolves the binding and sends the request.
- Expose the additional provenance on request detail and attempt detail responses so operators can query it later.
- Keep the existing callback reconciliation path intact, using `provider_message_id` as the callback join key.

Test scenarios:

- A successful send records the provider account that was actually used.
- A failed send records the provider account that failed before or after provider acceptance.
- A retry or failover records a separate attempt per provider/account hop.
- Historical attempts continue to show the provider account even if the binding table changes later.
- Callback reconciliation still works from `provider_message_id` after the schema change.

## Sequencing

1. Land the webhook scoping change first.
2. Add the country-aware routing model and worker selection next.
3. Add provider-account provenance tracking on delivery attempts.
4. Update API and integration tests once the storage shape is settled.
5. Refresh docs and examples after the code paths are stable.

The sequencing matters because the webhook change is a security fix, while the country-aware routing is an optimization that should not widen exposure or alter request contracts.

## Validation Plan

### Webhook scoping

- Add integration coverage in `tests/integration/api_flow_test.go` for tenant-isolated subscription delivery.
- Add storage-level coverage for the new scoped webhook subscription query.
- Add a notifier unit test that proves the dispatcher only targets matching subscriptions.

### Country-aware routing

- Add worker tests in `apps/worker/cmd/worker/main_test.go` for destination-country ranking.
- Add contract or validation tests for the new binding metadata in `libs/contracts/notification/provider_accounts_test.go` if the model changes require it.
- Add integration coverage that exercises SMS and WhatsApp requests with competing bindings in the same binding set.

### Provider-account provenance

- Add worker tests that verify each attempt records the chosen provider account and binding.
- Add storage tests that verify the new columns are persisted and returned consistently.
- Add integration coverage that queries request/attempt details after a send and confirms the provider account can be read back.

## Risks And Tradeoffs

- Tenant/client scoping requires a migration plan for existing webhook subscriptions. We need a clear default for legacy rows so old data does not accidentally become global again.
- Country inference depends on phone numbers being provided in a parseable format. If the input is inconsistent, the worker must safely fall back rather than misroute.
- Country-aware routing can become ambiguous if too many bindings declare overlapping country coverage. The selection rules need to stay deterministic.
- If we only add tenant scoping and not client scoping, we reduce blast radius but do not fully satisfy the per-client control goal.
- Provider-account provenance adds schema and API surface area, so we should be careful not to blur the distinction between a request-level record and an attempt-level record.

## Open Questions

1. Do we want the webhook subscription allowlist to support tenant-wide listeners later, or keep it strictly client-allowlisted for now?
2. Should country metadata live on `ProviderBinding`, on `ProviderAccount`, or as a separate routing table?
3. If no country-specific binding matches, should we always fall back to the existing priority order, or should some binding sets fail closed?
4. Do we want to support explicit country overrides later, or keep routing entirely derived from the phone number?
5. Do we want request detail APIs to embed provider-account provenance directly, or keep that only on delivery attempts and derive it in clients?

## Recommended Assumptions For This Round

- Use tenant-scoped webhook ownership with optional client scoping.
- Use a subscription listener allowlist with the registering client ID included by default.
- Treat tenant as metadata only for webhook subscriptions; delivery is keyed off the allowed client list.
- Keep country metadata on `ProviderBinding` because that is where routing already happens.
- Use a canonical country-code enum/value set instead of free-form country text.
- Let explicit request `binding_set` override derived country routing.
- Fall back to current priority-based binding selection when country parsing or matching does not produce a winner.
- Derive country from the phone number only for SMS and WhatsApp.
- Record provider-account provenance on each delivery attempt, not just in logs or mutable binding tables.
