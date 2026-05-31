# Managed Provider Platform Implementation Plan

This document turns the managed-provider platform design into a phased execution plan.

Use it together with:

- [Managed Provider Platform Design](/Users/Shaik/notifications/notification-control-plane/docs/architecture/managed-provider-platform.md)
- [System Architecture Dock](/Users/Shaik/notifications/notification-control-plane/docs/architecture/system-dock.md)
- [communication-engine Multi-Channel Integration](/Users/Shaik/notifications/notification-control-plane/docs/integrations/communication-engine-multi-channel.md)

The goal is to move from today's model:

- client-hosted or ad hoc connector assumptions
- provider bindings that point at `endpoint_url`
- loose `config_refs`

to the target model:

- first-party managed connectors
- provider accounts configured once during onboarding or deployment
- typed secret references
- runtime secret resolution at the connector boundary

## Outcome

At the end of this plan, a client should be able to:

1. register a provider account once
2. store provider credentials securely through the platform
3. configure routing and binding behavior
4. send canonical notification requests without provider secrets in the request
5. rely on the platform's first-party connectors for third-party provider delivery and callbacks

## Delivery Strategy

We should build this in layers so we do not destabilize the existing request flow.

The safest sequence is:

1. establish the new data model and onboarding APIs
2. add typed secret resolution
3. move the worker from connector endpoints to provider-account-based dispatch
4. evolve channel connectors into first-party managed adapters
5. harden callbacks, auditability, and operations

## Provider Extensibility Rules

We should make provider extension cheap by enforcing a small set of rules from the beginning.

### Every new provider must have

- a provider definition
- a provider account shape
- a typed secret material mapping
- one adapter inside the correct channel connector
- contract tests for send and callback behavior

### Every new provider must avoid

- new request fields for clients
- worker-specific provider branching
- bypassing the provider-definition schema
- leaking secret material into logs or request payloads

### Extension success looks like

- adding a provider mostly means adding one definition, one adapter, and one test suite
- the worker does not change for provider-specific request translation
- the callback gateway only learns a new provider route and verification rule, not a new system architecture
- operations can validate and disable a provider without code changes to clients

## Phase 0: Guardrails And Framing

This is the preparation phase before schema changes.

### Goals

- lock the target boundary
- preserve backward compatibility while the migration is in progress
- define what is in scope for v1 of the managed provider platform

### Decisions To Lock

- the worker remains provider-agnostic
- notification requests remain secret-free
- provider accounts are configured once during onboarding or deployment
- managed connectors are first-party platform services
- runtime secret resolution happens at the connector boundary

### Scope For The First Managed Release

- email
- SMS
- push
- webhook

WhatsApp should be designed for, but can ship after the first three if needed.

### Exit Criteria

- managed-provider architecture doc approved
- implementation plan approved
- initial provider list for first-party support chosen

## Phase 1: Storage And Domain Model Foundation

This phase adds the new durable models without changing the request contract.

### Storage Milestones

- add `provider_accounts`
- add `provider_secret_refs`
- add `channel_bindings_v2` or evolve `provider_bindings`
- add `callback_routes`
- add audit tables for provider-account and secret-reference changes

### Model Milestones

- add code models for:
  - `ProviderDefinition`
  - `ProviderAccount`
  - `SecretReference`
  - `ChannelBinding`
  - `CallbackRoute`
- keep the existing `ProviderBinding` path working during migration

### Migration Rules

- no plaintext secrets in Postgres
- support mapping existing `config_refs` into typed secret references
- retain existing binding priority behavior

### Exit Criteria

- schema migrations land cleanly
- storage layer supports new models
- old runtime behavior still works unchanged

## Phase 2: API And Onboarding Surface

This phase gives operators and clients a proper way to configure providers once.

### API Milestones

- add provider catalog endpoints
  - `GET /v1/provider-definitions`
- add provider account CRUD
  - `POST /v1/provider-accounts`
  - `GET /v1/provider-accounts`
  - `GET /v1/provider-accounts/{id}`
  - `PATCH /v1/provider-accounts/{id}`
  - `POST /v1/provider-accounts/{id}/disable`
- add channel binding CRUD that references provider accounts
- add callback route CRUD if callback routes are not fully derived from provider accounts

### Validation Milestones

- validate provider config against the provider definition schema
- validate secret material types before accepting configuration
- reject incomplete provider account setup before it reaches runtime

### Security Milestones

- add authentication for provider-management APIs
- add RBAC for provider account creation and modification
- add audit events for create, update, disable, and secret-ref changes

### Example Operator Flow

1. create a Twilio SMS provider account
2. attach secret refs for `account_sid` and `auth_token`
3. attach non-secret config like `from_number`
4. bind that provider account to `transactional-sms`
5. send normal notification requests without provider data in the payload

### Exit Criteria

- a provider account can be created end to end through API
- invalid provider configurations fail before runtime
- audit records exist for all provider-account changes

## Phase 3: Secret Resolver And Runtime Credential Path

This phase builds the secure runtime retrieval mechanism.

### Resolver Milestones

- add a secret resolver interface
- support at least two backends:
  - local/dev secret source
  - production secret manager
- support material types:
  - `secret_string`
  - `secret_json`
  - `secret_file`

### Connector Runtime Milestones

- connectors receive provider account IDs or resolved provider descriptors, not raw env-var-name maps
- connectors fetch typed runtime config through the resolver
- connectors cache secrets for a short TTL where safe
- connectors redact secrets from logs and error messages

### Reliability Milestones

- clear failure classification for:
  - secret not found
  - wrong material type
  - expired secret
  - backend unavailable
- metrics for secret fetch success and failure
- circuit-safe behavior when secret resolution fails repeatedly

### Exit Criteria

- worker no longer relies on `config_refs` for new bindings
- connectors can resolve provider credentials without request-time secret input
- secret resolution failures are observable and non-leaky

## Phase 4: Worker Refactor To Managed Dispatch

This phase moves provider selection onto the new model while keeping the worker provider-agnostic.

### Worker Milestones

- resolve `ChannelBinding` instead of raw `ProviderBinding`
- load provider account IDs from bindings
- stop requiring tenant-supplied connector `endpoint_url` for managed providers
- call first-party managed connectors by stable internal endpoint

### Backward Compatibility

- keep legacy bindings working during transition
- allow both:
  - legacy `endpoint_url` bindings
  - managed provider-account bindings
- prefer managed bindings when both exist for the same route only after an explicit rollout decision

### Routing Milestones

- preserve current binding set selection rules
- preserve failover-by-priority behavior
- preserve retries and dead-letter behavior
- attach provider account identity to attempt records for better debugging

### Exit Criteria

- managed provider-account dispatch works for at least one channel
- retry and failover behavior still matches existing semantics
- legacy bindings remain usable during rollout

## Phase 5: First-Party Managed Connectors

This phase turns connectors into real managed provider surfaces.

### Connector Shape

- one connector per channel
- multiple provider adapters inside each connector

### First Managed Set

- `connector-email`
  - one production-ready provider first
- `connector-sms`
  - Twilio
  - Gupshup
  - Karix
- `connector-push`
  - FCM
- `connector-webhook`
  - signed webhook delivery path

### Connector Milestones

- adapter registry inside each connector
- provider-specific request builders
- provider-specific auth handling
- provider-specific error classification
- normalized provider message ID extraction

### Delivery Contract Milestones

- stable normalized request from worker to connector
- stable normalized success/failure contract from connector to worker
- no provider-specific API shapes exposed outside the connector

### Exit Criteria

- at least one provider per target channel works through the managed model
- connectors are platform-owned, not client-owned
- provider-specific logic no longer lives outside the platform for supported providers

## Phase 6: Callback Gateway Hardening

This phase makes callbacks first-class and provider-account aware.

### Callback Milestones

- route callbacks by provider key and provider account
- support provider-specific verification modes
- resolve callback verification secrets through the secret resolver
- attach provider account identity to attempt lookup and callback processing

### Security Milestones

- signature verification where supported
- source validation where needed
- replay-safe callback handling where feasible

### Exit Criteria

- callbacks are verified for supported providers
- callback normalization remains provider-agnostic after verification
- callback failures are visible and auditable

## Phase 7: Observability, Audit, And Operations

This phase makes the managed provider platform trustworthy in production.

### Metrics Milestones

- provider account health metrics
- adapter-level send metrics
- secret resolution metrics
- callback verification metrics
- per-provider failure-class breakdown

### Audit Milestones

- audit provider account changes
- audit binding changes
- audit callback verification failures
- audit secret-resolution failures without logging secret values

### Operations Milestones

- dashboards for provider-account health
- alerts for callback failure spikes
- alerts for secret resolution failures
- runbooks for provider disable/failover

### Exit Criteria

- operators can identify provider, binding, secret, and callback issues quickly
- production failures are visible without exposing secret material

## Phase 8: Cutover And Cleanup

This phase removes the old bootstrap behavior once the managed path is proven.

### Cleanup Milestones

- deprecate loose `config_refs` for managed providers
- deprecate tenant-managed `endpoint_url` for supported first-party providers
- migrate docs from "build your own connector" default to "use managed providers" default
- keep custom connector support only as an explicit extension path

### Cleanup Rules

- do not break unsupported custom providers
- keep a custom connector escape hatch
- remove only the default path that made standard-provider support the client's responsibility

### Exit Criteria

- standard channels use managed provider accounts by default
- custom connectors remain possible but are no longer the normal path
- docs, examples, and operator flows all match the final product shape

## Workstreams By Service

### API

- provider catalog endpoints
- provider account CRUD
- channel binding CRUD
- callback route CRUD or derivation
- auth, RBAC, and audit hooks

### Storage

- new schema for provider accounts and secret refs
- migration path from `provider_bindings`
- audit tables
- callback-route storage

### Worker

- channel binding resolution by provider account
- managed dispatch path
- legacy compatibility layer
- attempt records with provider account identity

### Connectors

- adapter registry per channel
- provider-specific config validation
- secret resolver integration
- normalized error and callback metadata

### Callback Gateway

- provider-account-aware verification
- callback route lookup
- secret-backed signature validation

## Recommended Delivery Order

If we want the smallest useful sequence, build in this order:

1. storage foundation
2. provider account APIs
3. secret resolver
4. SMS managed path
5. callback verification for SMS
6. push managed path
7. email managed path
8. cleanup and deprecation

SMS is the best first managed channel because:

- it demonstrates multiple providers clearly
- it exercises failover and callback paths
- it avoids the extra credential-shape complexity of push as the first rollout

## Testing Plan

We should add tests at each phase, not after everything lands.

### Storage And API

- schema migration tests
- CRUD tests for provider accounts and bindings
- validation tests for config and secret-ref types

### Worker

- binding resolution tests with provider accounts
- legacy and managed compatibility tests
- failover and retry tests under managed bindings

### Connectors

- adapter contract tests
- provider config validation tests
- secret resolver failure tests

### Callbacks

- verification tests
- callback normalization tests by provider
- request-state recompute tests after verified callbacks

## Risks To Watch

- mixing legacy and managed paths can create confusing precedence
- provider adapter scope can expand too quickly
- secret resolver design can sprawl if material types are not kept small
- callback verification may differ widely across providers
- the first managed connector can become too generic if the adapter boundary is not kept clean

## Definition Of Done

We should consider the managed provider platform ready for general use when:

- clients configure providers once, outside request traffic
- secrets are never sent in notification requests
- managed connectors handle standard providers out of the box
- callbacks are verified and normalized by the platform
- legacy bootstrap paths are no longer the default for supported providers
- operations and auditability are strong enough for other teams to trust the platform
