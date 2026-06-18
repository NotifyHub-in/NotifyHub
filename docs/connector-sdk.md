# Connector Integration Guide

This project currently uses a connector pattern rather than a published reusable SDK package. The reference connectors in [connectors](/connectors) are the best implementation examples today, and the target product direction is for the platform to ship first-party connectors for the standard channels and providers.

This guide describes the contract and expectations for building a new connector or adapter.

## What A Connector Is

A connector is a small service that:

- accepts a normalized send request from the worker
- translates it into a provider-specific API call
- returns a normalized success or failure response
- exposes `healthz` and `metrics`

The connector should not own platform policy. It should not decide:

- routing
- retries
- failover
- dead-lettering
- binding-set selection

Those remain worker responsibilities.

## Current Connector Responsibilities

A connector should:

- validate the minimal provider-specific requirements for a send
- call the external provider
- return a provider message ID when accepted
- classify failures into platform-friendly categories

## Current Runtime Pattern

The worker discovers a connector through a provider binding:

- `connector_name`
- `endpoint_url`
- `channel`
- `binding_set`
- `priority`
- `provider_account_id`

The worker sends non-secret provider config plus typed provider secret references to the connector. The connector resolves those secrets at send time.

## Expected Endpoints

At minimum, a connector should expose:

- `GET /healthz`
- `GET /metrics`
- `POST /v1/send`

The reference connectors also expose a capabilities endpoint pattern where useful. If you add one, keep it informational and do not let the worker depend on it for critical-path sends.

## Send Request Shape

The worker sends a normalized payload that conceptually includes:

- request and attempt identifiers
- channel
- recipient destination
- rendered subject and body
- metadata
- provider configuration resolved from secret refs

Typical fields include:

- `request_id`
- `attempt_id`
- `channel`
- `recipient`
- `subject`
- `body`
- `variables`
- `metadata`
- `provider_config`

The exact schema is defined in code and reflected by the current connector implementations.

## Success Response Expectations

On acceptance, the connector should return enough information for the platform to correlate later callbacks:

- accepted status
- provider message ID
- optional provider-specific metadata

The provider message ID is especially important because the callback gateway uses it to correlate delivery updates back to the original attempt.

## Failure Response Expectations

Failure classification matters a lot because it drives retry behavior, dead-letter behavior, and circuit-breaker state.

Connectors should classify failures into categories such as:

- `transient`
- `rate_limited`
- `invalid_request`
- `misconfigured`
- `unauthorized`
- `permanent`

The platform then reacts differently:

- retryable classes can schedule retries
- invalid or permanent classes fail fast
- repeated retryable failures contribute to circuit opening

## Design Guidelines

### Keep Connectors Stateless

Connectors should not store delivery state locally. That state belongs in the control plane.

### Keep Business Rules Out

Do not embed client-specific routing or suppression logic in a connector. If a provider requires a specific field or shape, translate it there, but keep business policy in the worker and configuration resources.

### Validate Provider Config Early

If required provider config is missing or clearly invalid, return a structured non-retryable failure. That gives the operator a clean error instead of consuming retry budget.

### Use Timeouts

External provider calls should always have explicit timeouts. Hung connector calls can stall worker throughput and distort retry behavior.

### Return Stable Error Shapes

Error message text can evolve, but classification should stay stable. The worker and metrics depend on it.

## How To Add A New Connector

1. Create a new directory under [connectors](/connectors).
2. Add a small HTTP service with `healthz`, `metrics`, and `POST /v1/send`.
3. Reuse the patterns from:
   - [connectors/email/cmd/connector-email/main.go](/connectors/email/cmd/connector-email/main.go)
   - [connectors/sms/cmd/connector-sms/main.go](/connectors/sms/cmd/connector-sms/main.go)
   - [connectors/webhook/cmd/connector-webhook/main.go](/connectors/webhook/cmd/connector-webhook/main.go)
   - [connectors/push/cmd/connector-push/main.go](/connectors/push/cmd/connector-push/main.go)
4. Add a Docker service entry in [deployments/docker/compose.yml](/deployments/docker/compose.yml) if you want it in the local stack.
5. Add provider bindings through the API that point to the connector.
6. Add integration and load-test coverage for the new delivery path.

## Product Direction

For the standard channels and common third-party providers, the platform should own the connectors and provider adapters.

Clients should only have to:

- register provider accounts
- provide non-secret configuration values
- store secret references securely during onboarding or deployment
- choose routing and binding policies

Clients should only build custom connectors when a provider is not supported yet or a very specialized integration is needed.

## Recommended Future Work

This guide is intentionally practical for the current codebase. Later, the project can grow into a stronger reusable SDK by adding:

- a shared connector contract package
- helper libraries for request parsing and response formatting
- test fixtures for connector conformance
- callback verification helpers
- provider capability metadata conventions

For now, the repo’s best extension path is:

`follow the reference connectors and keep the worker/connector boundary clean`
