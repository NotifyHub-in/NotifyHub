# Roadmap

This roadmap is meant to show where the notification control plane can grow next without blurring the current product boundary.

## Current Boundary

Today the platform is focused on:

- notification intake
- policy-based routing
- templates
- preferences
- provider selection
- retries and dead letters
- callbacks
- observability

It is not currently trying to be a full workflow, journey, or campaign platform.

## Near-Term Priorities

### Production Hardening

- API authentication and authorization
- secret-manager integration
- callback signature verification
- signed lifecycle webhooks
- backup, restore, and retention guidance
- stronger deployment packaging

### Reliability

- deeper automated test coverage
- chaos testing for Kafka, Postgres, and providers
- stronger multi-channel partial-failure semantics
- improved queue partitioning and scaling guidance

### Operations

- production-grade alerts
- tracing and correlation IDs
- richer runbooks
- capacity planning guidance

## Medium-Term Product Expansion

These are areas that fit the control-plane model without turning it into a full workflow builder.

### Approval Gates

Useful for:

- sensitive outbound notifications
- internal operational alerts
- high-cost or high-risk message classes

The idea is not a generic BPM engine. It is a focused approval checkpoint in the notification delivery path.

### Regional And Compliance-Aware Routing

Useful for:

- country-specific providers
- region-specific delivery rules
- data-residency-aware routing
- channel restrictions by market

This expands the routing model without changing the control-plane boundary.

### Better Provider Capability Modeling

Useful for:

- selecting providers by capability
- understanding fallback compatibility
- clearer binding validation

### Richer Operator Workflows

Useful for:

- DLQ bulk replay
- provider maintenance mode
- circuit reset policies
- replay and retry audits

## Deliberately Deferred Areas

These are intentionally not immediate roadmap priorities because they would widen the product boundary too much right now.

- drag-and-drop journey builder
- low-code workflow canvas
- marketing campaign studio
- complex segment builder UI
- full multi-tenant enterprise admin suite

## Product Direction Summary

The intended direction is:

`be the notification control plane that product teams build on`

not:

`be every kind of customer-engagement product at once`
