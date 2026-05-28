# ADR 0003: Binding Sets And Provider Routing

## Status

Accepted

## Context

A single channel can have multiple providers, and different notification classes may need different provider pools. Routing only by channel is not enough for real-world setups like:

- transactional email vs marketing email
- regional SMS providers
- critical webhook endpoints vs general webhook endpoints

## Decision

Separate routing into two stages:

1. routing policies choose channel and optional binding set
2. provider bindings choose the ordered connector list within that channel and binding set

Default behavior remains available through the empty binding set.

## Consequences

Positive:

- supports multiple provider pools per channel cleanly
- keeps channel routing simple while allowing richer provider topology
- enables failover within a selected provider pool

Tradeoffs:

- more concepts for operators to understand
- requires clear documentation of default binding-set fallback
