# ADR 0003: Separate Connectors From Provider Bindings And Binding Sets

## Status

Accepted

## Context

A connector is code that knows how to speak to a provider API or protocol.

A running system also needs runtime configuration:

- which connector is active
- which endpoint should be used
- which bindings belong to a given route or provider pool
- how failover should work within a channel

## Decision

The platform will separate:

- connector implementation
- provider binding runtime configuration
- binding-set grouping for route-specific provider pools

Routing selects a channel and optionally a binding set. The worker then loads active bindings for that pool and applies priority-based failover.

## Consequences

Positive:

- provider changes do not require application code changes
- transactional vs marketing or regional provider pools can be modeled cleanly
- failover becomes a runtime concern rather than a compile-time decision

Tradeoff:

- more concepts for operators to understand
- more configuration to validate correctly
