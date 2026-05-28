# ADR 0005: Provider Health And Circuit Breakers

## Status

Accepted

## Context

Without a circuit breaker, the worker can keep hammering a provider that is already known to be unhealthy, consuming retry budget and increasing latency for other requests.

## Decision

Use `github.com/sony/gobreaker` for circuit-breaker mechanics and persist provider binding health in Postgres for operator visibility and manual reset.

Repeated retryable failures can open a circuit on a binding. While open:

- the worker skips that binding during cooldown
- the request may fail over to another binding or be scheduled for retry

Success later closes the circuit.

## Consequences

Positive:

- avoids repeatedly discovering the same outage on the hot path
- gives operators visibility and reset control
- integrates naturally with provider failover

Tradeoffs:

- adds another stateful behavior layer that must be explained and monitored
- requires alignment between transient-failure classification and breaker state changes
