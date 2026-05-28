# ADR 0002: Use PostgreSQL For State And Kafka For Async Processing

## Status

Accepted

## Context

The platform needs:

- durable request and attempt state
- policy and template storage
- retry and dead-letter persistence
- asynchronous worker consumption
- buffering between API intake and delivery execution

## Decision

- PostgreSQL is the system of record for request state, attempts, policies, templates, retries, dead letters, and provider health.
- Kafka is the async backbone for notification-request intake and worker processing.

## Consequences

Positive:

- clear separation between durable business state and async transport
- easier operator inspection through SQL plus Kafka tooling
- natural fit for retries, replay, and observability

Tradeoff:

- two core infrastructure dependencies instead of one
- delayed retries still need a durable scheduling strategy beyond Kafka’s base feature set
