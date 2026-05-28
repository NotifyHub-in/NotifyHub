# ADR 0002: Postgres + Kafka Runtime Model

## Status

Accepted

## Context

The platform needs a durable system of record plus an asynchronous processing backbone. It must support request persistence, retry workflows, callback correlation, and burst absorption.

## Decision

Use:

- PostgreSQL as the system of record
- Kafka as the asynchronous work backbone

PostgreSQL stores:

- requests
- attempts
- configuration resources
- scheduled retries
- dead letters
- provider health

Kafka carries:

- delivery work from API to worker
- replayed retry work back into the main worker path

## Consequences

Positive:

- clean separation between durable state and async work
- easier local inspection and debugging
- scalable worker processing model

Tradeoffs:

- two core infrastructure dependencies instead of one
- more operational overhead than a purely synchronous design
