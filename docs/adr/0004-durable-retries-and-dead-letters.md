# ADR 0004: Durable Retries And Dead Letters

## Status

Accepted

## Context

The original retry path used in-process sleep. That is fragile because retry timing is lost on worker restarts and long backoffs tie up worker execution.

## Decision

Use Postgres-backed scheduled retries and first-class dead-letter records.

When a retryable failure happens:

- the worker stores a scheduled retry row with future availability
- a retry poller republishes ready retries back into Kafka

When retry budget is exhausted:

- the worker creates a dead-letter record
- operators can inspect and replay that record through the API

## Consequences

Positive:

- retry timing survives worker restarts
- dead letters become inspectable and replayable
- worker concurrency is not blocked by sleep-based backoff

Tradeoffs:

- retry lifecycle now depends on database state plus worker poller logic
- more moving parts to test and observe
