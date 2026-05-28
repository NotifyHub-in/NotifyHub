# ADR 0004: Use Durable Scheduled Retries, Dead Letters, And Replay

## Status

Accepted

## Context

Simple in-process sleep-based retries are fragile:

- restart-sensitive
- hard to inspect
- poor fit for longer backoff
- difficult to replay or operate on explicitly

## Decision

The platform will persist scheduled retries and dead letters in PostgreSQL.

The worker:

- records retryable failures as scheduled retries
- republishes ready retries back through the async pipeline
- records exhausted requests as dead letters
- supports explicit replay through the control-plane API

## Consequences

Positive:

- retry state survives restarts
- operators can inspect and replay failures explicitly
- metrics can expose backlog and age of retry/DLQ state

Tradeoff:

- more state tables and worker logic
- replay semantics must stay well-documented
