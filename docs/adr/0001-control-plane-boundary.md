# ADR 0001: Control Plane Boundary

## Status

Accepted

## Context

The project is intended to be reusable across many products and teams. The main design risk is letting business-specific workflow logic leak into the platform until it becomes a hard-to-adopt monolith.

## Decision

The platform will operate as a notification control plane.

Business systems remain responsible for:

- deciding why a notification should happen
- deciding who should be notified
- deciding which business event or template key applies

The platform remains responsible for:

- intake
- policy evaluation
- template rendering
- provider selection
- retries
- dead letters
- callbacks
- observability

## Consequences

Positive:

- reusable across many client domains
- clearer boundary between domain logic and delivery infrastructure
- easier OSS adoption

Tradeoffs:

- some users may expect built-in workflow automation that is intentionally out of scope
- richer business orchestration must stay outside the core for now
