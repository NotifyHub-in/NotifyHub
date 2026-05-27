# Project Status

This file tracks what has been built so far in the notification control plane and what still needs to be completed to make it production ready.

## Completed Work

- [x] Monorepo service layout
  Why: keeps the control plane, worker, callback gateway, connectors, shared contracts, and deployment assets versioned together while the platform is still evolving quickly.

- [x] Dockerized local stack
  Why: gives us a reproducible development environment with API, worker, callback gateway, connectors, Postgres, Kafka, Kafka UI, Adminer, Prometheus, and Grafana.

- [x] Canonical notification request model
  Why: client systems can publish generic notification intent instead of hardcoding vendor-specific delivery logic inside their business apps.

- [x] API intake and persistence
  Why: requests are accepted through a stable control-plane API and stored durably before async processing begins.

- [x] Kafka-backed worker flow
  Why: decouples acceptance from delivery, absorbs traffic bursts, and provides the foundation for scalable background processing.

- [x] Delivery attempt persistence
  Why: every concrete send attempt is recorded for auditability, debugging, retries, and provider callback correlation.

- [x] Provider connectors for `email`, `sms`, and `webhook`
  Why: connector processes isolate provider-specific send logic behind a consistent internal contract.

- [x] Provider bindings
  Why: runtime configuration chooses which connector endpoint is active for a channel without requiring code changes.

- [x] Provider failover by priority
  Why: when one binding fails, the worker can fall through to a lower-priority binding for the same route.

- [x] Routing policies
  Why: events can resolve to channels declaratively instead of each client system hardcoding channel choice.

- [x] Preference policies
  Why: the platform can suppress delivery by user and channel centrally instead of leaving that responsibility to each application.

- [x] Template storage and rendering
  Why: templates become reusable platform resources instead of being embedded in client code or worker logic.

- [x] Safer template engine using `text/template` under a `{{variable}}` user syntax
  Why: users keep a simple templating experience while the backend gains stronger validation and missing-variable protection.

- [x] Delivery policies with retries and backoff
  Why: transient send failures can be retried consistently without every client implementing its own retry logic.

- [x] Idempotency handling
  Why: duplicate client submissions do not create duplicate logical notifications, and conflicting replays are rejected cleanly.

- [x] Callback normalization
  Why: provider-specific delivery updates are translated into a unified internal delivery state.

- [x] Lifecycle webhooks
  Why: downstream systems can subscribe to request state changes instead of polling the control plane repeatedly.

- [x] Outbound webhook delivery tracking
  Why: webhook notifications themselves are now observable and retryable instead of being fire-and-forget.

- [x] Request expiry / TTL
  Why: stale notifications can be dropped safely when they are no longer valid after queueing delays or processing lag.

- [x] Observability foundation
  Why: the system exposes metrics for request outcomes, attempts, final states, callbacks, DB timings, and service health.

- [x] Grafana and Prometheus integration
  Why: latency, queue lag, retries, failovers, callback activity, and service resource metrics can be inspected visually during testing.

- [x] Load test harness
  Why: we can exercise happy paths and edge cases repeatedly and see the platform’s behavior under concurrent traffic.

- [x] Binding-set aware provider selection
  Why: routing can now select not just a channel, but also a provider pool such as `transactional-email` or `marketing-email`.

- [x] Default binding-set fallback
  Why: if no binding set is provided, the platform still behaves predictably by using default channel bindings instead of forcing every request to specify a group.

## Current Selection Rules

Provider selection now works like this:

1. If the request includes a `binding_set`, the worker uses that set.
2. Otherwise, if the routing policy includes a `binding_set`, the worker uses that set.
3. Otherwise, the worker uses default bindings for the channel where `binding_set = ""`.
4. If no explicit default bindings exist yet, the worker falls back to all enabled bindings for that channel for backward compatibility.

## Pending Work For Production Readiness

### Core Platform Hardening

- [ ] Replace the single evolving SQL file with a proper migration/versioning workflow
- [ ] Add dead-letter queue handling and replay tooling
- [ ] Move retry timing off in-process sleeps to a scheduled/requeue-based retry model
- [ ] Define a clearer connector capability and error taxonomy contract
- [ ] Add stronger partial-failure semantics for multi-channel notification requests
- [ ] Add request validation rules for channel-recipient compatibility
- [ ] Add provider health state and circuit-breaker behavior
- [ ] Add cleanup/retention jobs for old audit data and webhook attempts

### Security

- [ ] Add authentication for control-plane APIs
- [ ] Add authorization / RBAC for config and query endpoints
- [ ] Add secure secret management for provider credentials
- [ ] Add signed outbound lifecycle webhooks
- [ ] Add inbound callback signature verification
- [ ] Add audit logs for policy, template, provider, and subscription changes
- [ ] Add API rate limiting and abuse protection

### Reliability And Correctness

- [ ] Add unit tests around routing, binding-set selection, retries, failover, preferences, expiry, and idempotency
- [ ] Add integration tests for end-to-end provider flows and callback loops
- [ ] Add chaos/failure tests for Kafka, Postgres, and connector failures
- [ ] Define explicit delivery guarantees and replay semantics in docs
- [ ] Add more robust queue partitioning and consumer scaling strategy
- [ ] Add safer handling for connector timeouts and hung requests

### Operations

- [ ] Add alert rules for lag, failure spikes, retry spikes, DB slowdowns, and callback failures
- [ ] Add structured correlation IDs across API -> worker -> callback -> webhook flows
- [ ] Add tracing across services
- [ ] Improve dashboards for production operations, not just load testing
- [ ] Add backup and restore guidance for Postgres
- [ ] Add capacity planning guidance for Kafka, DB pools, and worker replicas
- [ ] Add log retention and operational runbooks

### Deployment And Release

- [ ] Add environment-specific configuration strategy beyond local Docker
- [ ] Add Kubernetes manifests / Helm or equivalent production deployment packaging
- [ ] Add release versioning and changelog discipline
- [ ] Add CI checks for tests, formatting, and image builds
- [ ] Add image publishing and release automation

### OSS And Product Readiness

- [ ] Refresh the top-level README to match the current platform instead of the old scaffold state
- [ ] Add API documentation / OpenAPI spec
- [ ] Add connector SDK usage docs
- [ ] Add example client integrations for common product patterns
- [ ] Add ADRs for key architecture decisions
- [ ] Add operator documentation for local, staging, and production usage
- [ ] Add a roadmap section for future areas like workflows, approval gates, and regional routing

## Suggested Next Phase

If we resume from here, the highest-value next sequence is:

1. Authentication and secret management
2. Real migration/versioning discipline
3. Scheduled retries + DLQ + replay tooling
4. Test coverage and CI hardening
5. README/API docs/OSS packaging cleanup
