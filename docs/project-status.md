# Project Status

This file tracks what has been built so far in the NotifyHub and what still needs to be completed to make it production ready.

For the detailed production-readiness plan, sequencing, and go-live checklist, see [Production Release Plan](/docs/production-release-plan.md).

## Completed Work

- [x] Monorepo service layout
  Why: keeps the control plane, worker, callback gateway, connectors, shared contracts, and deployment assets versioned together while the platform is still evolving quickly.

- [x] Dockerized local stack
  Why: gives us a reproducible development environment with API, worker, callback gateway, connectors, Postgres, Kafka, Kafka UI, Adminer, Prometheus, and Grafana.

- [x] Versioned migration runner
  Why: schema changes are now applied in a tracked order through versioned SQL files, which gives adopters and future deploys a safer database upgrade path.

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

- [x] Env-backed provider secret references
  Why: provider bindings can reference runtime secret names without storing actual API keys or tokens in Postgres, which is a safer bridge until full secret-manager integration exists.

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

- [x] Scheduled retries with dead-letter and replay flow
  Why: retry timing now survives worker restarts, permanently failed notifications are inspectable, and operators have a first-class way to replay exhausted requests safely.

- [x] Connector error taxonomy and retry classification
  Why: the worker can now distinguish transient failures from permanent or misconfigured ones, which means only retryable errors consume retry budget while invalid requests and bad credentials fail fast into DLQ with clearer reasons.

- [x] Provider health state and circuit-breaker behavior
  Why: repeated retryable provider failures now open a persisted circuit on the affected binding, which lets the worker skip unhealthy providers during cooldown windows and recover automatically after a successful probe instead of repeatedly hammering the same outage.

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

- [x] Worker unit-test coverage foundation
  Why: the worker’s core helper decisions around retry classification, provider config resolution, and circuit-skip behavior now have automated test coverage, which makes reliability changes safer.

- [x] Live integration-test foundation
  Why: the local stack is now exercised through real request lifecycle tests covering accepted requests, retry scheduling, dead-letter replay, callback normalization, failover, and circuit-open skip behavior.

- [x] Basic CI workflow for formatting and Go tests
  Why: the repo now has a baseline GitHub Actions gate for formatting and `go test`, which reduces the chance of obvious regressions landing silently.

- [x] Core documentation bundle
  Why: the repo now has a complete first-pass docs set covering architecture, operations, extension points, examples, roadmap, and architecture decisions instead of relying on scattered explanations across conversations.

## Current Selection Rules

Provider selection now works like this:

1. If the request includes a `binding_set`, the worker uses that set.
2. Otherwise, if the routing policy includes a `binding_set`, the worker uses that set.
3. Otherwise, the worker uses default bindings for the channel where `binding_set = ""`.
4. If no explicit default bindings exist yet, the worker falls back to all enabled bindings for that channel for backward compatibility.

## Pending Work For Production Readiness

### Core Platform Hardening

- [ ] Add stronger partial-failure semantics for multi-channel notification requests
- [ ] Add request validation rules for channel-recipient compatibility
- [ ] Add cleanup/retention jobs for old audit data and webhook attempts

### Security

- [ ] Add authentication for control-plane APIs
  Deferred for now because the current deployment is internal-only, but it should be added before wider shared or external exposure.
- [ ] Add authorization / RBAC for config and query endpoints
- [ ] Replace env-backed secret refs with full secret-manager integration for provider credentials
- [ ] Add signed outbound lifecycle webhooks
- [ ] Add inbound callback signature verification
- [ ] Add audit logs for policy, template, provider, and subscription changes
- [x] Add API rate limiting and abuse protection
  Config-driven IP, client, read, and admin throttles now protect the northbound API and admin/query surfaces.

### Reliability And Correctness

- [ ] Add deeper unit tests across routing, binding-set selection, retries, failover, preferences, expiry, and idempotency
  Progress: worker helper coverage now exists for retry classification, provider config resolution, and circuit-skip decisions, but broader logic-path coverage should still be expanded.
- [ ] Add deeper integration tests for end-to-end provider flows and callback loops
  Progress: live integration coverage now includes accepted requests, retry scheduling, dead-letter replay, callback normalization, failover, and circuit-open skip behavior. More chaos-style and wider matrix coverage is still pending.
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

- [x] Add environment-specific configuration strategy beyond local Docker
  Why: the Helm chart and production deployment guide now separate production values, secret handling, and service wiring from the local Docker compose assumptions.
- [x] Add Kubernetes manifests / Helm or equivalent production deployment packaging
  Why: the repository now includes a Helm chart for the API, worker, callback gateway, connectors, and the migration job, plus a production deployment guide and rollback notes.
- [ ] Add release versioning and changelog discipline
- [x] Add basic CI checks for tests and formatting
  Why: the repo now runs a baseline GitHub Actions workflow for format checking and the normal Go test suite.
- [x] Add CI checks for image builds and broader release validation
  Why: the CI workflow now validates Docker builds for every shipped service image so release regressions are caught before a tag is published.
- [x] Add image publishing and release automation
  Why: a tag-driven GitHub Actions release workflow now publishes immutable GHCR images for the API, worker, callback gateway, migrate job, and all connectors.

### OSS And Product Readiness

- [x] Refresh the top-level README to match the current platform instead of the old scaffold state
  Why: the repo front door now explains the actual control-plane capabilities, local workflow, testing commands, and docs entry points instead of underselling the platform as a thin scaffold.
- [x] Add API documentation / OpenAPI spec
  Why: adopters can now understand the current API surface and resource shapes without reverse-engineering handler code.
- [x] Add connector integration docs
  Why: provider contributors now have a single guide for the current connector contract, failure classification expectations, and extension workflow instead of reverse-engineering the reference connectors.
- [x] Add example integrations for common product patterns
  Why: adopters can now see concrete northbound usage patterns for transactional email, OTP SMS, and operational webhook alert flows without reading source code first.
- [x] Add ADRs for key architecture decisions
  Why: the repo now records the main architectural choices around platform boundaries, storage and messaging, retries, provider bindings, and circuit breakers so contributors do not have to infer them from code history.
- [x] Add operator documentation for local, staging, and production usage
  Why: operators now have a single guide for stack startup, debugging, retries, dead letters, provider health, and observability entry points.
- [x] Add a roadmap section for future areas like workflows, approval gates, and regional routing
  Why: future expansion areas are now documented explicitly so the repo has a product direction reference without blurring the current platform boundary.

## Suggested Next Phase

If we resume from here, the highest-value next sequence is:

1. Release versioning and changelog discipline
2. Deeper test, chaos, and scaling hardening
3. Alerting, tracing, and operator runbook maturity
4. Tighten rollout automation with changelog generation and signed release notes

## Phase Tracking: Tenant-Scoped Webhooks And Country-Aware Routing

This section tracks the phase that will follow the current planning pass.

### Current Status

- [x] Problem frame identified
  Why: lifecycle webhook fanout is currently tenant-blind, and provider routing still needs country-aware selection for SMS and WhatsApp.

- [x] Implementation plan drafted
  Why: the proposed change set is captured in [Tenant-Scoped Webhooks And Country-Aware Routing](/docs/plans/tenant-scoped-webhooks-and-country-aware-routing.md).

- [x] Key behavioral decisions captured
  Why: the phase now has a documented stance on allowed-client webhook delivery, explicit binding-set override, derived phone-country routing, and provider-account provenance tracking.

### Remaining Work

- [ ] Scope lifecycle webhook delivery by allowed client list
  Remaining: wire subscription ownership and listener allowlists into webhook dispatch so only permitted client IDs receive lifecycle events.

- [ ] Add explicit `binding_set` override for channel routing
  Remaining: keep derived phone-country routing as the default, but honor an explicit binding set when the request provides one.

- [ ] Add canonical country-code routing metadata
  Remaining: define and persist the country value format used by binding configuration and worker matching.

- [ ] Persist provider-account provenance on delivery attempts
  Remaining: record the winning provider account and binding for each attempt so operators can query historical send paths.

- [ ] Add tests and docs for the new phase
  Remaining: cover tenant-sharing webhook delivery, country-aware provider selection, and provider provenance in integration coverage and updated docs.
