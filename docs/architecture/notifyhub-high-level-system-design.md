# NotifyHub High-Level System Design

This document is the compact system map for NotifyHub. It is intentionally high level so it can be pasted into a diagramming tool such as Eraser, or used as the top-level architecture reference for demos and onboarding.

## System Boundary

NotifyHub is a notification control plane.

- Upstream systems decide **what should happen**.
- NotifyHub decides **how the message gets delivered**.

The platform owns:

- request intake
- policy resolution
- routing
- template selection
- provider selection and failover
- delivery retries and dead letters
- callback reconciliation
- lifecycle webhooks
- observability

## High-Level Runtime View

```mermaid
flowchart LR
    U["Upstream systems / clients"] --> API["API / ingress"]
    API --> PG[(PostgreSQL)]
    API --> K["Kafka"]

    K --> W["Worker / reconciliation"]
    W --> PG

    W --> CE["Connectors"]
    CE --> E1["Email provider"]
    CE --> E2["SMS provider"]
    CE --> E3["WhatsApp provider"]
    CE --> E4["Push provider"]
    CE --> E5["Webhook target"]

    E1 --> CB["Callback gateway"]
    E2 --> CB
    E3 --> CB
    E4 --> CB
    E5 --> CB

    CB --> PG
    W --> WH["Lifecycle webhooks"]
    CB --> WH

    OBS["Prometheus / Grafana / Kafka UI / Adminer"] --- API
    OBS --- W
    OBS --- CB
    OBS --- PG
    OBS --- K
```

## Core Components

### API / ingress

Accepts notification requests and admin configuration.

Responsibilities:

- authenticate the client
- validate request shape
- persist canonical request state
- enqueue work to Kafka
- expose configuration APIs for provider accounts, bindings, routes, templates, preferences, and subscriptions

### Worker / reconciliation

Consumes delivery plans and decides how to send the notification.

Responsibilities:

- resolve routing policy
- resolve template variant by `template_key + channel + language_code`
- resolve provider binding and provider account
- apply user preferences
- render template content
- call the correct connector
- classify retryable vs terminal failures
- create retries or dead letters

### Connectors

Translate the generic control-plane request into provider-specific API calls.

Examples:

- email
- SMS
- WhatsApp
- push
- webhook

### Callback gateway

Receives provider delivery callbacks and reply events.

Responsibilities:

- verify callback authenticity
- normalize provider-specific payloads
- correlate the callback with a stored attempt
- update request and attempt state
- emit lifecycle webhook updates

### PostgreSQL

System of record for:

- notification requests
- delivery attempts
- provider accounts
- provider bindings
- routing policies
- preference policies
- templates
- delivery policies
- callback routes
- webhook subscriptions
- dead letters

### Kafka

Asynchronous work backbone.

Used for:

- request fan-out
- worker processing
- retry replay
- lifecycle event propagation

## End-To-End Flow

```mermaid
sequenceDiagram
    autonumber
    participant Client as Upstream system
    participant API as API / ingress
    participant DB as PostgreSQL
    participant MQ as Kafka
    participant Worker as Worker
    participant Conn as Connector
    participant Provider as External provider
    participant CB as Callback gateway

    Client->>API: POST /v1/notification-requests
    API->>DB: persist request
    API->>MQ: publish delivery plan
    API-->>Client: accepted

    MQ->>Worker: consume delivery plan
    Worker->>DB: load routing, templates, bindings, preferences
    Worker->>Conn: send normalized delivery payload
    Conn->>Provider: provider API request
    Provider-->>Conn: accepted or failed
    Conn-->>Worker: normalized result
    Worker->>DB: store attempt, retry, or dead letter

    Provider->>CB: callback or reply webhook
    CB->>DB: update attempt and request state
```

## Control-Plane Data Model

The key model chain is:

`client -> request -> route -> template -> binding set -> provider account -> connector -> provider callback`

The important point is that the business client only needs to know:

- event name
- channel
- recipient
- template key
- language code

Everything else is resolved by NotifyHub.

## What Makes It Generic

NotifyHub stays generic because the client service does not embed provider logic.

Instead, the control plane stores:

- provider account config
- secret references
- callback routes
- routing policies
- binding sets
- templates
- preferences

That means a new upstream service can integrate without knowing whether delivery happens through SMTP, Gupshup, Karix, FCM, or any future provider.

## Suggested Eraser Layout

If you want to recreate this in app.eraser.io, use this layout:

1. Left column: `Upstream systems / clients`
2. Center top: `API / ingress`
3. Center middle: `PostgreSQL` and `Kafka`
4. Center right: `Worker / reconciliation`
5. Far right: `Connectors` and external providers
6. Bottom row: `Callback gateway`, lifecycle webhooks, and observability

The main arrows should show:

- client request intake
- async worker processing
- provider delivery
- callback return path
- observability alongside the stack

