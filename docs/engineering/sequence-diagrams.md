# Sequence Diagrams

This document collects the most important runtime sequences in one place.

## 1. New Notification Request

```mermaid
sequenceDiagram
    autonumber
    participant APP as Upstream service
    participant API as API
    participant DB as PostgreSQL
    participant K as Kafka

    APP->>API: POST /v1/notification-requests with language_code optional
    API->>API: Authenticate NotificationClient
    API->>DB: Create NotificationRequest
    API->>K: Publish DeliveryPlan
    API-->>APP: NotificationAccepted
```

## 2. Worker Delivery Attempt

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka
    participant W as Worker
    participant DB as PostgreSQL
    participant C as Connector
    participant P as Provider

    K->>W: DeliveryPlan
    W->>DB: Load request
    W->>DB: Load routing policy
    W->>DB: Load preference policy
    W->>DB: Load template by key + channel + language_code
    W->>DB: Load delivery policy
    W->>DB: Load provider binding
    W->>DB: Load provider account
    W->>C: POST /v1/send
    C->>C: Resolve provider_secret_refs
    C->>P: Provider API call
    P-->>C: Accepted or failed
    C-->>W: Normalized connector result
    W->>DB: Create or update DeliveryAttempt
```

## 3. Retry Path

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker
    participant DB as PostgreSQL
    participant R as Retry scheduler

    W->>W: Classify connector failure
    alt Retryable and attempts remain
        W->>DB: Create ScheduledRetry
        DB-->>R: Retry available later
    else Exhausted or terminal
        W->>DB: Create DeadLetterNotification
        W->>DB: Mark request failed
    end
```

## 4. Callback Reconciliation

```mermaid
sequenceDiagram
    autonumber
    participant P as Provider
    participant G as callback-gateway
    participant DB as PostgreSQL

    P->>G: POST callback payload
    G->>DB: Load CallbackRoute
    G->>G: Verify signature or secret
    G->>G: Normalize provider status
    G->>DB: Load DeliveryAttempt by provider_message_id
    G->>DB: Update DeliveryAttempt
    G->>DB: Recompute NotificationRequest status
    G-->>P: 202 Accepted
```

## 5. Push Delivery

```mermaid
sequenceDiagram
    autonumber
    participant APP as Upstream service
    participant API as API
    participant W as Worker
    participant PUSH as connector-push
    participant FCM as FCM

    APP->>API: push request with recipient.push_token
    API-->>APP: accepted
    W->>PUSH: send with provider_config + provider_secret_refs
    PUSH->>PUSH: resolve service_account_json
    PUSH->>PUSH: exchange token if needed
    PUSH->>FCM: push request
    FCM-->>PUSH: provider message ID
    PUSH-->>W: accepted
```

## 6. WhatsApp Template Delivery

```mermaid
sequenceDiagram
    autonumber
    participant APP as Upstream service
    participant API as API
    participant W as Worker
    participant WA as connector-whatsapp
    participant Provider as WhatsApp provider

    APP->>API: whatsapp request with template language
    API-->>APP: accepted
    W->>W: select template variant and merge template metadata
    W->>WA: send request
    WA->>WA: resolve secrets
    WA->>WA: choose gupshup or karix adapter
    WA->>Provider: template-aware provider payload
    Provider-->>WA: accepted + provider message ID
    WA-->>W: accepted
```

## 7. Managed Secret Resolution

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker
    participant C as Connector
    participant FS as Mounted secret files

    W->>C: provider_config + provider_secret_refs
    C->>C: validate plain config
    C->>FS: read file:// secret refs
    C->>C: merge config + resolved secrets
    C->>C: build provider request
```
