# Upstream Service Multi-Channel Integration

This guide shows how an upstream service can integrate with NotifyHub across multiple channels:

- email
- SMS
- WhatsApp
- push

The control plane keeps the delivery mechanics. The upstream service keeps business intent, audience logic, and the event that triggered the notification.

## Big Picture

```mermaid
flowchart LR
    APP["upstream service"]
    API["NotifyHub API"]
    PG[(PostgreSQL)]
    K["Kafka"]
    W["Worker / reconciliation"]
    EB["Email bindings"]
    SB["SMS bindings"]
    WB["WhatsApp bindings"]
    PB["Push bindings"]
    CONN1["Connector email"]
    CONN2["Connector sms"]
    CONN3["Connector whatsapp"]
    CONN4["Connector push"]
    EP1["Email provider"]
    EP2["SMS provider"]
    EP3["WhatsApp provider"]
    EP4["Push provider"]
    CB["Callback gateway"]
    VW["Lifecycle webhooks"]

    APP --> API
    API --> PG
    API --> K
    K --> W
    W --> PG

    W --> EB
    W --> SB
    W --> WB
    W --> PB

    EB --> CONN1 --> EP1 --> CB
    SB --> CONN2 --> EP2 --> CB
    WB --> CONN3 --> EP3 --> CB
    PB --> CONN4 --> EP4 --> CB

    CB --> PG
    W --> VW
    CB --> VW
```

## How The Integration Works

The upstream service acts as the upstream application that decides:

- what business event happened
- which notification should be sent
- who should receive it
- which channel or channels are needed

It then submits a canonical notification request to the control plane.

The control plane then decides:

- which routing policy applies
- which binding set should be used
- which provider binding is active for that channel
- whether to retry, fail over, or dead-letter delivery

## Channel Mapping

### Email

- the upstream service creates a notification request with `channels: ["email"]`
- the worker resolves the email binding set
- the worker calls the email connector
- the email connector talks to the external email provider
- provider callbacks return through the callback gateway

### SMS

- the upstream service creates a notification request with `channels: ["sms"]`
- the worker resolves SMS bindings and provider order
- the worker can fail over between multiple SMS providers if the bindings are configured that way
- provider callbacks update request and attempt state

### WhatsApp

- the upstream service creates a notification request with `channels: ["whatsapp"]`
- the control plane routes it the same way as the other channels
- the worker sends it through a WhatsApp connector when that connector is wired into the deployment
- callback handling is the same durable loop as the other channels
- if the user replies to a WhatsApp message, the provider can send an inbound reply webhook back to the callback gateway
- the callback gateway normalizes that reply into a generic channel event and emits a webhook to subscribed services
  - the upstream service can subscribe to those inbound reply events if it wants to react to replies, hand off to support, or update a conversation record

### Push

- the upstream service creates a notification request with `channels: ["push"]`
- the worker resolves push bindings and the selected binding set
- the worker calls the push connector
- the push provider returns acceptance or failure, and later callbacks update durable state

### Webhook

- the upstream service can still emit notification-intent events that ultimately target webhook delivery
- the worker calls the first-party webhook connector
- the webhook connector posts the normalized payload to the customer-owned webhook URL
- callback normalization happens through the same control-plane path when the provider exposes delivery lifecycle events

## How Multiple Providers Fit In

If a single channel has multiple providers, the control plane does **not** need a separate API shape for each provider.

Instead:

1. the upstream service sends one canonical notification request.
2. The request chooses the channel.
3. The routing policy optionally chooses a binding set.
4. The worker loads all provider bindings for that channel and binding set.
5. The worker tries them in priority order and can fail over if one provider is unhealthy or fails transiently.

That means for SMS, for example, you can model:

- one binding for Gupshup
- one binding for Karix
- one binding for Twilio

Those bindings can all belong to the same channel, and they can share or split binding sets depending on how you want traffic to flow.

## Recommended Pattern For Upstream Services

The cleanest shape is:

- the upstream service decides the business event and recipient
- the control plane decides delivery mechanics
- channel-specific provider choice stays in provider bindings, not in the app

That keeps provider changes out of `upstream service`.

It also means you can:

- add a new SMS provider without changing product logic
- route some events to one provider pool and some to another
- keep retry, failover, and dead-letter behavior centralized

## Example Flow

1. the upstream service emits a notification intent.
2. It sends the request to the control plane API.
3. The API stores the request and queues the work.
4. The worker resolves the target channel and binding set.
5. The worker dispatches to the matching connector.
6. The connector talks to the external provider.
7. The provider callback arrives later and updates the stored delivery state.

## What You Should Configure

For each channel you want the upstream service to use, configure the provider account once during onboarding or deployment:

- a routing policy
- a binding set, if you want provider grouping
- provider accounts for the channel
- secret references stored securely in the control plane
- callback verification settings, if the provider needs them
- webhook subscriptions for lifecycle updates and inbound reply events, if client wants to consume them

For example:

- email can use one or more configured email provider accounts
- SMS can use one or more configured SMS provider accounts such as Twilio, Gupshup, or Karix
- WhatsApp can use a configured WhatsApp provider account
- push can use a configured FCM or APNs provider account
- webhook can use a managed connector that posts to customer-owned callback URLs

## Bottom Line

`upstream service` does not need to understand every provider.

It only needs to speak the control-plane request model.
NotifyHub then handles channel routing, provider onboarding, provider selection, retries, failover, callbacks, and observability.
