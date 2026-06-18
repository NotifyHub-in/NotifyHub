# Channel Examples

This guide collects concrete examples for the standard channels supported by the control plane.

## Email

### Typical provider account

- provider key: `smtp-email` or `sendgrid-email`
- destination field: `recipient.email`

### Example request

```json
{
  "idempotency_key": "welcome-email-001",
  "event_name": "user.welcome",
  "template_key": "welcome-email-v1",
  "language_code": "en",
  "channels": ["email"],
  "recipient": {
    "user_id": "user-42",
    "email": "user@example.com"
  },
  "variables": {
    "name": "Priya"
  }
}
```

## SMS

### Typical providers

- `gupshup-sms`
- `karix-sms`
- `twilio-sms`

### Example request

```json
{
  "idempotency_key": "otp-sms-001",
  "event_name": "auth.login.otp",
  "template_key": "login-otp-sms-v1",
  "language_code": "hi-in",
  "channels": ["sms"],
  "recipient": {
    "user_id": "user-42",
    "phone": "+919999999999"
  },
  "variables": {
    "otp": "123456"
  }
}
```

## WhatsApp

### Typical providers

- `gupshup-whatsapp`
- `karix-whatsapp`

### Example request

```json
{
  "idempotency_key": "otp-whatsapp-001",
  "event_name": "auth.login.otp",
  "template_key": "login-otp-whatsapp-v1",
  "language_code": "hi-in",
  "channels": ["whatsapp"],
  "recipient": {
    "user_id": "user-42",
    "phone": "+919999999999"
  },
  "variables": {
    "otp": "123456"
  }
}
```

### Template metadata note

WhatsApp templates often need provider-specific metadata, for example:

- `gupshup_template_name`
- `karix_template_name`
- `media_type`
- `interactive_attributes`

That metadata belongs on the control-plane template resource, not in every request.

## Push

### Typical provider

- `fcm-push`

### Example request

```json
{
  "idempotency_key": "ops-push-001",
  "event_name": "ops.alert",
  "template_key": "ops-alert-push-v1",
  "language_code": "en",
  "channels": ["push"],
  "recipient": {
    "user_id": "ops-user-1",
    "push_token": "<fcm_device_token>"
  },
  "variables": {
    "title": "Ops alert",
    "body": "Disk pressure high"
  }
}
```

### Farm app example

- provider key: `fcm-push`
- project ID: `nurture-farm`
- service account file mounted into `connector-push`

### Generic Push Example

- provider key: `fcm-push`
- project ID: `example-service`
- separate service account file mounted into `connector-push`

## Webhook

### Typical destination

- customer-owned HTTPS endpoint

### Example request

```json
{
  "idempotency_key": "partner-webhook-001",
  "event_name": "partner.event.ready",
  "template_key": "partner-webhook-v1",
  "language_code": "en",
  "channels": ["webhook"],
  "recipient": {
    "user_id": "partner-1",
    "webhook": "https://partner.example.com/events"
  },
  "variables": {
    "event_id": "evt-2001"
  }
}
```

## Upstream Service Example

For an upstream service, the clean mapping is:

- client decides the event and recipient
- client chooses the template key
- client sends one canonical control-plane request
- control plane resolves provider bindings and delivery

See [Upstream Service Multi-Channel Integration](/docs/integrations/upstream-service-multi-channel.md).

## Default Language Rule

If a request does not specify `language_code`, the control plane treats it as `en`.

For multilingual templates, create one template row per language code and keep the `template_key` stable across all languages.
