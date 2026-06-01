# Channel Setup Checklist

This guide explains what you need to do for each supported channel after the control plane is deployed to Kubernetes.

It focuses on the managed-provider model:
- create a provider account once
- store credentials as secret references
- create a binding for the channel
- create routing and templates
- send notifications through the canonical API

## Shared Concepts For Every Channel

Before enabling any channel, make sure you have:

1. a client API key for the upstream service
2. a provider account for the channel
3. a provider binding pointing at that provider account
4. a routing policy for the event name
5. a template for every language you plan to support
6. a secret mount or external secret store for provider credentials
7. callback configuration if the provider supports delivery receipts

## Secret Rules

Use placeholders in docs and configuration examples.

Examples:
- `<provider-username>`
- `<provider-password>`
- `<smtp-host>`
- `<service-account-json>`
- `<callback-secret>`
- `<device-token>`

Do not commit real provider secrets to Git or paste them into docs.

## Email

### What You Need

- SMTP credentials or an email API provider account
- a `from` address you control
- one template per language if you send localized mail

### Setup Steps

1. Create a provider account.
2. Store the SMTP password or email API credential as a secret reference.
3. Create a provider binding for the email channel.
4. Create one or more templates.
5. Create a routing policy for the event name.
6. Send a test email to a controlled inbox.

### Example Provider Account Shape

```json
{
  "tenant_id": "tenant-a",
  "provider_key": "smtp-email",
  "display_name": "Tenant A SMTP",
  "channel": "email",
  "enabled": true,
  "config": {
    "host": "smtp.example.com",
    "port": "587",
    "from_email": "noreply@example.com"
  },
  "secret_refs": {
    "user": {
      "ref": "file:///run/notification-secrets/smtp_user.txt",
      "material_type": "secret_string",
      "source": "file"
    },
    "password": {
      "ref": "file:///run/notification-secrets/smtp_password.txt",
      "material_type": "secret_string",
      "source": "file"
    }
  }
}
```

### Notes

- Email delivery is usually acceptance-only unless your provider supports event webhooks.
- If you use SendGrid, configure its event webhook separately.
- If you add SendGrid callbacks, follow [Callbacks And Delivery Tracking](/Users/Shaik/notifications/notification-control-plane/docs/guides/callbacks-and-delivery-tracking.md) and create a matching callback route.
- If the provider verifies callbacks, store the verification secret as a file-backed secret reference and mount it in the callback gateway.

## SMS

### What You Need

- an SMS provider account such as Gupshup or Twilio
- sender ID or originating phone number
- templates for each language you need

### Setup Steps

1. Create the SMS provider account.
2. Store username/password or API token as secret references.
3. Create a provider binding for SMS.
4. Create routing policies for OTP, alerts, or transactional SMS.
5. Create template variants for each language.
6. For non-English messages, ensure Unicode handling is enabled by the provider adapter.
7. Send a smoke SMS to a test number.

### Notes

- For Gupshup SMS, the control plane should send Unicode flags for non-English content.
- Use `language_code` to choose the template variant.
- Keep the template body and provider template name aligned.
- If the provider supports SMS delivery receipts, configure the callback URL and secret using [Callbacks And Delivery Tracking](/Users/Shaik/notifications/notification-control-plane/docs/guides/callbacks-and-delivery-tracking.md).
- Store the callback verification secret as a mounted file secret rather than in the template or request payload.

## WhatsApp

### What You Need

- a WhatsApp provider account such as Gupshup
- approved WhatsApp templates in the provider dashboard
- image/video/document metadata if the template is media-based
- callback configuration for delivery receipts if the provider supports them

### Setup Steps

1. Create the WhatsApp provider account.
2. Store provider credentials as file-backed secret references.
3. Create a provider binding for WhatsApp.
4. Create or import approved WhatsApp templates.
5. Register one template row per language.
6. For media templates, include media metadata such as `media_type`, `media_url`, and approved provider template names.
7. Configure the provider callback URL if delivery receipts are available.
8. Send a test WhatsApp message to a controlled phone number.

### Media Template Notes

- Treat image/video/document templates as provider-approved media templates.
- Use the exact provider template name from the source system.
- Keep header, footer, and body placeholders aligned with the provider-approved template.
- Use `language_code` to select the correct translation.
- If the provider returns delivery receipts for WhatsApp, configure the callback route in the control plane and in the provider dashboard.
- If WhatsApp callback verification is enabled, keep the verification secret in the mounted secret store and reference it from the callback route.

### Example Template Metadata

```json
{
  "media_type": "image",
  "gupshup_template_name": "example_whatsapp_image_template",
  "interactive_attributes": "{\"footer\":\"Sample footer\"}"
}
```

## Push

### What You Need

- an FCM service account JSON
- the Firebase project ID
- a recipient device token or topic

### Setup Steps

1. Create the FCM provider account.
2. Store the service-account JSON as a file-backed secret reference.
3. Mount the secret into `connector-push` in the cluster.
4. Create a provider binding for push.
5. Create routing policies for push events.
6. Create templates for push title/body content.
7. Send a push to a controlled device token or topic.

### Notes

- Push uses `recipient.push_token` for device delivery.
- If you use topics, populate `recipient.topic` instead.
- Push is generally acceptance-based; delivery receipts depend on the provider and client platform.
- Push usually does not use the same callback flow as SMS or WhatsApp; treat provider acceptance as the main signal unless your FCM integration adds a separate status reconciliation path.

## Webhook

### What You Need

- the destination URL owned by the downstream system
- optional signing secret if you want signed payloads
- a template or payload shape that matches the consumer contract

### Setup Steps

1. Create a webhook provider binding.
2. Create a routing policy for the event.
3. Configure the downstream endpoint URL.
4. Add a signing secret if you need request verification.
5. Send a test webhook event.

### Notes

- Webhook is outbound delivery to a customer-owned endpoint.
- There is no third-party provider dashboard for webhook callbacks.
- Webhook delivery is tracked as an outbound attempt, not as a provider callback route.

## Recommended First Production Matrix

If you are launching carefully, start with:

- one email path
- Gupshup SMS
- Gupshup WhatsApp
- FCM push
- webhook delivery to customer endpoints

Keep any additional provider variants disabled until they are verified end to end.

## Smoke Test Order

A practical order for testing after setup is:

1. email
2. SMS
3. WhatsApp text
4. WhatsApp media
5. push
6. webhook

That order makes it easier to isolate provider-specific issues.

## Troubleshooting Checklist

If a channel does not work:

1. confirm the provider account is enabled
2. confirm the provider binding points to the right provider account
3. confirm the routing policy points to the right binding set
4. confirm the template exists for the requested language
5. confirm the required secret is mounted in the connector pod
6. confirm the provider callback route is configured if receipts are expected
7. inspect request status and delivery attempts

## Next Step

After the channel checklist is complete, use [Send Notifications](/Users/Shaik/notifications/notification-control-plane/docs/guides/send-notifications.md) to exercise the canonical API flow.
