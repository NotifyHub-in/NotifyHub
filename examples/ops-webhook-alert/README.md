# Operations Webhook Alert Example

This example shows an operational or partner-facing webhook alert for a failed payment event.

## When To Use It

Use this pattern when:

- another system wants to receive a machine-readable callback
- you want routing and delivery policy to be centralized
- you want request state and callback normalization tracked in one place

## Request Payload

See [request.json](/examples/ops-webhook-alert/request.json).

## Send It

```bash
curl -s -X POST http://localhost:8080/v1/notification-requests \
  -H 'Content-Type: application/json' \
  -d @examples/ops-webhook-alert/request.json
```

## Expected Platform Behavior

- API accepts the request
- worker dispatches through the `webhook` connector
- delivery attempt is stored with a provider message id
- request becomes `dispatched`
- if the downstream provider later calls back through `callback-gateway`, the control plane normalizes that update and can move the request to `delivered` or `failed`
- lifecycle webhooks can notify other subscribers about the request state change

## Good Companion Config

This example assumes:

- template key `payment-failed-v1` exists for `webhook`
- a `webhook` provider binding is active
- callback-gateway is reachable if provider delivery callbacks are expected

## Useful Follow-Up Calls

```bash
curl -s http://localhost:8080/v1/notification-requests/{request_id}
curl -s -X POST http://localhost:8082/v1/providers/webhook/webhook-account-1/callbacks \
  -H 'Content-Type: application/json' \
  -d '{"provider_message_id":"...","status":"delivered"}'
```
