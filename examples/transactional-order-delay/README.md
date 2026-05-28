    # Transactional Order Delay Example

This example shows a transactional product flow where an order service wants to notify a user that an order is delayed.

## When To Use It

Use this pattern when:

- your business system already knows the recipient
- you want an email immediately
- you want the control plane to handle retry, failover, audit trail, and delivery lifecycle

## Request Payload

See [request.json](/Users/Shaik/notifications/notification-control-plane/examples/transactional-order-delay/request.json).

## Send It

```bash
curl -s -X POST http://localhost:8080/v1/notification-requests \
  -H 'Content-Type: application/json' \
  -d @examples/transactional-order-delay/request.json
```

## Expected Platform Behavior

- API accepts the request and returns a `request_id`
- worker renders `order-delayed-v1`
- worker resolves channel `email`
- worker prefers binding set `transactional-email`
- provider binding is selected and dispatched
- delivery attempt is stored in Postgres
- request state becomes `dispatched`
- if a provider callback later reports success, request state becomes `delivered`

## Good Companion Config

This example assumes:

- template key `order-delayed-v1` exists for `email`
- binding set `transactional-email` is configured
- delivery policy for `email` is enabled

## Useful Follow-Up Calls

```bash
curl -s http://localhost:8080/v1/notification-requests/{request_id}
curl -s http://localhost:8080/v1/provider-bindings/email?binding_set=transactional-email
```

