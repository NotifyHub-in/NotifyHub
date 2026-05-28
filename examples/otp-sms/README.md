# OTP SMS Example

This example shows a high-priority OTP flow where an auth or login service needs to send a one-time password over SMS.

## When To Use It

Use this pattern when:

- the product needs a fast transactional SMS
- retry behavior matters
- you want SMS provider failover handled outside your auth service

## Request Payload

See [request.json](/Users/Shaik/notifications/notification-control-plane/examples/otp-sms/request.json).

## Send It

```bash
curl -s -X POST http://localhost:8080/v1/notification-requests \
  -H 'Content-Type: application/json' \
  -d @examples/otp-sms/request.json
```

## Expected Platform Behavior

- API accepts the request
- worker renders `otp-requested-v1`
- SMS connector is selected from configured bindings
- request becomes `dispatched` after provider acceptance
- if the provider is temporarily unavailable, retry scheduling and DLQ logic are applied automatically
- if callbacks are configured, later provider updates can move the request toward terminal delivery visibility

## Good Companion Config

This example assumes:

- template key `otp-requested-v1` exists for `sms`
- an active `sms` provider binding exists
- `sms` delivery policy has a retry/backoff configuration suitable for OTPs

## Useful Follow-Up Calls

```bash
curl -s http://localhost:8080/v1/notification-requests/{request_id}
curl -s http://localhost:8080/v1/delivery-policies/sms
```

