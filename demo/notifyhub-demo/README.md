# NotifyHub Demo Studio

This folder contains a local-only presentation UI for the NotifyHub control plane.

It is intentionally separate from the public docs site and is not meant to be published to GitHub Pages.

## What It Does

- sends live email, SMS, WhatsApp, and push requests through the control plane
- shows request status, delivery attempts, and webhook receipts
- captures lifecycle callbacks in a local inbox
- refreshes live state automatically every few seconds
- listens to a live server-sent event stream for callback-driven updates

## How To Run

Start the control plane stack first, then run:

```bash
go run ./demo/notifyhub-demo
```

Then open:

```text
http://localhost:8787
```

## Local Defaults

The demo talks to the local control plane at `http://localhost:8080` and seeds demo bindings, templates, routing policies, and a webhook inbox.

The page also proxies reads through `/api` so the browser can stay on the same origin.

The demo server also creates or reuses a local client identity and uses that client API key when it sends notification requests, so the send flow behaves like a real upstream service.

That local client state is cached under `/tmp/notifyhub-demo-client.json` by default and is not meant to be committed.

## Useful Endpoints

- `GET /demo/bootstrap` for demo config
- `GET /demo/webhooks` for the local callback inbox
- `POST /hooks/notification-events` for the webhook receiver that stores inbound callbacks into the inbox

When the control plane is running in Docker, point webhook subscriptions at `http://host.docker.internal:8788/hooks/notification-events` so the container can reach the host demo server.
- `POST /demo/seed` to re-seed the demo resources

## Notes

- The demo is designed for local presentation only.
- It reuses the real control-plane APIs and does not mock the send lifecycle.
- If a callback target is not reachable, the UI will still show the delivery attempt failures and webhook inbox state.
