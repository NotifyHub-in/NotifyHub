# Load Testing

This load test exercises NotifyHub with a mix of:

- happy-path email notifications
- happy-path SMS notifications
- happy-path webhook routing through stored policy
- suppressed notifications through preference policy
- scheduled retry, dead-letter, and replay flow through a temporary failing binding
- render failures caused by missing template variables
- unsupported channel requests
- idempotent exact replays
- idempotency conflicts
- validation failures
- malformed JSON payloads

It also posts provider callbacks for a configurable fraction of accepted requests so the callback gateway and delivered-state metrics move during the run.

## Run

```bash
go run ./tests/load/cmd/loadgen
```

Useful flags:

```bash
go run ./tests/load/cmd/loadgen \
  -requests 300 \
  -concurrency 30 \
  -callback-fraction 0.8
```

## Watch

- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`

Recommended dashboards:

- `NotifyHub Overview`
- `NotifyHub Load Test`
