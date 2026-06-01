# Notification Control Plane Observability Helm Chart

This chart deploys the basic observability stack for the Notification Control Plane:

- Prometheus
- Grafana
- Grafana dashboard provisioning
- cAdvisor
- Postgres exporter
- Kafka exporter
- Prometheus alert rules
- a Grafana alerting dashboard

It is intentionally separate from the application chart so operators can install or upgrade monitoring independently.

## Namespace Layout

This chart is designed for a three-namespace split:

- `platform` for the notification control plane app
- `metrics` for Prometheus, Grafana, and cAdvisor
- `exporters` for Postgres and Kafka exporters

Set:

- `appNamespace: platform`
- `exportersNamespace: exporters`

## Install

```bash
helm upgrade --install notification-control-plane-observability deployments/helm/notification-control-plane-observability \
  --namespace metrics \
  -f values.observability.yaml
```

For the production-shaped local layout, deploy:

- the app chart into `platform`
- this observability chart into `metrics`
- the Postgres/Kafka exporter secrets into `exporters`

## What It Scrapes

By default, Prometheus is configured to scrape:

- the app release services, using `appReleaseName` and the standard service names
- the observability release exporters
- any extra targets you provide in values

The app services are expected to be named like:

- `<appReleaseName>-api.<appNamespace>.svc.cluster.local:8080`
- `<appReleaseName>-worker.<appNamespace>.svc.cluster.local:8081`
- `<appReleaseName>-callback-gateway.<appNamespace>.svc.cluster.local:8082`
- `<appReleaseName>-connector-email.<appNamespace>.svc.cluster.local:8091`
- `<appReleaseName>-connector-sms.<appNamespace>.svc.cluster.local:8092`
- `<appReleaseName>-connector-webhook.<appNamespace>.svc.cluster.local:8093`
- `<appReleaseName>-connector-push.<appNamespace>.svc.cluster.local:8094`
- `<appReleaseName>-connector-whatsapp.<appNamespace>.svc.cluster.local:8095`

The observability release exports in the `exporters` namespace:

- `<observabilityRelease>-postgres-exporter.exporters.svc.cluster.local:9187`
- `<observabilityRelease>-kafka-exporter.exporters.svc.cluster.local:9308`

You must provide either:

- `postgresExporter.existingSecret` or `postgresExporter.dataSourceName`
- `kafkaExporter.brokersCSV`

If the app release uses secret-backed environment variables for the email connector, reference them in the app chart with `connectors.definitions[].envFromSecret` and create the matching Kubernetes Secret in `platform`.

## Dashboards

Grafana is provisioned with the existing Notification Control Plane dashboards:

- overview
- load test

The dashboards are loaded from ConfigMaps at install time, including the dedicated alerts dashboard.

## Example Values

```yaml
appReleaseName: notification-control-plane
appNamespace: platform
exportersNamespace: exporters

postgresExporter:
  existingSecret: notification-control-plane-observability-postgres-dsn
  existingSecretKey: data-source-name

kafkaExporter:
  brokersCSV: kafka:9092
```
