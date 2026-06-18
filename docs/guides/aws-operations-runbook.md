# AWS Operations Runbook

This guide captures the AWS deployment we used for the NotifyHub and the exact day-2 commands for starting it again, stopping it, and tearing it down.

It assumes the production-shaped three-namespace layout:

- `platform` for the control plane app
- `metrics` for Prometheus, Grafana, and cAdvisor
- `exporters` for Postgres and Kafka exporters

It also assumes:

- a fresh EKS cluster in `ap-south-1`
- managed Postgres and Kafka already provisioned
- the Helm charts in this repository

## What Is Deployed

- Application chart in `platform`
  - `api`
  - `worker`
  - `callback-gateway`
  - `connector-email`
  - `connector-sms`
  - `connector-webhook`
  - `connector-push`
  - `connector-whatsapp`
- Observability chart in `metrics`
  - Prometheus
  - Grafana
  - cAdvisor
- Exporters in `exporters`
  - Postgres exporter
  - Kafka exporter

## Required Environment Variables

Set these once in your shell before running the commands below:

```bash
export AWS_PROFILE=<aws-profile>
export AWS_REGION=ap-south-1
export CLUSTER_NAME=notification-control-plane-prod-low
export PLATFORM_NAMESPAclient=platform
export METRICS_NAMESPAclient=metrics
export EXPORTERS_NAMESPAclient=exporters
```

If your cluster name differs, update `CLUSTER_NAME` accordingly.

## One-Time Cluster Access

Update your kubeconfig so `kubectl` and `helm` talk to the right cluster:

```bash
aws eks update-kubeconfig \
  --name "$CLUSTER_NAME" \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE"
```

## Start Or Resume The Stack

Use this when the cluster is already created and you want the platform running again.

### 1. Make sure the node group is running

If you previously scaled the nodes down to zero, scale them back up:

```bash
eksctl scale nodegroup \
  --cluster "$CLUSTER_NAME" \
  --name platform-nodes \
  --region "$AWS_REGION" \
  --nodes 2
```

You can use a different node count if you want a smaller or larger footprint.

### 2. Install Or Upgrade The App Release

```bash
helm upgrade --install notification-control-plane deployments/helm/notification-control-plane \
  --namespace "$PLATFORM_NAMESPAclient" \
  --create-namespace \
  -f deployments/helm/notification-control-plane/values.aws.yaml
```

### 3. Install Or Upgrade The Observability Release

```bash
helm upgrade --install notification-control-plane-observability deployments/helm/notification-control-plane-observability \
  --namespace "$METRICS_NAMESPAclient" \
  --create-namespace \
  -f deployments/helm/notification-control-plane-observability/values.aws.yaml
```

### 4. Verify The Stack

```bash
kubectl -n "$PLATFORM_NAMESPAclient" get pods
kubectl -n "$METRICS_NAMESPAclient" get pods
kubectl -n "$EXPORTERS_NAMESPAclient" get pods
```

Then check:

- API health
- worker readiness
- callback gateway readiness
- connector readiness
- Prometheus targets
- Grafana dashboards

## Stop The Stack

Use this when you want to pause the application without deleting the cluster.

### Stop Only The Application And Observability Pods

```bash
helm uninstall notification-control-plane -n "$PLATFORM_NAMESPAclient"
helm uninstall notification-control-plane-observability -n "$METRICS_NAMESPAclient"
```

This leaves the EKS cluster, RDS, and MSK intact, so you can start the stack again later.

### Optional: Scale The Node Group To Zero

If you want to reduce compute cost while keeping the cluster object around, scale the node group down:

```bash
eksctl scale nodegroup \
  --cluster "$CLUSTER_NAME" \
  --name platform-nodes \
  --region "$AWS_REGION" \
  --nodes 0
```

This stops the worker nodes, but it does not delete managed AWS services like RDS or MSK.

## Full Teardown

Use this only when you want to remove the AWS environment entirely.

### 1. Remove The Helm Releases

```bash
helm uninstall notification-control-plane -n "$PLATFORM_NAMESPAclient" || true
helm uninstall notification-control-plane-observability -n "$METRICS_NAMESPAclient" || true
```

### 2. Delete The Namespaces

```bash
kubectl delete namespace "$PLATFORM_NAMESPAclient" || true
kubectl delete namespace "$METRICS_NAMESPAclient" || true
kubectl delete namespace "$EXPORTERS_NAMESPAclient" || true
```

### 3. Delete The Node Group Or Cluster

If you only want to remove the worker nodes:

```bash
eksctl delete nodegroup \
  --cluster "$CLUSTER_NAME" \
  --name platform-nodes \
  --region "$AWS_REGION"
```

If you want to remove the whole EKS cluster:

```bash
eksctl delete cluster \
  --name "$CLUSTER_NAME" \
  --region "$AWS_REGION"
```

### 4. Decide What To Do With RDS And MSK

This guide does not delete RDS or MSK automatically.

If you created dedicated AWS-managed Postgres and Kafka for this stack, clean them up separately after you are sure you no longer need the data.

## Quick Smoke Test Commands

After a fresh start, the fastest way to validate the stack is:

```bash
kubectl -n "$PLATFORM_NAMESPAclient" get pods
kubectl -n "$METRICS_NAMESPAclient" get pods
kubectl -n "$EXPORTERS_NAMESPAclient" get pods
```

Then open Grafana and Prometheus through your port-forwards or ingress and confirm:

- request counters move
- connector latency panels move
- exporter health is green
- Kafka lag is visible

## Notes

- This runbook intentionally avoids real secret values.
- If you update the AWS values files, rerun the `helm upgrade --install` commands above.
- If you only need to restart pods after a config change, `helm upgrade --install` is usually enough.
