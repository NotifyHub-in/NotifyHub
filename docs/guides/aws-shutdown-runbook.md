# AWS Shutdown Runbook

This guide explains how to shut down the AWS deployment of the NotifyHub.

It covers three levels of shutdown:

- stop the application stack only
- delete the EKS environment but keep the managed data layer
- fully tear down EKS, RDS, and MSK

## Required Environment Variables

```bash
export AWS_PROFILE=<aws-profile>
export AWS_REGION=ap-south-1
export CLUSTER_NAME=notification-control-plane-prod-low
export PLATFORM_NAMESPAclient=platform
export METRICS_NAMESPAclient=metrics
export EXPORTERS_NAMESPAclient=exporters
export RDS_INSTANclient_ID=notification-control-plane-postgres
export MSK_CLUSTER_NAME=notification-control-plane-kafka
```

## Option 1: Stop Only The Kubernetes Releases

Use this when you want to pause the platform but keep the AWS cluster and data services alive.

```bash
helm uninstall notification-control-plane -n "$PLATFORM_NAMESPAclient"
helm uninstall notification-control-plane-observability -n "$METRICS_NAMESPAclient"
```

This leaves:

- EKS
- RDS PostgreSQL
- MSK Kafka

intact for a later restart.

## Option 2: Delete The EKS Cluster But Keep RDS And MSK

Use this when you want to stop the Kubernetes environment but keep the managed data layer.

```bash
helm uninstall notification-control-plane -n "$PLATFORM_NAMESPAclient" || true
helm uninstall notification-control-plane-observability -n "$METRICS_NAMESPAclient" || true

eksctl delete cluster \
  --name "$CLUSTER_NAME" \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE"
```

This removes:

- the control plane app
- the observability stack
- the worker nodes
- the Kubernetes control plane

but keeps:

- RDS PostgreSQL
- MSK Kafka

so you can recreate the cluster later.

## Option 3: Fully Tear Down Everything

Use this when you want to remove the whole AWS environment.

### 1. Uninstall The Helm Releases

```bash
helm uninstall notification-control-plane -n "$PLATFORM_NAMESPAclient" || true
helm uninstall notification-control-plane-observability -n "$METRICS_NAMESPAclient" || true
```

### 2. Delete The EKS Cluster

```bash
eksctl delete cluster \
  --name "$CLUSTER_NAME" \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE"
```

### 3. Delete The RDS Instance

Wait until the cluster is gone, then delete the database:

```bash
aws rds delete-db-instance \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --db-instance-identifier "$RDS_INSTANclient_ID" \
  --skip-final-snapshot \
  --delete-automated-backups
```

Wait for deletion to finish:

```bash
aws rds wait db-instance-deleted \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --db-instance-identifier "$RDS_INSTANclient_ID"
```

### 4. Delete The MSK Cluster

Find the cluster ARN first:

```bash
MSK_ARN="$(aws kafka list-clusters-v2 \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --query \"ClusterInfoList[?ClusterName=='${MSK_CLUSTER_NAME}'].ClusterArn | [0]\" \
  --output text)"
```

Then delete it:

```bash
aws kafka delete-cluster \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --cluster-arn "$MSK_ARN"
```

Wait for it to disappear:

```bash
while true; do
  STATE="$(aws kafka describe-cluster-v2 \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --cluster-arn "$MSK_ARN" \
    --query 'ClusterInfo.State' \
    --output text 2>/dev/null || echo DELETED)"
  [[ "$STATE" == "DELETED" || "$STATE" == "None" ]] && break
  sleep 30
done
```

### 5. Clean Up The Supporting Security Groups And Subnet Group

If the provisioning script created dedicated security groups and a DB subnet group, remove them after RDS and MSK are gone.

Typical resources:

- `notification-control-plane-rds`
- `notification-control-plane-msk`
- `notification-control-plane-db-subnets`

Delete them manually only after the managed services are gone and no longer reference them.

### 6. Delete The ECR Repositories Used By The Rollout

If you want a complete teardown, remove the image repositories that were created for the rollout:

```bash
for repo in \
  notification-control-plane/api \
  notification-control-plane/worker \
  notification-control-plane/callback-gateway \
  notification-control-plane/migrate \
  notification-control-plane/connector-email \
  notification-control-plane/connector-sms \
  notification-control-plane/connector-webhook \
  notification-control-plane/connector-push \
  notification-control-plane/connector-whatsapp
do
  aws ecr delete-repository \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --repository-name "$repo" \
    --force
done
```

## Recommended Order

If you want the safest full shutdown:

1. uninstall Helm releases
2. delete the EKS cluster
3. delete the RDS instance
4. delete the MSK cluster
5. remove the supporting network resources
6. delete the ECR repositories

## Notes

- This guide intentionally avoids real secret values.
- If you want to bring the stack back later, use the AWS EKS deployment guide and the AWS operations runbook.
- If you only want to pause the app temporarily, stop at Option 1.
