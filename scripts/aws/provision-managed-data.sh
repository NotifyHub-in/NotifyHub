#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-ap-south-1}"
AWS_PROFILE="${AWS_PROFILE:-}"
if [[ -z "${AWS_PROFILE}" ]]; then
  echo "AWS_PROFILE is required." >&2
  exit 1
fi

CLUSTER_NAME="${CLUSTER_NAME:-notification-control-plane-prod-low}"
RDS_INSTANCE_ID="${RDS_INSTANCE_ID:-notification-control-plane-postgres}"
MSK_CLUSTER_NAME="${MSK_CLUSTER_NAME:-notification-control-plane-kafka}"
DB_USERNAME="${DB_USERNAME:-notification}"
DB_NAME="${DB_NAME:-notification_control_plane}"
DB_INSTANCE_CLASS="${DB_INSTANCE_CLASS:-db.t3.micro}"
MSK_INSTANCE_TYPE="${MSK_INSTANCE_TYPE:-kafka.m5.large}"
MSK_BROKER_COUNT="${MSK_BROKER_COUNT:-2}"
MSK_KAFKA_VERSION="${MSK_KAFKA_VERSION:-3.7.x}"
WORKDIR="${WORKDIR:-/tmp/notification-control-plane-aws}"
mkdir -p "$WORKDIR"

STACK_NAME="eksctl-${CLUSTER_NAME}-cluster"
VPC_ID="$(aws cloudformation describe-stack-resources --profile "$AWS_PROFILE" --region "$AWS_REGION" --stack-name "$STACK_NAME" --query 'StackResources[?LogicalResourceId==`VPC`].PhysicalResourceId' --output text)"
NODE_SG="$(aws cloudformation describe-stack-resources --profile "$AWS_PROFILE" --region "$AWS_REGION" --stack-name "$STACK_NAME" --query 'StackResources[?LogicalResourceId==`ClusterSharedNodeSecurityGroup`].PhysicalResourceId' --output text)"
PRIVATE_SUBNETS=($(aws cloudformation describe-stack-resources --profile "$AWS_PROFILE" --region "$AWS_REGION" --stack-name "$STACK_NAME" --query 'StackResources[?starts_with(LogicalResourceId, `SubnetPrivate`)].PhysicalResourceId' --output text))

if [[ "${#PRIVATE_SUBNETS[@]}" -lt 2 ]]; then
  echo "Expected at least two private subnets from the EKS stack." >&2
  exit 1
fi

RDS_PASSWORD_FILE="$WORKDIR/rds-postgres-password.txt"
if [[ ! -f "$RDS_PASSWORD_FILE" ]]; then
  openssl rand -base64 24 > "$RDS_PASSWORD_FILE"
fi
RDS_PASSWORD="$(cat "$RDS_PASSWORD_FILE")"

ensure_sg() {
  local name="$1"
  local description="$2"
  local port="$3"
  local source_sg="$4"
  local sg_id
  sg_id="$(aws ec2 describe-security-groups --profile "$AWS_PROFILE" --region "$AWS_REGION" --filters Name=vpc-id,Values="$VPC_ID" Name=group-name,Values="$name" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
  if [[ -z "$sg_id" || "$sg_id" == "None" ]]; then
    sg_id="$(aws ec2 create-security-group --profile "$AWS_PROFILE" --region "$AWS_REGION" --vpc-id "$VPC_ID" --group-name "$name" --description "$description" --query GroupId --output text)"
  fi
  aws ec2 authorize-security-group-ingress --profile "$AWS_PROFILE" --region "$AWS_REGION" --group-id "$sg_id" --protocol tcp --port "$port" --source-group "$source_sg" >/dev/null 2>&1 || true
  printf '%s' "$sg_id"
}

RDS_SG_ID="$(ensure_sg notifyhub-rds "NotifyHub RDS access" 5432 "$NODE_SG")"
MSK_SG_ID="$(ensure_sg notifyhub-msk "NotifyHub MSK access" 9092 "$NODE_SG")"

RDS_SUBNET_GROUP="notification-control-plane-db-subnets"
if ! aws rds describe-db-subnet-groups --profile "$AWS_PROFILE" --region "$AWS_REGION" --db-subnet-group-name "$RDS_SUBNET_GROUP" >/dev/null 2>&1; then
  aws rds create-db-subnet-group \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --db-subnet-group-name "$RDS_SUBNET_GROUP" \
    --db-subnet-group-description "NotifyHub private DB subnets" \
    --subnet-ids "${PRIVATE_SUBNETS[@]}" >/dev/null
fi

if ! aws rds describe-db-instances --profile "$AWS_PROFILE" --region "$AWS_REGION" --db-instance-identifier "$RDS_INSTANCE_ID" >/dev/null 2>&1; then
  aws rds create-db-instance \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --db-instance-identifier "$RDS_INSTANCE_ID" \
    --db-instance-class "$DB_INSTANCE_CLASS" \
    --engine postgres \
    --allocated-storage 20 \
    --storage-type gp3 \
    --master-username "$DB_USERNAME" \
    --master-user-password "$RDS_PASSWORD" \
    --db-name "$DB_NAME" \
    --vpc-security-group-ids "$RDS_SG_ID" \
    --db-subnet-group-name "$RDS_SUBNET_GROUP" \
    --no-publicly-accessible \
    --backup-retention-period 7 \
    --storage-encrypted >/dev/null
fi

MSK_EXISTS="$(aws kafka list-clusters-v2 --profile "$AWS_PROFILE" --region "$AWS_REGION" --query "ClusterInfoList[?ClusterName=='${MSK_CLUSTER_NAME}'].ClusterArn | [0]" --output text)"
if [[ -z "$MSK_EXISTS" || "$MSK_EXISTS" == "None" ]]; then
  cat > "$WORKDIR/msk-cluster.json" <<EOF
{
  "ClusterName": "${MSK_CLUSTER_NAME}",
  "Provisioned": {
    "BrokerNodeGroupInfo": {
      "ClientSubnets": [
        "${PRIVATE_SUBNETS[0]}",
        "${PRIVATE_SUBNETS[1]}"
      ],
      "InstanceType": "${MSK_INSTANCE_TYPE}",
      "SecurityGroups": ["${MSK_SG_ID}"]
    },
    "ClientAuthentication": {
      "Unauthenticated": {
        "Enabled": true
      }
    },
    "EncryptionInfo": {
      "EncryptionInTransit": {
        "ClientBroker": "TLS_PLAINTEXT",
        "InCluster": true
      }
    },
    "KafkaVersion": "${MSK_KAFKA_VERSION}",
    "NumberOfBrokerNodes": ${MSK_BROKER_COUNT},
    "EnhancedMonitoring": "DEFAULT"
  }
}
EOF
  aws kafka create-cluster-v2 --profile "$AWS_PROFILE" --region "$AWS_REGION" --cli-input-json "file://${WORKDIR}/msk-cluster.json" >/dev/null
fi

echo "Waiting for RDS instance ${RDS_INSTANCE_ID}..."
aws rds wait db-instance-available --profile "$AWS_PROFILE" --region "$AWS_REGION" --db-instance-identifier "$RDS_INSTANCE_ID"

echo "Waiting for MSK cluster ${MSK_CLUSTER_NAME}..."
MSK_ARN="$(aws kafka list-clusters-v2 --profile "$AWS_PROFILE" --region "$AWS_REGION" --query "ClusterInfoList[?ClusterName=='${MSK_CLUSTER_NAME}'].ClusterArn | [0]" --output text)"
while true; do
  STATE="$(aws kafka describe-cluster-v2 --profile "$AWS_PROFILE" --region "$AWS_REGION" --cluster-arn "$MSK_ARN" --query 'ClusterInfo.State' --output text)"
  if [[ "$STATE" == "ACTIVE" ]]; then
    break
  fi
  sleep 30
done

DB_ENDPOINT="$(aws rds describe-db-instances --profile "$AWS_PROFILE" --region "$AWS_REGION" --db-instance-identifier "$RDS_INSTANCE_ID" --query 'DBInstances[0].Endpoint.Address' --output text)"
KAFKA_BROKERS="$(aws kafka get-bootstrap-brokers --profile "$AWS_PROFILE" --region "$AWS_REGION" --cluster-arn "$MSK_ARN" --query BootstrapBrokerString --output text)"

cat <<EOF
RDS_SG_ID=$RDS_SG_ID
MSK_SG_ID=$MSK_SG_ID
RDS_PASSWORD_FILE=$RDS_PASSWORD_FILE
DATABASE_URL=postgresql://${DB_USERNAME}:<redacted>@${DB_ENDPOINT}:5432/${DB_NAME}?sslmode=require
KAFKA_BROKERS=$KAFKA_BROKERS
EOF
