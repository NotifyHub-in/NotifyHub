#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-ap-south-1}"
AWS_PROFILE="${AWS_PROFILE:-}"
if [[ -z "${AWS_PROFILE}" ]]; then
  echo "AWS_PROFILE is required." >&2
  exit 1
fi
CLUSTER_NAME="${CLUSTER_NAME:-notification-control-plane-prod}"
APP_NAMESPACE="${APP_NAMESPACE:-platform}"
METRICS_NAMESPACE="${METRICS_NAMESPACE:-metrics}"
EXPORTERS_NAMESPACE="${EXPORTERS_NAMESPACE:-exporters}"
SECRETS_DIR="${SECRETS_DIR:-/tmp/notification-control-plane-secrets}"
IMAGE_TAG="${IMAGE_TAG:-aws-$(git rev-parse --short HEAD)}"
ECR_REPO_PREFIX="${ECR_REPO_PREFIX:-notification-control-plane}"
DATABASE_URL="${DATABASE_URL:-}"
KAFKA_BROKERS="${KAFKA_BROKERS:-}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"
READ_TOKEN="${READ_TOKEN:-}"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-}"

if [[ -z "$DATABASE_URL" ]]; then
  echo "DATABASE_URL is required for AWS deployment." >&2
  exit 1
fi

if [[ -z "$KAFKA_BROKERS" ]]; then
  echo "KAFKA_BROKERS is required for AWS deployment." >&2
  exit 1
fi

if [[ -z "$ADMIN_TOKEN" ]]; then
  ADMIN_TOKEN="$(openssl rand -hex 24)"
fi

if [[ -z "$READ_TOKEN" ]]; then
  READ_TOKEN="$(openssl rand -hex 24)"
fi

if [[ -z "$GRAFANA_ADMIN_PASSWORD" ]]; then
  GRAFANA_ADMIN_PASSWORD="$(openssl rand -hex 18)"
fi

AWS_ACCOUNT_ID="$(aws sts get-caller-identity --profile "$AWS_PROFILE" --query Account --output text)"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

ensure_repo() {
  local repo_name="$1"
  if ! aws ecr describe-repositories --profile "$AWS_PROFILE" --region "$AWS_REGION" --repository-names "$repo_name" >/dev/null 2>&1; then
    aws ecr create-repository --profile "$AWS_PROFILE" --region "$AWS_REGION" --repository-name "$repo_name" >/dev/null
  fi
}

push_image() {
  local component="$1"
  local dockerfile="$2"
  local repo_name="${ECR_REPO_PREFIX}/${component}"
  local image_uri="${ECR_REGISTRY}/${repo_name}:${IMAGE_TAG}"
  ensure_repo "$repo_name"
  docker buildx build \
    --platform linux/amd64 \
    -f "$dockerfile" \
    -t "$image_uri" \
    --push \
    .
}

aws ecr get-login-password --profile "$AWS_PROFILE" --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_REGISTRY"

push_image "api" "build/docker/api.Dockerfile"
push_image "worker" "build/docker/worker.Dockerfile"
push_image "callback-gateway" "build/docker/callback-gateway.Dockerfile"
push_image "migrate" "build/docker/migrate.Dockerfile"
push_image "connector-email" "build/docker/connector-email.Dockerfile"
push_image "connector-sms" "build/docker/connector-sms.Dockerfile"
push_image "connector-webhook" "build/docker/connector-webhook.Dockerfile"
push_image "connector-push" "build/docker/connector-push.Dockerfile"
push_image "connector-whatsapp" "build/docker/connector-whatsapp.Dockerfile"

kubectl create namespace "$APP_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace "$METRICS_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace "$EXPORTERS_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$APP_NAMESPACE" create secret generic notification-control-plane-auth \
  --from-literal=notification-admin-api-token="$ADMIN_TOKEN" \
  --from-literal=notification-readonly-api-token="$READ_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$EXPORTERS_NAMESPACE" create secret generic notification-control-plane-observability-postgres-dsn \
  --from-literal=data-source-name="$DATABASE_URL" \
  --dry-run=client -o yaml | kubectl apply -f -

SECRET_FILES=(
  "firebase_service_account.json"
  "smtp_user.txt"
  "smtp_password.txt"
  "sms_username.txt"
  "sms_password.txt"
  "whatsapp_password.txt"
  "gupshup_whatsapp_callback_secret.txt"
  "gupshup_sms_callback_secret.txt"
  "provider_callback_secret.txt"
)

SECRET_ARGS=()
for file in "${SECRET_FILES[@]}"; do
  if [[ -f "${SECRETS_DIR}/${file}" ]]; then
    SECRET_ARGS+=(--from-file="${file}=${SECRETS_DIR}/${file}")
  fi
done

if [[ "${#SECRET_ARGS[@]}" -gt 0 ]]; then
  kubectl -n "$APP_NAMESPACE" create secret generic notification-control-plane-secrets \
    "${SECRET_ARGS[@]}" \
    --dry-run=client -o yaml | kubectl apply -f -
else
  echo "No provider secret files were found in ${SECRETS_DIR}; skipping notification-control-plane-secrets creation." >&2
fi

APP_VALUES="$(mktemp)"
OBS_VALUES="$(mktemp)"
trap 'rm -f "$APP_VALUES" "$OBS_VALUES"' EXIT

cat > "$APP_VALUES" <<EOF
global:
  appEnv: production
  appVersion: "${IMAGE_TAG}"
  imagePullPolicy: IfNotPresent
  imagePullSecrets: []
  labels: {}
  secretVolume:
    enabled: true
    name: notification-control-plane-secrets
    mountPath: /run/notification-secrets
    readOnly: true

auth:
  createSecret: false
  existingSecret: notification-control-plane-auth
  adminToken: ""
  readToken: ""
  adminTokenKey: notification-admin-api-token
  readTokenKey: notification-readonly-api-token

database:
  url: "${DATABASE_URL}"

kafka:
  brokers: "${KAFKA_BROKERS}"

api:
  replicaCount: 2
  port: 8080
  authRequired: true
  rateLimit:
    enabled: true
    anonymousRPS: 2
    anonymousBurst: 4
    clientRPS: 10
    clientBurst: 20
    readRPS: 15
    readBurst: 30
    adminRPS: 10
    adminBurst: 20
    cleanupInterval: 5m
    entryTTL: 15m

images:
  api:
    repository: "${ECR_REGISTRY}/${ECR_REPO_PREFIX}/api"
    tag: "${IMAGE_TAG}"
  worker:
    repository: "${ECR_REGISTRY}/${ECR_REPO_PREFIX}/worker"
    tag: "${IMAGE_TAG}"
  callback-gateway:
    repository: "${ECR_REGISTRY}/${ECR_REPO_PREFIX}/callback-gateway"
    tag: "${IMAGE_TAG}"
  migrate:
    repository: "${ECR_REGISTRY}/${ECR_REPO_PREFIX}/migrate"
    tag: "${IMAGE_TAG}"
  connector-email:
    repository: "${ECR_REGISTRY}/${ECR_REPO_PREFIX}/connector-email"
    tag: "${IMAGE_TAG}"
  connector-sms:
    repository: "${ECR_REGISTRY}/${ECR_REPO_PREFIX}/connector-sms"
    tag: "${IMAGE_TAG}"
  connector-webhook:
    repository: "${ECR_REGISTRY}/${ECR_REPO_PREFIX}/connector-webhook"
    tag: "${IMAGE_TAG}"
  connector-push:
    repository: "${ECR_REGISTRY}/${ECR_REPO_PREFIX}/connector-push"
    tag: "${IMAGE_TAG}"
  connector-whatsapp:
    repository: "${ECR_REGISTRY}/${ECR_REPO_PREFIX}/connector-whatsapp"
    tag: "${IMAGE_TAG}"
EOF

cat > "$OBS_VALUES" <<EOF
global:
  appEnv: production
  labels: {}

appReleaseName: notification-control-plane
appNamespace: ${APP_NAMESPACE}
exportersNamespace: ${EXPORTERS_NAMESPACE}

postgresExporter:
  existingSecret: notification-control-plane-observability-postgres-dsn
  existingSecretKey: data-source-name
  dataSourceName: ""

kafkaExporter:
  brokersCSV: ${KAFKA_BROKERS}

grafana:
  adminUser: admin
  adminPassword: "${GRAFANA_ADMIN_PASSWORD}"
EOF

helm upgrade --install notification-control-plane deployments/helm/notification-control-plane \
  --namespace "$APP_NAMESPACE" \
  --create-namespace \
  -f "$APP_VALUES"

helm upgrade --install notification-control-plane-observability deployments/helm/notification-control-plane-observability \
  --namespace "$METRICS_NAMESPACE" \
  --create-namespace \
  -f "$OBS_VALUES"

echo "The control plane and observability charts were installed."
echo "Admin token secret: kubectl -n ${APP_NAMESPACE} get secret notification-control-plane-auth -o jsonpath='{.data.notification-admin-api-token}' | base64 -d"
echo "Read token secret: kubectl -n ${APP_NAMESPACE} get secret notification-control-plane-auth -o jsonpath='{.data.notification-readonly-api-token}' | base64 -d"
echo "Grafana password secret: kubectl -n ${METRICS_NAMESPACE} get secret notification-control-plane-observability-grafana-admin -o jsonpath='{.data.admin-password}' | base64 -d"
