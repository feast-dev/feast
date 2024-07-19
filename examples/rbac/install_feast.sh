#!/bin/bash

# Default paths
DEFAULT_FEATURE_STORE_YAML_PATH="feature_store_remote.yaml"
DEFAULT_HELM_CHART_PATH="../../infra/charts/feast-feature-server"
DEFAULT_SERVICE_ACCOUNT_NAME="feast-sa"

# Accept parameters or use default values
FEATURE_STORE_YAML_PATH=${1:-$DEFAULT_FEATURE_STORE_YAML_PATH}
HELM_CHART_PATH=${2:-$DEFAULT_HELM_CHART_PATH}
SERVICE_ACCOUNT_NAME=${3:-$DEFAULT_SERVICE_ACCOUNT_NAME}

# Check if the feature_store.yaml file exists
if [ ! -f "$FEATURE_STORE_YAML_PATH" ]; then
  echo "Error: File not found at $FEATURE_STORE_YAML_PATH"
  exit 1
fi

echo "File found at $FEATURE_STORE_YAML_PATH"

# Base64 encode the feature_store.yaml file and store it in a variable
FEATURE_STORE_YAML_BASE64=$(base64 < "$FEATURE_STORE_YAML_PATH")

# Check if base64 encoding was successful
if [ -z "$FEATURE_STORE_YAML_BASE64" ]; then
  echo "Error: Failed to base64 encode the feature_store.yaml file."
  exit 1
fi

# echo "Base64 Encoded Content: $FEATURE_STORE_YAML_BASE64"

# Apply Kubernetes configurations
echo "Applying service account, roles, and role bindings"
kubectl apply -f service_account.yaml
kubectl apply -f role.yaml
kubectl apply -f role_binding.yaml

# Upgrade or install Feast components
echo "Upgrading or installing Feast components"
helm upgrade --install feast-feature-server $HELM_CHART_PATH \
  --set feature_store_yaml_base64=$FEATURE_STORE_YAML_BASE64 \
  --set serviceAccount.name=$SERVICE_ACCOUNT_NAME

helm upgrade --install feast-offline-server $HELM_CHART_PATH \
  --set feast_mode=offline \
  --set feature_store_yaml_base64=$FEATURE_STORE_YAML_BASE64 \
  --set serviceAccount.name=$SERVICE_ACCOUNT_NAME

helm upgrade --install feast-ui-server $HELM_CHART_PATH \
  --set feast_mode=ui \
  --set feature_store_yaml_base64=$FEATURE_STORE_YAML_BASE64 \
  --set serviceAccount.name=$SERVICE_ACCOUNT_NAME

helm upgrade --install feast-registry-server $HELM_CHART_PATH \
  --set feast_mode=registry \
  --set feature_store_yaml_base64=$FEATURE_STORE_YAML_BASE64 \
  --set serviceAccount.name=$SERVICE_ACCOUNT_NAME
