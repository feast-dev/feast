#!/bin/bash

# Default paths
FEATURE_STORE_OFFLINE_YAML_PATH="server/feature_store_offline.yaml"
FEATURE_STORE_ONLINE_YAML_PATH="server/feature_store_online.yaml"
FEATURE_STORE_REGISTRY_YAML_PATH="server/feature_store_registry.yaml"
HELM_CHART_PATH="../../infra/charts/feast-feature-server"
SERVICE_ACCOUNT_NAME="feast-sa"
CONFIG_DIR="client/feature_repo"

# Function to check if a file exists and encode it to base64
encode_to_base64() {
  local file_path=$1
  if [ ! -f "$file_path" ]; then
    echo "Error: File not found at $file_path"
    exit 1
  fi
  base64 < "$file_path"
}

# Encode the YAML files to base64
FEATURE_STORE_OFFLINE_YAML_BASE64=$(encode_to_base64 "$FEATURE_STORE_OFFLINE_YAML_PATH")
FEATURE_STORE_ONLINE_YAML_BASE64=$(encode_to_base64 "$FEATURE_STORE_ONLINE_YAML_PATH")
FEATURE_STORE_REGISTRY_YAML_BASE64=$(encode_to_base64 "$FEATURE_STORE_REGISTRY_YAML_PATH")

# Check if base64 encoding was successful
if [ -z "$FEATURE_STORE_OFFLINE_YAML_BASE64" ] || [ -z "$FEATURE_STORE_ONLINE_YAML_BASE64" ] || [ -z "$FEATURE_STORE_REGISTRY_YAML_BASE64" ]; then
  echo "Error: Failed to base64 encode one or more feature_store.yaml files."
  exit 1
fi

# Upgrade or install Feast components
read -p "Deploy server components? (y/n) " confirm_server
if [[ $confirm_server == [yY] ]]; then
  # Apply the server service accounts and role bindings
  kubectl apply -f server/server_resources.yaml

  # Upgrade or install Feast components
  echo "Upgrading or installing Feast server components"

  helm upgrade --install feast-registry-server $HELM_CHART_PATH \
    --set feast_mode=registry \
    --set feature_store_yaml_base64=$FEATURE_STORE_REGISTRY_YAML_BASE64 \
    --set serviceAccount.name=$SERVICE_ACCOUNT_NAME

  helm upgrade --install feast-feature-server $HELM_CHART_PATH \
    --set feature_store_yaml_base64=$FEATURE_STORE_ONLINE_YAML_BASE64 \
    --set serviceAccount.name=$SERVICE_ACCOUNT_NAME

  helm upgrade --install feast-offline-server $HELM_CHART_PATH \
    --set feast_mode=offline \
    --set feature_store_yaml_base64=$FEATURE_STORE_OFFLINE_YAML_BASE64 \
    --set serviceAccount.name=$SERVICE_ACCOUNT_NAME

  echo "Server components deployed."
else
  echo "Server components not deployed."
fi

read -p "Apply client creation examples (admin and user)? (y/n) " confirm_clients
if [[ $confirm_clients == [yY] ]]; then
  kubectl delete configmap client-feature-repo-config --ignore-not-found
  kubectl create configmap client-feature-repo-config --from-file=$CONFIG_DIR
  kubectl apply -f client/admin_resources.yaml
  kubectl apply -f client/user_resources.yaml
  echo "Client resources applied."
else
  echo "Client resources not applied."
fi

echo "Setup completed."
