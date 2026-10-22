#!/bin/bash

# Specify the RBAC type (folder)
read -p "Enter RBAC type (e.g., k8s or oidc): " FOLDER

echo "You have selected the RBAC type: $FOLDER"

# feature_store files name for the servers
OFFLINE_YAML="feature_store_offline.yaml"
ONLINE_YAML="feature_store_online.yaml"
REGISTRY_YAML="feature_store_registry.yaml"

# Helm chart path and service account
HELM_CHART_PATH="../../infra/charts/feast-feature-server"
SERVICE_ACCOUNT_NAME="feast-sa"
CLIENT_REPO_DIR="client/$FOLDER/feature_repo"

# Function to check if a file exists and encode it to base64
encode_to_base64() {
  local file_path=$1
  if [ ! -f "$file_path" ]; then
    echo "Error: File not found at $file_path"
    exit 1
  fi
  base64 < "$file_path"
}

FEATURE_STORE_OFFLINE_YAML_PATH="server/$FOLDER/$OFFLINE_YAML"
FEATURE_STORE_ONLINE_YAML_PATH="server/$FOLDER/$ONLINE_YAML"
FEATURE_STORE_REGISTRY_YAML_PATH="server/$FOLDER/$REGISTRY_YAML"

# Encode the YAML files to base64
FEATURE_STORE_OFFLINE_YAML_BASE64=$(encode_to_base64 "$FEATURE_STORE_OFFLINE_YAML_PATH")
FEATURE_STORE_ONLINE_YAML_BASE64=$(encode_to_base64 "$FEATURE_STORE_ONLINE_YAML_PATH")
FEATURE_STORE_REGISTRY_YAML_BASE64=$(encode_to_base64 "$FEATURE_STORE_REGISTRY_YAML_PATH")

# Check if base64 encoding was successful
if [ -z "$FEATURE_STORE_OFFLINE_YAML_BASE64" ] || [ -z "$FEATURE_STORE_ONLINE_YAML_BASE64" ] || [ -z "$FEATURE_STORE_REGISTRY_YAML_BASE64" ]; then
  echo "Error: Failed to base64 encode one or more feature_store.yaml files in folder $FOLDER."
  exit 1
fi

# Upgrade or install Feast components for the specified folder
read -p "Deploy Feast server components for $FOLDER? (y/n) " confirm_server
if [[ $confirm_server == [yY] ]]; then
  # Apply the server service accounts and role bindings
  kubectl apply -f "server/k8s/server_resources.yaml"

  # Upgrade or install Feast components
  echo "Upgrading or installing Feast server components for $FOLDER"

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

  echo "Server components deployed for $FOLDER."
else
  echo "Server components not deployed for $FOLDER."
fi

read -p "Apply client creation examples ? (y/n) " confirm_clients
if [[ $confirm_clients == [yY] ]]; then
  kubectl delete configmap client-feature-repo-config --ignore-not-found
  kubectl create configmap client-feature-repo-config --from-file=$CLIENT_REPO_DIR

  kubectl apply -f "client/$FOLDER/admin_user_resources.yaml"
  kubectl apply -f "client/$FOLDER/readonly_user_resources.yaml"
  kubectl apply -f "client/$FOLDER/unauthorized_user_resources.yaml"

  echo "Client resources applied."
else
  echo "Client resources not applied."
fi

read -p "Apply 'feast apply' in the remote registry? (y/n) " confirm_apply
if [[ $confirm_apply == [yY] ]]; then

  POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep '^feast-registry-server-feast-feature-server')

  if [ -z "$POD_NAME" ]; then
    echo "No pod found with the prefix feast-registry-server-feast-feature-server"
    exit 1
  fi

  LOCAL_DIR="./server/feature_repo/"
  REMOTE_DIR="/app/"

  echo "Copying files from $LOCAL_DIR to $POD_NAME:$REMOTE_DIR"
  kubectl cp $LOCAL_DIR $POD_NAME:$REMOTE_DIR

  echo "Files copied successfully!"

  kubectl exec $POD_NAME -- feast -c feature_repo apply
  echo "'feast apply' command executed successfully in the for remote registry."
else
  echo "'feast apply' not performed ."
fi

echo "Setup completed."
