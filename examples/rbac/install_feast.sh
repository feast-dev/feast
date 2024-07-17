#!/bin/bash

# Default paths
DEFAULT_FEATURE_STORE_YAML_PATH="feature_store_remote.yaml"
DEFAULT_HELM_CHART_PATH="../../infra/charts/feast-feature-server"

# Accept parameters or use default values
FEATURE_STORE_YAML_PATH=${1:-$DEFAULT_FEATURE_STORE_YAML_PATH}
HELM_CHART_PATH=${2:-$DEFAULT_HELM_CHART_PATH}

# Check if the feature_store.yaml file exists
if [ ! -f "$FEATURE_STORE_YAML_PATH" ]; then
  echo "Error: File not found at $FEATURE_STORE_YAML_PATH"
  exit 1
fi

# Print the file path
echo "File found at $FEATURE_STORE_YAML_PATH"

# Base64 encode the feature_store.yaml file and store it in a variable
FEATURE_STORE_YAML_BASE64=$(base64 < "$FEATURE_STORE_YAML_PATH")

# Check if base64 encoding was successful
if [ -z "$FEATURE_STORE_YAML_BASE64" ]; then
  echo "Error: Failed to base64 encode the feature_store.yaml file."
  exit 1
fi

# Print the base64 encoded content (optional, for debugging purposes)
# echo "Base64 Encoded Content: $FEATURE_STORE_YAML_BASE64"

# Upgrade or install feast-feature-server
helm upgrade --install feast-feature-server $HELM_CHART_PATH \
  --set feature_store_yaml_base64=$FEATURE_STORE_YAML_BASE64

# Upgrade or install feast-offline-server
helm upgrade --install feast-offline-server $HELM_CHART_PATH \
  --set feast_mode=offline \
  --set feature_store_yaml_base64=$FEATURE_STORE_YAML_BASE64

# Upgrade or install feast-registry-server
helm upgrade --install feast-registry-server $HELM_CHART_PATH \
  --set feast_mode=registry \
  --set feature_store_yaml_base64=$FEATURE_STORE_YAML_BASE64
