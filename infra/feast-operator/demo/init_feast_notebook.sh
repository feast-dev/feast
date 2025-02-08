#!/bin/bash

NOTEBOOK_NAME=""
FEAST_NAME=""
CM_MOUNT_PATH="/feast/feature_repository"

show_help() {
  echo "Usage: init_feast_notebook.sh [--notebook <notebook_name>] [--feast <feast_name>] [--help]"
  echo
  echo "Options:"
  echo "  --notebook   Specify the notebook name. Defaults to the only Notebook instance in the current namespace."
  echo "  --feast      Specify the Feast instance name. Defaults to the only Feast instance in the current namespace."
  echo "  --help       Display this help message."
  exit 0
}

get_single_resource_name() {
  local resource_type=$1
  local resource_count
  resource_count=$(kubectl get "$resource_type" --no-headers | wc -l)
  
  if [ "$resource_count" -eq 1 ]; then
    kubectl get "$resource_type" --no-headers | awk '{print $1}'
  else
    echo ""
  fi
}

patch_config_map() {
  local notebook_name=$1
  local config_map_name=$2
  local mount_path=$3
  echo "Patching Notebook $notebook_name with ConfigMap $config_map_name"
  kubectl patch notebook "$notebook_name" -n feast --type='json' -p="[
    {
      \"op\": \"add\",
      \"path\": \"/spec/template/spec/volumes/-\",
      \"value\": {
        \"name\": \"$config_map_name\",
        \"configMap\": {
          \"name\": \"$config_map_name\"
        }
      }
    },
    {
      \"op\": \"add\",
      \"path\": \"/spec/template/spec/containers/0/volumeMounts/-\",
      \"value\": {
        \"name\": \"$config_map_name\",
        \"mountPath\": \"$mount_path\"
      }
    }
  ]"
}

get_tls_secret_name() {
  local service_type=$1
  local config_map_name=$2
  kubectl get cm $config_map_name -oyaml | yq '.data."feature_store.yaml"' | yq ".${service_type}.cert" | xargs dirname
}

patch_tls_secret(){
  local notebook_name=$1
  local secret_name=$2
  local mount_path=$3

  echo "Patching Notebook $notebook_name with Secret $secret_name mounted at $mount_path"
  kubectl patch notebook "$notebook_name" -n feast --type='json' -p="[
    {
      \"op\": \"add\",
      \"path\": \"/spec/template/spec/volumes/-\",
      \"value\": {
        \"name\": \"$secret_name\",
        \"secret\": {
          \"defaultMode\": 420,
          \"secretName\": \"$secret_name\"
        }
      }
    },
    {
      \"op\": \"add\",
      \"path\": \"/spec/template/spec/containers/0/volumeMounts/-\",
      \"value\": {
        \"name\": \"$secret_name\",
        \"mountPath\": \"$mount_path\"
      }
    }
  ]"
}

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --notebook)
      NOTEBOOK_NAME="$2"
      shift
      shift
      ;;
    --feast)
      FEAST_NAME="$2"
      shift
      shift
      ;;
    --help)
      show_help
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

if [ -z "$NOTEBOOK_NAME" ]; then
  NOTEBOOK_NAME=$(get_single_resource_name "notebook")
  if [ -z "$NOTEBOOK_NAME" ]; then
    echo "Error: Multiple or no Notebook instances found. Specify the --notebook parameter."
    exit 1
  fi
fi

if [ -z "$FEAST_NAME" ]; then
  FEAST_NAME=$(get_single_resource_name "feast")
  if [ -z "$FEAST_NAME" ]; then
    echo "Error: Multiple or no Feast instances found. Specify the --feast parameter."
    exit 1
  fi
fi

echo "Notebook Name: $NOTEBOOK_NAME"
echo "Feast Name: $FEAST_NAME"
echo "ConfigMap Mount Path: $CM_MOUNT_PATH"

echo "Connecting Notebook '$NOTEBOOK_NAME' with Feast '$FEAST_NAME'..."

client_configmap_name="feast-${FEAST_NAME}-client"
patch_config_map $NOTEBOOK_NAME $client_configmap_name $CM_MOUNT_PATH

offline_tls_mount_path=$(get_tls_secret_name "offline_store" $client_configmap_name)
online_tls_mount_path=$(get_tls_secret_name "online_store" $client_configmap_name)
registry_tls_mount_path=$(get_tls_secret_name "registry" $client_configmap_name)
echo "Offline TLS Mount Path: $offline_tls_secret_name"
echo "Online TLS Mount Path: $online_tls_mount_path"
echo "Registry TLS Mount Path: $registry_tls_mount_path"

if [ "$offline_tls_mount_path" != "." ]; then
  secret_name="feast-${FEAST_NAME}-offline-tls"
  patch_tls_secret $NOTEBOOK_NAME $secret_name ${offline_tls_mount_path}
fi
if [ "$online_tls_mount_path" != "." ]; then
  secret_name="feast-${FEAST_NAME}-online-tls"
  patch_tls_secret $NOTEBOOK_NAME $secret_name ${online_tls_mount_path}
fi
if [ "$registry_tls_mount_path" != "." ]; then
  secret_name="feast-${FEAST_NAME}-registry-tls"
  patch_tls_secret $NOTEBOOK_NAME $secret_name ${registry_tls_mount_path}
fi




