#!/bin/bash

DEFAULT_HELM_RELEASES=("feast-feature-server" "feast-offline-server" "feast-registry-server")
NAMESPACE="feast-dev"

HELM_RELEASES=(${1:-${DEFAULT_HELM_RELEASES[@]}})
NAMESPACE=${2:-$NAMESPACE}

echo "Deleting Helm releases..."
for release in "${HELM_RELEASES[@]}"; do
  helm uninstall $release -n $NAMESPACE
done

echo "Deleting Kubernetes roles, role bindings, and service accounts for clients"
kubectl delete -f client/admin_user_resources.yaml
kubectl delete -f client/readonly_user_resources.yaml
kubectl delete -f client/unauthorized_user_resources.yaml
kubectl delete -f server/server_resources.yaml
kubectl delete configmap client-feature-repo-config


echo "Cleanup completed."
