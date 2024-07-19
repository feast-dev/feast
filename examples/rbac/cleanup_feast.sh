#!/bin/bash

DEFAULT_HELM_RELEASES=("feast-feature-server" "feast-offline-server" "feast-ui-server" "feast-registry-server")
NAMESPACE="feast-qe"

HELM_RELEASES=(${1:-${DEFAULT_HELM_RELEASES[@]}})
NAMESPACE=${2:-$NAMESPACE}

echo "Deleting Helm releases..."
for release in "${HELM_RELEASES[@]}"; do
  helm uninstall $release -n $NAMESPACE
done

echo "Deleting Kubernetes roles, role bindings, and service accounts..."
kubectl delete -f role.yaml
kubectl delete -f role_binding.yaml
kubectl delete -f service_account.yaml

echo "Cleanup completed."
echo "Cleanup completed."
