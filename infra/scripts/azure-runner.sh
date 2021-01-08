#!/usr/bin/env bash

set -euo pipefail

STEP_BREADCRUMB='~~~~~~~~'
SECONDS=0
TIMEFORMAT="${STEP_BREADCRUMB} took %R seconds"

GIT_TAG=$PULL_PULL_SHA
GIT_REMOTE_URL=https://github.com/feast-dev/feast.git

echo "########## Starting e2e tests for ${GIT_REMOTE_URL} ${GIT_TAG} ###########"

# Note requires running in root feast directory
source infra/scripts/k8s-common-functions.sh

# Workaround for COPY command in core docker image that pulls local maven repo into the image
# itself.
mkdir .m2 2>/dev/null || true

# Log into k8s.
echo "${STEP_BREADCRUMB} Updating kubeconfig"
az login --service-principal -u "$AZ_SERVICE_PRINCIPAL_ID" -p "$AZ_SERVICE_PRINCIPAL_PASS" --tenant "$AZ_SERVICE_PRINCIPAL_TENANT_ID" >/dev/null
az aks get-credentials --resource-group "$RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME"

# Sanity check that kubectl is working.
echo "${STEP_BREADCRUMB} k8s sanity check"
kubectl get pods

# e2e test - runs in sparkop namespace for consistency with AWS sparkop test.
NAMESPACE=sparkop
RELEASE=sparkop

# Delete old helm release and PVCs
k8s_cleanup "$RELEASE" "$NAMESPACE"

wait_for_images "${DOCKER_REPOSITORY}" "${GIT_TAG}"

# Helm install everything in a namespace
helm_install "$RELEASE" "${DOCKER_REPOSITORY}" "${GIT_TAG}" "$NAMESPACE" \
        --set "feast-jobservice.envOverrides.FEAST_AZURE_BLOB_ACCOUNT_NAME=${AZURE_BLOB_ACCOUNT_NAME}" \
        --set "feast-jobservice.envOverrides.FEAST_AZURE_BLOB_ACCOUNT_ACCESS_KEY=${AZURE_BLOB_ACCOUNT_ACCESS_KEY}"

# Delete old test running pod if it exists
kubectl delete pod -n "$NAMESPACE" ci-test-runner 2>/dev/null || true

# Delete all sparkapplication resources that may be left over from the previous test runs.
kubectl delete sparkapplication --all -n "$NAMESPACE" || true

# Make sure the test pod has permissions to create sparkapplication resources
setup_sparkop_role

# Run the test suite as a one-off pod.
echo "${STEP_BREADCRUMB} Running the test suite"
time kubectl run -n "$NAMESPACE" -i ci-test-runner  \
    --pod-running-timeout=5m \
    --restart=Never \
    --image="${DOCKER_REPOSITORY}/feast-ci:${GIT_TAG}" \
    --env="STAGING_PATH=${STAGING_PATH}" \
    --env="FEAST_AZURE_BLOB_ACCOUNT_NAME=${AZURE_BLOB_ACCOUNT_NAME}" \
    --env="FEAST_AZURE_BLOB_ACCOUNT_ACCESS_KEY=${AZURE_BLOB_ACCOUNT_ACCESS_KEY}" \
    --  \
    bash -c "mkdir src && cd src && git clone ${GIT_REMOTE_URL} && cd feast && git config remote.origin.fetch '+refs/pull/*:refs/remotes/origin/pull/*' && git fetch -q && git checkout ${GIT_TAG} && ./infra/scripts/setup-e2e-env-sparkop.sh && ./infra/scripts/test-end-to-end-sparkop.sh"

echo "########## e2e tests took $SECONDS seconds ###########"
