#!/bin/bash

set -euo pipefail

STEP_BREADCRUMB='~~~~~~~~'
SECONDS=0
TIMEFORMAT="${STEP_BREADCRUMB} took %R seconds"

function k8s_cleanup {
    local RELEASE=$1
    local NAMESPACE=$2

    # Create namespace if it doesn't exist.
    kubectl create namespace "$NAMESPACE" || true

    # Uninstall previous feast release if there is any.
    helm uninstall "$RELEASE" -n "$NAMESPACE" || true

    # `helm uninstall` doesn't remove PVCs, delete them manually.
    time kubectl delete pvc --all -n "$NAMESPACE" || true

    kubectl get service -n "$NAMESPACE"

    # Set a new postgres password. Note that the postgres instance is not available outside
    # the k8s cluster anyway so it doesn't have to be super secure.
    echo "${STEP_BREADCRUMB} Setting PG password"
    PG_PASSWORD=$(head -c 59 /dev/urandom | md5sum | head -c 16)
    kubectl delete secret feast-postgresql -n "$NAMESPACE" || true
    kubectl create secret generic feast-postgresql --from-literal=postgresql-password="$PG_PASSWORD" -n "$NAMESPACE"
}

function helm_install {
    # helm install Feast into k8s cluster and display a nice error if it fails.
    # Usage: helm_install $RELEASE $DOCKER_REPOSITORY $GIT_TAG ...
    # Args:
    #   $RELEASE is helm release name
    #   $DOCKER_REPOSITORY is the docker repo containing feast images tagged with $GIT_TAG
    #   ... you can pass additional args to this function that are passed on to helm install

    local RELEASE=$1
    local DOCKER_REPOSITORY=$2
    local GIT_TAG=$3

    shift 3

    # We skip statsd exporter and other metrics stuff since we're not using it anyway, and it
    # has some issues with unbound PVCs (that cause kubectl delete pvc to hang).
    echo "${STEP_BREADCRUMB} Helm installing feast"

    if ! time helm install --wait "$RELEASE" infra/charts/feast \
        --timeout 15m \
        --set "feast-jupyter.image.repository=${DOCKER_REPOSITORY}/feast-jupyter" \
        --set "feast-jupyter.image.tag=${GIT_TAG}" \
        --set "feast-online-serving.image.repository=${DOCKER_REPOSITORY}/feast-serving" \
        --set "feast-online-serving.image.tag=${GIT_TAG}" \
        --set "feast-jobservice.image.repository=${DOCKER_REPOSITORY}/feast-jobservice" \
        --set "feast-jobservice.image.tag=${GIT_TAG}" \
        --set "feast-core.image.repository=${DOCKER_REPOSITORY}/feast-core" \
        --set "feast-core.image.tag=${GIT_TAG}" \
        --set "prometheus-statsd-exporter.enabled=false" \
        --set "prometheus.enabled=false" \
        --set "grafana.enabled=false" \
        --set "feast-jobservice.enabled=false" \
        "$@" ; then

        echo "Error during helm install. "
        kubectl -n "$NAMESPACE" get pods

        readarray -t CRASHED_PODS < <(kubectl -n "$NAMESPACE" get pods --no-headers=true | grep "$RELEASE" | awk '{if ($2 == "0/1") { print $1 } }')
        echo "Crashed pods: ${CRASHED_PODS[*]}"

        for POD in "${CRASHED_PODS[@]}"; do
            echo "Logs from pod error $POD:"
            kubectl -n "$NAMESPACE" logs "$POD" --previous
        done

        exit 1
    fi
}

function setup_sparkop_role {
    # Set up permissions for the default user in sparkop namespace so that Feast SDK can manage
    # sparkapplication resources from the test runner pod.

    cat <<EOF | kubectl apply -f -
kind: Role
apiVersion: rbac.authorization.k8s.io/v1beta1
metadata:
  name: use-spark-operator
rules:
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["create", "delete", "deletecollection", "get", "list", "update", "watch", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1beta1
kind: RoleBinding
metadata:
  name: use-spark-operator
roleRef:
  kind: Role
  name: use-spark-operator
  apiGroup: rbac.authorization.k8s.io
subjects:
  - kind: ServiceAccount
    name: default
EOF
}

GIT_TAG=$(git rev-parse HEAD)
GIT_REMOTE_URL=$(git config --get remote.origin.url)

echo "########## Starting e2e tests for ${GIT_REMOTE_URL} ${GIT_TAG} ###########"

# This seems to make builds a bit faster.
export DOCKER_BUILDKIT=1

# Workaround for COPY command in core docker image that pulls local maven repo into the image
# itself.
mkdir .m2 2>/dev/null || true

# Log into k8s.
echo "${STEP_BREADCRUMB} Updating kubeconfig"
az login --service-principal -u "$AZ_SERVICE_PRINCIPAL_ID" -p "$AZ_SERVICE_PRINCIPAL_PASS" --tenant "$AZ_SERVICE_PRINCIPAL_TENANT_ID"
az aks get-credentials --resource-group "$RESOURCE_GROUP" --name "$AKS_CLUSTER_NAME"

# Sanity check that kubectl is working.
echo "${STEP_BREADCRUMB} k8s sanity check"
kubectl get pods

# e2e test - runs in sparkop namespace for consistency with AWS sparkop test.
NAMESPACE=sparkop
RELEASE=sparkop

# Delete old helm release and PVCs
k8s_cleanup "$RELEASE" "$NAMESPACE"

# Helm install everything in a namespace
helm_install "$RELEASE" "${DOCKER_REPOSITORY}" "${GIT_TAG}" --namespace "$NAMESPACE"

# Delete old test running pod if it exists
kubectl delete pod -n "$NAMESPACE" ci-test-runner 2>/dev/null || true

# Delete all sparkapplication resources that may be left over from the previous test runs.
kubectl delete sparkapplication --all -n "$NAMESPACE" || true

# Make sure the test pod has permissions to create sparkapplication resources
setup_sparkop_role

# Run the test suite as a one-off pod.
echo "${STEP_BREADCRUMB} Running the test suite"
time kubectl run --rm -n "$NAMESPACE" -i ci-test-runner  \
    --restart=Never \
    --image="${DOCKER_REPOSITORY}/feast-ci:${GIT_TAG}" \
    --env="STAGING_PATH=$STAGING_PATH" \
    --  \
    bash -c "mkdir src && cd src && git clone $GIT_REMOTE_URL && cd feast && git config remote.origin.fetch '+refs/pull/*:refs/remotes/origin/pull/*' && git fetch -q && git checkout $GIT_TAG && ./infra/scripts/setup-e2e-env-sparkop.sh && ./infra/scripts/test-end-to-end-sparkop.sh"

echo "########## e2e tests took $SECONDS seconds ###########"
