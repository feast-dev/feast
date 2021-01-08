#!/bin/bash

set -euo pipefail

function wait_for_images {
    local DOCKER_REPOSITORY=$1
    local GIT_TAG=$2
    # Wait for images to be available in the docker repository; ci is the last image built
    timeout 15m bash -c "while ! gcloud container images list-tags ${DOCKER_REPOSITORY}/feast-ci --format=json | jq -e \".[] | select(.tags[] | contains (\\\"${GIT_TAG}\\\"))\" > /dev/null; do sleep 10s; done"
}

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
    echo "${STEP_BREADCRUMB:-} Setting PG password"

    # use either shasum or md5sum, whichever exists
    SUM=$(which md5sum shasum | grep -v "not found" | tail -n1 || true )

    PG_PASSWORD=$(head -c 59 /dev/urandom | $SUM | head -c 16)
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
    #   $NAMESPACE is the namespace name

    local RELEASE=$1
    local DOCKER_REPOSITORY=$2
    local GIT_TAG=$3
    local NAMESPACE=$4

    shift 4

    # We skip statsd exporter and other metrics stuff since we're not using it anyway, and it
    # has some issues with unbound PVCs (that cause kubectl delete pvc to hang).
    echo "${STEP_BREADCRUMB:-} Helm installing feast"

    if ! time helm install --wait "$RELEASE" ./infra/charts/feast \
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
        --namespace "$NAMESPACE" \
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
  namespace: sparkop
rules:
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["create", "delete", "deletecollection", "get", "list", "update", "watch", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1beta1
kind: RoleBinding
metadata:
  name: use-spark-operator
  namespace: sparkop
roleRef:
  kind: Role
  name: use-spark-operator
  apiGroup: rbac.authorization.k8s.io
subjects:
  - kind: ServiceAccount
    name: default
EOF
}