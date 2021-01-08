#!/bin/bash
set -euo pipefail

STEP_BREADCRUMB='~~~~~~~~'

pushd "$(dirname $0)"
source k8s-common-functions.sh

# spark k8s test - runs in sparkop namespace (so it doesn't interfere with a concurrently
# running EMR test).
NAMESPACE=sparkop
RELEASE=sparkop

# Clean up old release
k8s_cleanup "$RELEASE" "$NAMESPACE"

# Helm install everything in a namespace
helm_install "$RELEASE" "${DOCKER_REPOSITORY}" "${GIT_TAG}" "$NAMESPACE" --create-namespace

# Delete all sparkapplication resources that may be left over from the previous test runs.
kubectl delete sparkapplication --all -n "$NAMESPACE" || true

# Make sure the test pod has permissions to create sparkapplication resources
setup_sparkop_role

echo "DONE"