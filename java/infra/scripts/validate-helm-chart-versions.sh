#!/usr/bin/env bash

function finish {
  echo "Please ensure the Chart.yaml have the version ${1}"
  exit
}

trap "finish $1" ERR

set -e

if [ $# -ne 1 ]; then
    echo "Please provide a single semver version (without a \"v\" prefix) to test the repository against, e.g 0.99.0"
    exit 1
fi

# Get project root
PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)

echo "Trying to find version ${1} in the feast-core Chart.yaml. Exiting if not found."
grep "version: ${1}" "${PROJECT_ROOT_DIR}/infra/charts/feast-core/Chart.yaml"

echo "Trying to find version ${1} in the feast-serving Chart.yaml. Exiting if not found."
grep "version: ${1}" "${PROJECT_ROOT_DIR}/infra/charts/feast-serving/Chart.yaml"

echo "Success! All versions found!"