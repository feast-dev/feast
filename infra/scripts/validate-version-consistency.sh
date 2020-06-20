#!/usr/bin/env bash

# This script will scan through a list of files to validate that all versions are consistent with the master version
set -e

# List of files to validate
declare -a files_to_validate=(
  "infra/charts/feast/Chart.yaml"
  "infra/charts/feast/charts/feast-core/Chart.yaml"
  "infra/charts/feast/charts/feast-serving/Chart.yaml"
  "infra/charts/feast/charts/feast-jupyter/Chart.yaml"
  "infra/charts/feast/requirements.yaml" # We are only testing for the version once
)

# Determine the current Feast version from Maven (pom.xml)
export FEAST_MASTER_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
[[ -z "$FEAST_MASTER_VERSION" ]] && {
  echo "$FEAST_MASTER_VERSION is missing, please check pom.xml and maven"
  exit 1
}

echo
echo "Testing list of files to ensure they have the following version $FEAST_MASTER_VERSION"
echo

for i in "${files_to_validate[@]}"; do
  echo
  echo
  echo "Testing whether versions are correctly set within file: $i"
  echo
  echo "File contents:"
  echo "========================================================="
  cat "$i"
  echo "========================================================="
  grep -q "$FEAST_MASTER_VERSION" "$i"
  echo "SUCCESS: Version found"
  echo "========================================================="
done