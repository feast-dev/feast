#!/usr/bin/env bash

# This script will scan through a list of files to validate that all versions are consistent with
# - Master version (could be snapshot)
# - Highest stable commit (latest tag)
set -e

# Determine the current Feast version from Maven (pom.xml)
export FEAST_MASTER_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
[[ -z "$FEAST_MASTER_VERSION" ]] && {
  echo "$FEAST_MASTER_VERSION is missing, please check pom.xml and maven"
  exit 1
}

# Determine the highest released version from Git history
FEAST_RELEASE_VERSION_WITH_V=$(git tag -l --sort -version:refname | head -n 1)
echo $FEAST_RELEASE_VERSION_WITH_V

export FEAST_RELEASE_VERSION=${FEAST_RELEASE_VERSION_WITH_V#"v"}
echo $FEAST_RELEASE_VERSION

[[ -z "$FEAST_RELEASE_VERSION" ]] && {
  echo "FEAST_RELEASE_VERSION is missing"
  exit 1
}

# List of files to validate with master version (from pom.xml)
# Structure is a comma separated list of structure
# <File to validate>, <Amount of occurrences of specific version to look for>, <version to look for>
#

declare -a files_to_validate_version=(
  "infra/charts/feast/Chart.yaml,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/charts/feast-core/Chart.yaml,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/charts/feast-core/values.yaml,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-core/README.md,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-serving/Chart.yaml,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/charts/feast-jupyter/values.yaml,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-jupyter/README.md,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-jupyter/Chart.yaml,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/charts/feast-serving/values.yaml,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-serving/README.md,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/requirements.yaml,4,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/requirements.lock,4,${FEAST_RELEASE_VERSION}"
  "infra/docker-compose/.env.sample,1,${FEAST_RELEASE_VERSION}"
)

echo
echo "Testing list of files to ensure they have the correct version"
echo

for i in "${files_to_validate_version[@]}"; do
  IFS=',' read -r FILE_PATH EXPECTED_OCCURRENCES VERSION <<<"${i}"
  echo
  echo
  echo "Testing whether versions are correctly set within file: $FILE_PATH"
  echo
  echo "File contents:"
  echo "========================================================="
  cat "$FILE_PATH"
  echo
  echo "========================================================="
  ACTUAL_OCCURRENCES=$(grep -c "$VERSION" "$FILE_PATH" || true)

  if [ "${ACTUAL_OCCURRENCES}" -eq "${EXPECTED_OCCURRENCES}" ]; then
    echo "SUCCESS"
    echo
    echo "Expecting $EXPECTED_OCCURRENCES occurrences of $VERSION in $FILE_PATH, and found $ACTUAL_OCCURRENCES"
  else
    echo "FAILURE"
    echo
    echo "Expecting $EXPECTED_OCCURRENCES occurrences of $VERSION in $FILE_PATH, but found $ACTUAL_OCCURRENCES"
    exit 1
  fi
  echo "========================================================="
done
