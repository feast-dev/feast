#!/usr/bin/env bash

# This script will scan through a list of files to validate that all versions are consistent with
# - fix-version-cross-branch-ref version:  (could be snapshot)
# - Docker images version: 'dev' on fix-version-cross-branch-ref, Highest tag on release branches 
# - Release version: Highest stable commit. Latest tag repo wide, release candidates not included
set -e

# Matches (ie vMAJOR.MINOR-branch) release branch names
RELEASE_BRANCH_REGEX="v[0-9]+\.[0-9]+-branch"

# Determine the current Feast version from Maven (pom.xml)
export FEAST_MAVEN_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
[[ -z "$FEAST_MAVEN_VERSION" ]] && {
  echo "$FEAST_MAVEN_VERSION is missing, please check pom.xml and maven"
  exit 1
}
echo "Linting Maven version: $FEAST_MAVEN_VERSION"

# Determine Docker image version tag relative to current branch
BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
if [ $BRANCH_NAME = "fix-version-cross-branch-ref" ]
then
    # Use development version
    FEAST_DOCKER_VERSION="dev"
elif echo "$BRANCH_NAME" | grep -P $RELEASE_BRANCH_REGEX &>/dev/null
then
    # Use last release tag tagged on the release branch
    LAST_MERGED_TAG=$(git tag -l --sort -version:refname --merged | head -n 1)
    FEAST_DOCKER_VERSION=${LAST_MERGED_TAG:"v"}
else
    # Do not enforce version linting as we don't know if the target merge branch
    FEAST_DOCKER_VERSION="_ANY"
fi
[[ -z "$FEAST_DOCKER_VERSION" ]] && {
  echo "FEAST_DOCKER_VERSION is missing"
  exit 1
}
export FEAST_DOCKER_VERSION
echo "Linting docker image version: $FEAST_DOCKER_VERSION"

# Determine highest stable version relative to current branch
# Regular expression for matching stable tags in the format vMAJOR.MINOR.PATCH
STABLE_TAG_REGEX="v[0-9]+\.[0-9]+\.[0-9]+"
if [ $BRANCH_NAME = "fix-version-cross-branch-ref" ]
then
    # Use last stable tag repo wide
    LAST_STABLE_TAG=$(git tag --sort -version:refname | grep -P "$STABLE_TAG_REGEX" | head -n 1)
    FEAST_STABLE_VERSION=${LAST_STABLE_TAG#"v"}
elif echo "$BRANCH_NAME" | grep -P $RELEASE_BRANCH_REGEX &>/dev/null
then
    # Use last stable tag tagged on the release branch
    LAST_STABLE_MERGE_TAG=$(git tag --sort -version:refname --merged | grep -P "$STABLE_TAG_REGEX" | head -n 1)
    FEAST_STABLE_VERSION=${LAST_STABLE_MERGE_TAG#"v"}
else
    # Do not enforce version linting as we don't know if the target merge branch
    FEAST_STABLE_VERSION="_ANY"
fi
[[ -z "$FEAST_STABLE_VERSION" ]] && {
  echo "FEAST_STABLE_VERSION is missing"
  exit 1
}
export FEAST_STABLE_VERSION
echo "Linting stable version: $FEAST_STABLE_VERSION"

# List of files to validate with fix-version-cross-branch-ref version (from pom.xml)
# Structure is a comma separated list of structure
# <File to validate>, <Amount of occurrences of specific version to look for>, <version to look for>

declare -a files_to_validate_version=(
  "infra/charts/feast/Chart.yaml,1,${FEAST_MAVEN_VERSION}"
  "infra/charts/feast/charts/feast-core/Chart.yaml,1,${FEAST_MAVEN_VERSION}"
  "infra/charts/feast/charts/feast-core/values.yaml,1,${FEAST_STABLE_VERSION}"
  "infra/charts/feast/charts/feast-core/README.md,1,${FEAST_STABLE_VERSION}"
  "infra/charts/feast/charts/feast-serving/Chart.yaml,1,${FEAST_MAVEN_VERSION}"
  "infra/charts/feast/charts/feast-jupyter/values.yaml,1,${FEAST_STABLE_VERSION}"
  "infra/charts/feast/charts/feast-jupyter/README.md,1,${FEAST_STABLE_VERSION}"
  "infra/charts/feast/charts/feast-jupyter/Chart.yaml,1,${FEAST_MAVEN_VERSION}"
  "infra/charts/feast/charts/feast-serving/values.yaml,1,${FEAST_STABLE_VERSION}"
  "infra/charts/feast/charts/feast-serving/README.md,1,${FEAST_STABLE_VERSION}"
  "infra/charts/feast/charts/feast-jobcontroller/Chart.yaml,1,${FEAST_MAVEN_VERSION}"
  "infra/charts/feast/charts/feast-jobcontroller/values.yaml,1,${FEAST_STABLE_VERSION}"
  "infra/charts/feast/charts/feast-jobcontroller/README.md,1,${FEAST_STABLE_VERSION}"
  "infra/charts/feast/requirements.yaml,4,${FEAST_MAVEN_VERSION}"
  "infra/charts/feast/requirements.lock,4,${FEAST_STABLE_VERSION}"
  "infra/docker-compose/.env.sample,1,${FEAST_STABLE_VERSION}"
)

echo
echo "Testing list of files to ensure they have the correct version"
echo

for i in "${files_to_validate_version[@]}"; do
  IFS=',' read -r FILE_PATH EXPECTED_OCCURRENCES VERSION <<<"${i}"
  # Disable version lint if '_ANY' specified as version.
  if [ "$VERSION" = "_ANY" ]
  then
      continue
  fi
  
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
