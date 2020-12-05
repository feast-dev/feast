#!/usr/bin/env bash

# This script will scan through a list of files to validate that all versions are consistent with
# - Master version: version set in maven (could be snapshot)
# - Release version: 'dev' on master, Lastest tag on release branches.
# - Stable Version: Highest stable tag. release candidates not included.
# Usage: ./validate-version-consistency.sh 
# Optionaly set TARGET_MERGE_BRANCH var to the target merge branch to lint 
#   versions against the given merge branch.
set -e

source infra/scripts/setup-common-functions.sh

# Fetch tags and current branch
git fetch --prune --unshallow --tags || true
BRANCH_NAME=${TARGET_MERGE_BRANCH-$(git rev-parse --abbrev-ref HEAD)}

# Matches (ie vMAJOR.MINOR-branch) release branch names
RELEASE_BRANCH_REGEX="^v[0-9]+\.[0-9]+-branch$"

# Determine the current Feast version from Maven (pom.xml)
export FEAST_MASTER_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
[[ -z "$FEAST_MASTER_VERSION" ]] && {
  echo "$FEAST_MASTER_VERSION is missing, please check pom.xml and maven"
  exit 1
}
echo "Linting Master Version: $FEAST_MASTER_VERSION"

# Determine Last release tag relative to current branch
if [ $BRANCH_NAME = "master" ]
then
    # Use development version
    FEAST_RELEASE_VERSION="develop"
elif echo "$BRANCH_NAME" | grep -P $RELEASE_BRANCH_REGEX &>/dev/null
then
    # Use last release tag tagged on the release branch
    LAST_MERGED_TAG=$(get_tag_release -m)
    FEAST_RELEASE_VERSION=${LAST_MERGED_TAG#"v"}
else
    # Do not enforce version linting as we don't know if the target merge branch FEAST_RELEASE_VERSION="_ANY"
    FEAST_RELEASE_VERSION="_ANY"
    echo "WARNING: Skipping docker version lint"
fi
[[ -z "$FEAST_RELEASE_VERSION" ]] && {
  echo "FEAST_RELEASE_VERSION is missing"
  exit 1
}
export FEAST_RELEASE_VERSION
echo "Linting Release Version: $FEAST_RELEASE_VERSION"

# Determine highest stable version (no release candidates) relative to current branch.
# Regular expression for matching stable tags in the format vMAJOR.MINOR.PATCH
STABLE_TAG_REGEX="^v[0-9]+\.[0-9]+\.[0-9]+$"
if [ $BRANCH_NAME = "master" ]
then
    # Use last stable tag repo wide
    LAST_STABLE_TAG=$(get_tag_release -s)
    FEAST_STABLE_VERSION=${LAST_STABLE_TAG#"v"}
elif echo "$BRANCH_NAME" | grep -P $RELEASE_BRANCH_REGEX &>/dev/null
then
    # Use last stable tag tagged on the release branch
    LAST_STABLE_MERGE_TAG=$(get_tag_release -sm)
    FEAST_STABLE_VERSION=${LAST_STABLE_MERGE_TAG#"v"}
else
    # Do not enforce version linting as we don't know if the target merge branch
    FEAST_STABLE_VERSION="_ANY"
    echo "WARNING: Skipping stable version lint"
fi
[[ -z "$FEAST_STABLE_VERSION" ]] && {
  echo "FEAST_STABLE_VERSION is missing"
  exit 1
}
export FEAST_STABLE_VERSION
echo "Linting Stable Version: $FEAST_STABLE_VERSION"

# List of files to validate with master version (from pom.xml)
# Structure is a comma separated list of structure
# <File to validate>, <Amount of occurrences of specific version to look for>, <version to look for>

declare -a files_to_validate_version=(
  "infra/charts/feast/Chart.yaml,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/charts/feast-core/Chart.yaml,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/charts/feast-core/values.yaml,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-core/README.md,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/charts/feast-core/README.md,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-serving/Chart.yaml,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/charts/feast-jupyter/values.yaml,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-jupyter/README.md,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-jupyter/README.md,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/charts/feast-jupyter/Chart.yaml,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/charts/feast-serving/values.yaml,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-serving/README.md,1,${FEAST_RELEASE_VERSION}"
  "infra/charts/feast/charts/feast-serving/README.md,1,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/requirements.yaml,3,${FEAST_MASTER_VERSION}"
  "infra/charts/feast/requirements.lock,3,${FEAST_MASTER_VERSION}"
  "infra/docker-compose/.env.sample,1,${FEAST_RELEASE_VERSION}"
  "datatypes/java/README.md,1,${FEAST_MASTER_VERSION}"
  "docs/contributing/development-guide.md,4,${FEAST_MASTER_VERSION}"
  "CHANGELOG.md,2,${FEAST_STABLE_VERSION}"
)

echo
echo "Testing list of files to ensure they have the correct version"
echo


IS_LINT_SUCCESS=true
for i in "${files_to_validate_version[@]}"; do
  IFS=',' read -r FILE_PATH EXPECTED_OCCURRENCES VERSION <<<"${i}"
  # Disable version lint if '_ANY' specified as version.
  if [ "$VERSION" = "_ANY" ]
  then
      continue
  fi
  
  echo "========================================================="
  echo "Testing whether versions are correctly set within file: $FILE_PATH"
  ACTUAL_OCCURRENCES=$(grep -c -P "\bv?$VERSION\b" "$FILE_PATH" || true)

  if [ "${ACTUAL_OCCURRENCES}" -ge "${EXPECTED_OCCURRENCES}" ]; then
    echo "OK: Expecting $EXPECTED_OCCURRENCES occurrences of '$VERSION' in $FILE_PATH, and found $ACTUAL_OCCURRENCES"
  else
    echo "FAIL: Expecting $EXPECTED_OCCURRENCES occurrences of '$VERSION' in $FILE_PATH, but found $ACTUAL_OCCURRENCES"
    IS_LINT_SUCCESS=false
  fi
done

if $IS_LINT_SUCCESS; then exit 0; else exit 1; fi
