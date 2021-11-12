#!/usr/bin/env bash

# Usage: TAG=$(get_tag_release [-ms])
# Parses the last release from git tags.
# Options:
# -m - Use only tags that are tagged on the current branch
# -s - Use only stable version tags. (ie no prerelease tags).
get_tag_release() {
  local GIT_TAG_CMD="git tag -l"
  # Match only Semver tags
  # Regular expression should match MAJOR.MINOR.PATCH[-PRERELEASE[.IDENTIFIER]]
  # eg. v0.7.1 v0.7.2-alpha v0.7.2-rc.1
  local TAG_REGEX='^v[0-9]+\.[0-9]+\.[0-9]+(-([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?$'
  local OPTIND opt
  while getopts "ms" opt; do
    case "${opt}" in
      m)
        GIT_TAG_CMD="$GIT_TAG_CMD --merged"
        ;;
      s)
        # Match only stable version tags.
        TAG_REGEX="^v[0-9]+\.[0-9]+\.[0-9]+$"
        ;;
      *)
        echo "get_tag_release(): Error: Bad arguments: $@"
        return 1
        ;;
    esac
  done
  shift $((OPTIND-1))

  # Retrieve tags from git and filter as per regex.
  local FILTERED_TAGS=$(bash -c "$GIT_TAG_CMD" | grep -P "$TAG_REGEX")

  # Sort version tags in highest semver version first.
  # To make sure that prerelease versions (ie versions vMAJOR.MINOR.PATCH-PRERELEASE suffix)
  # are sorted after stable versions (ie vMAJOR.MINOR.PATCH), we append '_' after 
  # eachustable version as '_' is after '-' found in prerelease version
  # alphanumerically and remove after sorting.
  local SEMVER_SORTED_TAGS=$(echo "$FILTERED_TAGS" | sed -e '/-/!{s/$/_/}' | sort -rV \
    | sed -e 's/_$//')
  echo $(echo "$SEMVER_SORTED_TAGS" | head -n 1)
}
