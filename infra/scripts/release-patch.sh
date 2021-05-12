#!/usr/bin/env bash
set -eo pipefail


usage()
{
    echo "usage: release-patch.sh

    This script is used to release a patch release. It is untested on major/minor releases and for those, some modification may be necessary.

    -v, --version           version to release, example: 0.10.6
    -t, --github-token      personal GitHub token
    -c, --starting-commit   git commit hash; the script will cherry-pick that commit and all on master after it
"
}

while [ "$1" != "" ]; do
  case "$1" in
      -v | --version )           VERSION="$2";            shift;;
      -t | --github-token )      GH_TOKEN="$2";           shift;;
      -c | --starting-commit )   STARTING_COMMIT="$2";    shift;;
      -h | --help )              usage;                   exit;; 
      * )                        usage;                   exit 1
  esac
  shift
done

if [ -z $VERSION ]; then usage; exit 1; fi
if [ -z $GH_TOKEN ]; then usage; exit 1; fi
regex="([0-9]+)\.([0-9]+)\.([0-9]+)"
if [[ $VERSION =~ $regex ]]
then
    MAJOR="${BASH_REMATCH[1]}"
    MINOR="${BASH_REMATCH[2]}"
    PATCH="${BASH_REMATCH[3]}"
else
    usage
    exit 1
fi

echo "This script is mostly idempotent; check git status for temp files before restarting. It will always prompt you before making any non-local change."

# Go to infra/scripts directory
cd $(dirname "$0")
# Login to GitHub CLI
echo $GH_TOKEN | gh auth login --with-token

echo "Step 1: Cherry-pick new commits onto release branch"
git fetch origin
git checkout master
last_commit=$(git rev-parse HEAD)
git checkout v$MAJOR.$MINOR-branch
cherrypick_from_master()
{
    echo "Cherrypicking commits"
    git cherry-pick $STARTING_COMMIT^..$last_commit
    echo "Commits cherrypicked"
}
if [ -z $STARTING_COMMIT ]; then
    read -p "No starting commit given. Skip this step (y) or exit (n)? " choice
    case "$choice" in
        y|Y ) echo "Skipping this step" ;;
        n|N ) exit ;;
        * ) exit ;;
    esac ;
else
    read -p "Will cherry-pick starting from commit $STARTING_COMMIT. Continue (y) or skip this step (n)? " choice
    case "$choice" in
        y|Y ) cherrypick_from_master ;;
        n|N ) echo "Skipping this step" ;;
        * ) echo "Skipping this step" ;;
    esac ;
fi

commit_changelog()
{
    echo "Committing CHANGELOG.md"
    git add ../../CHANGELOG.md
    git commit -m "Update CHANGELOG for Feast v$MAJOR.$MINOR.$PATCH"
}
update_changelog()
{
    echo "Running changelog generator (will take up to a few minutes)"
    echo -e "# Changelog\n" > temp \
    && docker run -it --rm ferrarimarco/github-changelog-generator \
    --user feast-dev \
    --project feast  \
    --release-branch master  \
    --future-release v$MAJOR.$MINOR.$PATCH  \
    --unreleased-only  \
    --no-issues  \
    --bug-labels kind/bug  \
    --enhancement-labels kind/feature  \
    --breaking-labels compat/breaking  \
    -t $GH_TOKEN \
    --max-issues 1 -o \
    | sed -n '/## \[v'"$MAJOR.$MINOR.$PATCH"'\]/,$p' \
    | sed '$d' | sed '$d' | sed '$d' | tr -d '\r' >> temp \
    && sed '1d' ../../CHANGELOG.md >> temp && mv temp ../../CHANGELOG.md
    git diff ../../CHANGELOG.md
    echo "Check CHANGELOG.md carefully and fix any errors. In particular, make sure the new enhancements/PRs/bugfixes aren't already listed somewhere lower down in the file."
    read -p "Once you're done checking, continue to commit the changelog (y) or exit (n)? " choice
    case "$choice" in
        y|Y ) commit_changelog ;;
        n|N ) exit ;;
        * ) exit ;;
    esac ;
}
echo "Step 2: Updating CHANGELOG.md"
if grep -q "https://github.com/feast-dev/feast/tree/v$MAJOR.$MINOR.$PATCH" ../../CHANGELOG.md ; then
    read -p "CHANGELOG.md appears updated. Skip this step (y/n)? " choice
    case "$choice" in
        y|Y ) echo "Skipping this step" ;;
        n|N ) update_changelog ;;
        * ) update_changelog ;;
    esac ;
else
    update_changelog ;
fi

tag_commit()
{
    echo "Tagging commit"
    git tag v$MAJOR.$MINOR.$PATCH
    echo "Commit tagged"
}
echo "Step 3: Tag commit"
if git tag | grep -q "v$MAJOR.$MINOR.$PATCH" ; then
    read -p "The tag already exists. Skip this step (y/n)? " choice
    case "$choice" in
        y|Y ) echo "Skipping this step" ;;
        n|N ) tag_commit ;;
        * ) tag_commit ;;
    esac ;
else
    tag_commit ;
fi

echo "Step 4: Push commits and tags"
push_commits()
{
    echo "Pushing commits"
    git push origin v$MAJOR.$MINOR-branch
    echo "Commits pushed"
}
echo "Step 4a: Push commits"
if git status | grep -q "nothing to commit" ; then
    echo "The commits appear pushed. Skipping this sub-step"
else
    read -p "Commits are not pushed. Continue (y) or skip this sub-step (n)? " choice
    case "$choice" in
        y|Y ) push_commits ;;
        n|N ) echo "Skipping this sub-step" ;;
        * ) echo "Skipping this sub-step" ;;
    esac ;
fi

push_tag()
{
    echo "Pushing tag"
    git push origin v$MAJOR.$MINOR.$PATCH
    echo "Tag pushed"
}
echo "Step 4b: Push tag"
if git ls-remote --tags origin | grep -q "v$MAJOR.$MINOR.$PATCH" ; then
    read -p "The tag appears pushed. Skip this sub-step (y/n)? " choice
    case "$choice" in
        y|Y ) echo "Skipping this sub-step" ;;
        n|N ) push_tag ;;
        * ) push_tag ;;
    esac ;
else
    read -p "The tag is not pushed. Continue (y) or skip this sub-step (n)? " choice
    case "$choice" in
        y|Y ) push_tag ;;
        n|N ) echo "Skipping this sub-step" ;;
        * ) echo "Skipping this sub-step" ;;
    esac ;
fi

read -p "Now wait for the CI to pass. Continue (y) or exit and fix the problem (n)? " choice
case "$choice" in
    y|Y ) "Moving on to the next step" ;;
    n|N ) exit ;;
    * ) exit ;;
esac ;

echo "Step 6: Add changelog to master"
changelog_hash=$(git rev-parse HEAD)
git checkout master
cp_changelog()
{
    echo "Cherry-picking"
    git cherry-pick $changelog_hash
    echo "Cherry-pick done"
}
echo "Step 6a: Cherry-pick changelog to master"
if grep -q "https://github.com/feast-dev/feast/tree/v$MAJOR.$MINOR.$PATCH" ../../CHANGELOG.md ; then
    read -p "The changelog appears to be cherry-picked onto master. Skip this sub-step (y/n)? " choice
    case "$choice" in
        y|Y ) echo "Skipping this sub-step" ;;
        n|N ) cp_changelog ;;
        * ) cp_changelog ;;
    esac ;
else
    read -p "The changelog does not appear to be cherry-picked onto master. Continue (y) or skip this sub-step (n)? " choice
    case "$choice" in
        y|Y ) cp_changelog ;;
        n|N ) echo "Skipping this sub-step" ;;
        * ) echo "Skipping this sub-step" ;;
    esac ;
fi

push_cp()
{
    echo "Pushing cherry-pick"
    git push origin master
    echo "Commit pushed"
}
echo "Step 6b: Push changelog to master"
if git status | grep -q "nothing to commit" ; then
    echo "The commit appears pushed. Skipping this sub-step"
else
    read -p "The commit is not pushed. Continue (y) or skip this sub-step (n)? " choice
    case "$choice" in
        y|Y ) push_cp ;;
        n|N ) echo "Skipping this sub-step" ;;
        * ) echo "Skipping this sub-step" ;;
    esac ;
fi

create_release()
{
    echo "Creating GitHub release"
    cat ../../CHANGELOG.md | sed -n '/## \[v0.10.4\]/,/## \[v0.10.3\]/p' | sed -n '/**Implemented enhancements/,$p' | sed '$d' > temp2 \
    && gh release create v$MAJOR.$MINOR.$PATCH -t "Feast v$MAJOR.$MINOR.$PATCH" --repo feast-dev/feast --notes-file temp2 \
    && rm temp2
    echo "GitHub release created"
}
echo "Step 7: Create a GitHub release"
if gh release list --repo feast-dev/feast | grep -q "v$MAJOR.$MINOR.$PATCH" ; then
    read -p "GitHub release appears created. Skip this step (y/n)? " choice
    case "$choice" in
        y|Y ) echo "Skipping this step" ;;
        n|N ) create_release ;;
        * ) create_release ;;
    esac ;
else
    read -p "A GitHub release has not been created. Continue (y) or skip this step (n)? " choice
    case "$choice" in
        y|Y ) create_release ;;
        n|N ) echo "Skipping this step" ;;
        * ) echo "Skipping this step" ;;
    esac ;
fi

echo "Do these final two steps on your own:"
echo "Step 8: Update the Upgrade Guide"
echo "Step 9: Update Feast Supported Versions"
