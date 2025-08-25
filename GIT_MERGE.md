 # Steps to Sync the Fork from the Upstream Repository (feast-dev/feast)

This guide provides step-by-step instructions to sync your fork with the upstream repository changes.

## Step 1: Create a New Branch
```sh
    git checkout -b GIT_BRANCH_NAME
```
## Step 2: Add the Upstream Remote Repository
```sh
    git remote add upstream https://github.com/feast-dev/feast.git
    git fetch upstream
```
## Step 3: Find the Common Ancestor
```sh
    git merge-base GIT_BRANCH_NAME upstream/master
```
## Step 4: Merge to a Specific Commit
You don't need to merge all pending commits. You can pick a commit and repeat this process until you complete all the pending commits. 
```sh
    git merge GIT_COMMIT_HASH
```
## Step 5: Resolve the Merge Conflicts
IntelliJ IDE has a good Merge conflict resolution tool.
## Step 6: Regenerate requirements file
```sh
    make lock-python-dependencies-all   # Unable to run pixi commands on Mac
    or
    pip install uv
    make lock-python-dependencies-uv-all
```
## Step 7: Create a virtual environment and Run the tests locally. Resolve the issues identified.
```sh
    python -m  venv .venv
    source .venv/bin/activate
    make install-python-dependencies-dev
    make test-python-unit
    make test-python-universal # Snowflake tests may fail. Rest all should pass. 
```
## Step 8: Push the Changes to Your Fork and create Pull Request for Review
```sh
    git push origin GIT_BRANCH_NAME
```
## Step 9: Review and Merge your changes (not Squash Merge)
