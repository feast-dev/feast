# Development Guide: Main Feast Repository

## Table of Contents

- [Development Guide: Main Feast Repository](#development-guide-main-feast-repository)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Compatibility](#compatibility)
  - [Community](#community)
  - [Making a pull request](#making-a-pull-request)
    - [Pull request checklist](#pull-request-checklist)
    - [Good practices to keep in mind](#good-practices-to-keep-in-mind)
    - [Forking the repo](#forking-the-repo)
    - [Pre-commit Hooks](#pre-commit-hooks)
    - [Signing off commits](#signing-off-commits)
    - [Incorporating upstream changes from master](#incorporating-upstream-changes-from-master)
  - [Feast Python SDK and CLI](#feast-python-sdk-and-cli)
    - [Environment Setup](#environment-setup)
    - [Code Style and Linting](#code-style-and-linting)
    - [Unit Tests](#unit-tests)
    - [Integration Tests](#integration-tests)
      - [Local integration tests](#local-integration-tests)
      - [(Advanced) Full integration tests](#advanced-full-integration-tests)
      - [(Advanced) Running specific provider tests or running your test against specific online or offline stores](#advanced-running-specific-provider-tests-or-running-your-test-against-specific-online-or-offline-stores)
      - [(Experimental) Run full integration tests against containerized services](#experimental-run-full-integration-tests-against-containerized-services)
    - [Contrib integration tests](#contrib-integration-tests)
      - [(Contrib) Running tests for Spark offline store](#contrib-running-tests-for-spark-offline-store)
      - [(Contrib) Running tests for Trino offline store](#contrib-running-tests-for-trino-offline-store)
      - [(Contrib) Running tests for Postgres offline store](#contrib-running-tests-for-postgres-offline-store)
      - [(Contrib) Running tests for Postgres online store](#contrib-running-tests-for-postgres-online-store)
      - [(Contrib) Running tests for HBase online store](#contrib-running-tests-for-hbase-online-store)
  - [(Experimental) Feast UI](#experimental-feast-ui)
  - [Feast Java Serving](#feast-java-serving)
  - [Developing the Feast Helm charts](#developing-the-feast-helm-charts)
    - [Feast Java Feature Server Helm Chart](#feast-java-feature-server-helm-chart)
    - [Feast Python Feature Server Helm Chart](#feast-python-feature-server-helm-chart)
    - [Testing with Github Actions workflows](#testing-with-github-actions-workflows)
  - [Feast Data Storage Format](#feast-data-storage-format)
## Overview
This guide is targeted at developers looking to contribute to Feast components in
the main Feast repository:
- [Feast Python SDK and CLI](#feast-python-sdk-and-cli)
- [Feast Java Serving](#feast-java-serving)

Please see [this page](../reference/codebase-structure.md) for more details on the structure of the entire codebase.

## Compatibility

The compatibility policy for Feast can be found [here](compatibility.md), and should be followed for all changes proposed, by maintainers or contributors.

## Community
See [Contribution process](./contributing.md) and [Community](../community.md) for details on how to get more involved in the community.

## Making a pull request
We use the convention that the assignee of a PR is the person with the next action.

If the assignee is empty it means that no reviewer has been found yet.
If a reviewer has been found, they should also be the assigned the PR.
Finally, if there are comments to be addressed, the PR author should be the one assigned the PR.

PRs that are submitted by the general public need to be identified as `ok-to-test`. Once enabled, [Prow](https://github.com/kubernetes/test-infra/tree/master/prow) will run a range of tests to verify the submission, after which community members will help to review the pull request.

### Pull request checklist
A quick list of things to keep in mind as you're making changes:
- As you make changes
  - Make your changes in a [forked repo](#forking-the-repo) (instead of making a branch on the main Feast repo)
  - [Sign your commits](#signing-off-commits) as you go (to avoid DCO checks failing)
  - [Rebase from master](#incorporating-upstream-changes-from-master) instead of using `git pull` on your PR branch
  - Install [pre-commit hooks](#pre-commit-hooks) to ensure all the default linters / formatters are run when you push.
- When you make the PR
  - Make a pull request from the forked repo you made
  - Ensure the title of the PR matches semantic release conventions (e.g. start with `feat:` or `fix:` or `ci:` or `chore:` or `docs:`). Keep in mind that any PR with `feat:` or `fix:` will directly make it into the change log of a release, so make sure they are understandable!
  - Ensure you add a GitHub **label** (i.e. a kind tag to the PR (e.g. `kind/bug` or `kind/housekeeping`)) or else checks will fail.
  - Ensure you leave a release note for any user facing changes in the PR. There is a field automatically generated in the PR request. You can write `NONE` in that field if there are no user facing changes.
  - Please run tests locally before submitting a PR (e.g. for Python, the [local integration tests](#local-integration-tests))
  - Try to keep PRs smaller. This makes them easier to review.

### Good practices to keep in mind
* Fill in the description based on the default template configured when you first open the PR
  * What this PR does/why we need it
  * Which issue\(s\) this PR fixes
  * Does this PR introduce a user-facing change
* Add `WIP:` to PR name if more work needs to be done prior to review


### Forking the repo
Fork the Feast Github repo and clone your fork locally. Then make changes to a local branch to the fork.

See [Creating a pull request from a fork](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork)

### Pre-commit Hooks
Setup [`pre-commit`](https://pre-commit.com/) to automatically lint and format the codebase on commit:
1. Ensure that you have Python (3.7 and above) with `pip`, installed.
2. Install `pre-commit` with `pip` &amp; install pre-push hooks
```sh
pip install pre-commit
pre-commit install --hook-type pre-commit --hook-type pre-push
```
3. On push, the pre-commit hook will run. This runs `make format` and `make lint`.

### Signing off commits
> :warning: Warning: using the default integrations with IDEs like VSCode or IntelliJ will not sign commits.
> When you submit a PR, you'll have to re-sign commits to pass the DCO check.

Use git signoffs to sign your commits. See
https://docs.github.com/en/github/authenticating-to-github/managing-commit-signature-verification for details

Then, you can sign off commits with the `-s` flag:
```
git commit -s -m "My first commit"
```

GPG-signing commits with `-S` is optional.

### Incorporating upstream changes from master
Our preference is the use of `git rebase [master]` instead of `git merge` : `git pull -r`.

Note that this means if you are midway through working through a PR and rebase, you'll have to force push:
`git push --force-with-lease origin [branch name]`

## Feast Python SDK and CLI
### Environment Setup
#### Tools
- Docker:  Docker is used to provision service dependencies during testing, and build images for feature servers and other components.
   - Please note that we use [Docker with BuiltKit](https://docs.docker.com/develop/develop-images/build_enhancements/).
   - _Alternatively_ - To use [podman](https://podman.io/) on a Fedora or RHEL machine, follow this [guide](https://github.com/feast-dev/feast/issues/4190)
- `make` is used to run various scripts
- [uv](https://docs.astral.sh/) for managing python dependencies. [installation instructions](https://docs.astral.sh/uv/getting-started/installation/)
- (M1 Mac only): Follow the [dev guide if you have issues](https://github.com/feast-dev/feast/issues/2105)
- (Optional): Node & Yarn (needed for building the feast UI)
- (Optional): [Pixi](https://pixi.sh/latest/) for recompile python lock files. Only when you make changes to requirements or simply want to update python lock files to reflect latest versioons.

### Quick start
- create a new virtual env: `uv venv --python 3.11` (Replace the python version with your desired version)
- activate the venv: `source venv/bin/activate`
- Install dependencies `make install-python-dependencies-dev`

### Building the UI
To build the UI from the latest released NPM package (hosted under @feast-dev/feast-ui):

```sh
make build-ui
```

If you want to test backend and frontend together using 'feast ui' command and with a locally built Feast UI package, you can build using:

```sh
make build-ui-local
```
Use this when you are making changes to the React UI code and want to see them live via the backend.

### Recompiling python lock files
Recompile python lock files. This only needs to be run when you make changes to requirements or simply want to update python lock files to reflect latest versions.

```sh
make lock-python-dependencies-all
```

### Building protos
```sh
make compile-protos-python
```

### Building a docker image for development
```sh
docker build -t docker-whale -f ./sdk/python/feast/infra/feature_servers/multicloud/Dockerfile .
```

### Code Style and Linting
Feast Python SDK and CLI codebase:
- Conforms to [Black code style](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html)
- Has type annotations as enforced by `mypy`
- Has imports sorted by `ruff` (see [isort (I) rules](https://docs.astral.sh/ruff/rules/#isort-i))
- Is lintable by `ruff`

To ensure your Python code conforms to Feast Python code standards:
- Autoformat your code to conform to the code style:
```sh
make format-python
```

- Lint your Python code before submitting it for review:
```sh
make lint-python
```

> Setup [pre-commit hooks](#pre-commit-hooks) to automatically format and lint on commit.

### Unit Tests
Unit tests (`pytest`) for the Feast Python SDK and CLI can run as follows:
```sh
make test-python-unit
```

> :warning: Local configuration can interfere with Unit tests and cause them to fail:
> - Ensure [no AWS configuration is present](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)
    > and [no AWS credentials can be accessed](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials) by `boto3`
> - Ensure Feast Python SDK and CLI is not configured with configuration overrides (ie `~/.feast/config` should be empty).

### Integration Tests
There are two sets of tests you can run:
1. Local integration tests (for faster development, tests file offline store & key online stores)
2. Full integration tests (requires cloud environment setups)

#### Local integration tests
For this approach of running tests, you'll need to have docker set up locally: [Get Docker](https://docs.docker.com/get-docker/)

It leverages a file based offline store to test against emulated versions of Datastore, DynamoDB, and Redis, using ephemeral containers.

These tests create new temporary tables / datasets locally only, and they are cleaned up. when the containers are torn down.

```sh
make test-python-integration-local
```

#### (Advanced) Full integration tests
To test across clouds, on top of setting up Redis, you also need GCP / AWS / Snowflake setup.

> Note: you can manually control what tests are run today by inspecting
> [RepoConfiguration](https://github.com/feast-dev/feast/blob/master/sdk/python/tests/integration/feature_repos/repo_configuration.py)
> and commenting out tests that are added to `DEFAULT_FULL_REPO_CONFIGS`

**GCP**
1. You can get free credits [here](https://cloud.google.com/free/docs/free-cloud-features#free-trial).
2. You will need to setup a service account, enable the BigQuery API, and create a staging location for a bucket.

* Setup your service account and project using steps 1-5 [here](https://codelabs.developers.google.com/codelabs/cloud-bigquery-python#0).
  * Remember to save your `PROJECT_ID` and your `key.json`. These will be your secrets that you will need to configure in Github actions. Namely, `secrets.GCP_PROJECT_ID` and `secrets.GCP_SA_KEY`. The `GCP_SA_KEY` value is the contents of your `key.json` file.
* Follow these [instructions](https://cloud.google.com/storage/docs/creating-buckets) in your project to create a bucket for running GCP tests and remember to save the bucket name.
  * Make sure to add the service account email that you created in the previous step to the users that can access your bucket. Then, make sure to give the account the correct access roles, namely `objectCreator`, `objectViewer`, `objectAdmin`, and `admin`, so that your tests can use the bucket.

3. Install the [Cloud SDK](https://cloud.google.com/sdk/docs/install).
4. Login to gcloud if you haven't already:
  ```
  gcloud auth login
  gcloud auth application-default login
  ```
- When you run `gcloud auth application-default login`, you should see some output of the form:
  ```
  Credentials saved to file: [$HOME/.config/gcloud/application_default_credentials.json]
  ```
- You should run `export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/gcloud/application_default_credentials.json”` to add the application credentials to your .zshrc or .bashrc.
5. Run `export GCLOUD_PROJECT=[your project id from step 2]` to your .zshrc or .bashrc.
6. Running `gcloud config list` should give you something like this:
  ```sh
  $ gcloud config list
  [core]
  account = [your email]
  disable_usage_reporting = True
  project = [your project id]

  Your active configuration is: [default]
  ```
7. Export GCP specific environment variables in your workflow. Namely,
  ```sh
  export GCS_REGION='[your gcs region e.g US]'
  export GCS_STAGING_LOCATION='[your gcs staging location]'
  ```
**NOTE**: Your `GCS_STAGING_LOCATION` should be in the form `gs://<bucket name>` where the bucket name is from step 2.

8. Once authenticated, you should be able to run the integration tests for BigQuery without any failures.

**AWS**
1. Setup AWS by creating an account, database, and cluster. You will need to enable Redshift and Dynamo.
* You can get free credits [here](https://aws.amazon.com/free/?all-free-tier.sort-by=item.additionalFields.SortRank&al[…]f.Free%20Tier%20Types=*all&awsf.Free%20Tier%20Categories=*all).
2. To run the AWS Redshift and Dynamo integration tests you will have to export your own AWS credentials. Namely,

```sh
export AWS_REGION='[your aws region]'
export AWS_CLUSTER_ID='[your aws cluster id]'
export AWS_USER='[your aws user]'
export AWS_DB='[your aws database]'
export AWS_STAGING_LOCATION='[your s3 staging location uri]'
export AWS_IAM_ROLE='[redshift and s3 access role]'
export AWS_LAMBDA_ROLE='[your aws lambda execution role]'
export AWS_REGISTRY_PATH='[your aws registry path]'
```

**Snowflake**
1. See https://signup.snowflake.com/ to setup a trial.
2. Setup your account and if you are not an `ACCOUNTADMIN` (if you created your own account, you should be), give yourself the `SYSADMIN` role.
  ```sql
  grant role accountadmin, sysadmin to user user2;
  ```
* Also remember to save your [account name](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#:~:text=organization_name%20is%20the%20name%20of,your%20account%20within%20your%20organization), username, and role.
* Your account name can be found under
3. Create Dashboard and add a Tile.
4. Create a warehouse and database named `FEAST` with the schemas `OFFLINE` and `ONLINE`.
  ```sql
  create or replace warehouse feast_tests_wh with
  warehouse_size='MEDIUM' --set your warehouse size to whatever your budget allows--
  auto_suspend = 180
  auto_resume = true
  initially_suspended=true;

  create or replace database FEAST;
  use database FEAST;
  create schema OFFLINE;
  create schema ONLINE;
  ```
5. You will need to create a data unloading location(either on S3, GCP, or Azure). Detailed instructions [here](https://docs.snowflake.com/en/user-guide/data-unload-overview.html). You will need to save the storage export location and the storage export name. You will need to create a [storage integration ](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html) in your warehouse to make this work. Name this storage integration `FEAST_S3`.
6. Then to run successfully, you'll need some environment variables setup:
  ```sh
  export SNOWFLAKE_CI_DEPLOYMENT='[your snowflake account name]'
  export SNOWFLAKE_CI_USER='[your snowflake username]'
  export SNOWFLAKE_CI_PASSWORD='[your snowflake pw]'
  export SNOWFLAKE_CI_ROLE='[your CI role e.g. SYSADMIN]'
  export SNOWFLAKE_CI_WAREHOUSE='[your warehouse]'
  export BLOB_EXPORT_STORAGE_NAME='[your data unloading storage name]'
  export BLOB_EXPORT_URI='[your data unloading blob uri]`
  ```
7. Once everything is setup, running snowflake integration tests should pass without failures.

Note that for Snowflake / GCP / AWS, running `make test-python-integration`  will create new temporary tables / datasets in your cloud storage tables.

#### (Advanced) Running specific provider tests or running your test against specific online or offline stores

1. If you don't need to have your test run against all of the providers(`gcp`, `aws`, and `snowflake`) or don't need to run against all of the online stores, you can tag your test with specific providers or stores that you need(`@pytest.mark.universal_online_stores` or `@pytest.mark.universal_online_stores` with the `only` parameter). The `only` parameter selects specific offline providers and online stores that your test will test against. Example:

```python
# Only parametrizes this test with the sqlite online store
@pytest.mark.universal_online_stores(only=["sqlite"])
def test_feature_get_online_features_types_match():
```

2. You can also filter tests to run by using pytest's cli filtering. Instead of using the make commands to test Feast, you can filter tests by name with the `-k` parameter. The parametrized integration tests are all uniquely identified by their provider and online store so the `-k` option can select only the tests that you need to run. For example, to run only Redshift related tests, you can use the following command:

```sh
python -m pytest -n 8 --integration -k Redshift sdk/python/tests
```

#### (Experimental) Run full integration tests against containerized services
Test across clouds requires existing accounts on GCP / AWS / Snowflake, and may incur costs when using these services.

For this approach of running tests, you'll need to have docker set up locally: [Get Docker](https://docs.docker.com/get-docker/)

It's possible to run some integration tests against emulated local versions of these services, using ephemeral containers.
These tests create new temporary tables / datasets locally only, and they are cleaned up. when the containers are torn down.

The services with containerized replacements currently implemented are:
- Datastore
- DynamoDB
- Redis
- Trino
- HBase
- Postgres
- Cassandra

You can run `make test-python-integration-container` to run tests against the containerized versions of dependencies.

### Contrib integration tests
#### (Contrib) Running tests for Spark offline store
You can run `make test-python-universal-spark` to run all tests against the Spark offline store. (Note: you'll have to run `pip install -e ".[dev]"` first).

Not all tests are passing yet

#### (Contrib) Running tests for Trino offline store
You can run `make test-python-universal-trino` to run all tests against the Trino offline store. (Note: you'll have to run `pip install -e ".[dev]"` first)

#### (Contrib) Running tests for Postgres offline store
You can run `test-python-universal-postgres-offline` to run all tests against the Postgres offline store. (Note: you'll have to run `pip install -e ".[dev]"` first)

#### (Contrib) Running tests for Postgres online store
You can run `test-python-universal-postgres-online` to run all tests against the Postgres offline store. (Note: you'll have to run `pip install -e ".[dev]"` first)

#### (Contrib) Running tests for HBase online store
TODO

## (Experimental) Feast UI
See [Feast contributing guide](https://github.com/feast-dev/feast/blob/master/ui/CONTRIBUTING.md)

## Feast Java Serving
See [Java contributing guide](https://github.com/feast-dev/feast/blob/master/java/CONTRIBUTING.md)

See also development instructions related to the helm chart below at [Developing the Feast Helm charts](#developing-the-feast-helm-charts)

## Developing the Feast Helm charts
There are 2 helm charts:
- Feast Java feature server
- Feast Python feature server

Generally, you can override the images in the helm charts with locally built Docker images, and install the local helm
chart.

All README's for helm charts are generated using [helm-docs](https://github.com/norwoodj/helm-docs). You can install it
(e.g. with `brew install norwoodj/tap/helm-docs`) and then run `make build-helm-docs`.

### Feast Java Feature Server Helm Chart
See the Java demo example (it has development instructions too using minikube) [here](https://github.com/feast-dev/feast/blob/master/examples/java-demo/README.md)

It will:
- run `make build-java-docker-dev` to build local Java feature server binaries
- configure the included `application-override.yaml` to override the image tag to use the locally built dev images.
- install the local chart with `helm install feast-release ../../../infra/charts/feast --values application-override.yaml`

### Feast Python Feature Server Helm Chart
See the Python demo example (it has development instructions too using minikube) [here](examples/python-helm-demo/README.md)

It will:
- run `make build-feature-server-dev` to build a local python feature server binary
- install the local chart with `helm install feast-release ../../../infra/charts/feast-feature-server --set image.tag=dev --set feature_store_yaml_base64=$(base64 feature_store.yaml)`

### Testing with Github Actions workflows

Please refer to the maintainers [doc](maintainers.md) if you would like to locally test out the github actions workflow changes.
This document will help you setup your fork to test the ci integration tests and other workflows without needing to make a pull request against feast-dev master.

## Feast Data Storage Format

Feast data storage contracts are documented in the following locations:

* [Feast Offline Storage Format](https://github.com/feast-dev/feast/blob/master/docs/specs/offline_store_format.md): Used by BigQuery, Snowflake \(Future\), Redshift \(Future\).
* [Feast Online Storage Format](https://github.com/feast-dev/feast/blob/master/docs/specs/online_store_format.md): Used by Redis, Google Datastore.
