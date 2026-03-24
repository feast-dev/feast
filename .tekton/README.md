# Tekton Pipelines (Pipelines-as-Code)

This directory contains Tekton `PipelineRun` definitions used for building and testing the Open Data Hub (ODH) Feast components on Konflux ITS build Openshift environment. Pipelines are triggered by [Pipelines-as-Code](https://pipelinesascode.com/) based on repository events and comments.

For Tekton concepts (Tasks, Pipelines, PipelineRuns), see the [Tekton Getting Started](http://tekton.dev/docs/getting-started/) documentation.

## Pipelines

### `feast-group-test.yaml` — Feast group integration test

Runs integration tests across **multiple Feast components** in one pipeline. Use this when you need to validate that changes work correctly with both the Feast operator and the feature server (e.g. API or image changes that affect both).

| Item | Description |
|------|-------------|
| **Name** | `feast-group-test` |
| **Type** | Test (`pipelines.appstudio.openshift.io/type: test`) |
| **Application** | `group-testing` / component: `feast-group` |

**When it runs**

- **Event:** `group-test` (e.g. from Konflux/App Studio group-test workflow).
- **Comment:** Post `/group-test` on a pull request to trigger manually.

**What it tests**

- **feast-operator**
- **feature-server**

**How images are built from the PR source**

The group test uses container images built from **your PR’s source**. That happens in two steps:

1. **Per-component PR pipelines build the images**  
   When you open or update a PR to the Feast repo (target branch `stable`), these pipelines run and build images from the PR’s commit:
   - **odh-feast-operator-pull-request** — clones the repo at `{{source_url}}` / `{{revision}}`, builds from `infra/feast-operator`, and pushes e.g. `quay.io/opendatahub/feast-operator:odh-pr-<revision>`.
   - **odh-feature-server-pull-request** — same repo/revision, builds from `sdk/python/feast/infra/feature_servers/multicloud`, and pushes e.g. `quay.io/opendatahub/feature-server:odh-pr-<revision>`.

2. **Group test uses a snapshot of those images**  
   When you trigger the group test (e.g. by commenting `/group-test`), the pipeline runs the **generate-snapshot** task. That task finds the image refs (and git commit/URL) for each group component for the current PR and outputs a **snapshot** JSON. The **deploy-and-test** step then deploys the Feast operator and feature server using those snapshot images and runs tests after cloning the repo at the snapshot’s git commit.

So the code under test is always the PR’s commit: the same commit that was used to build the images and that is checked out to run the e2e and REST API tests.

**Pipeline definition**

The group test runs the pipeline defined in [odh-konflux-central](https://github.com/opendatahub-io/odh-konflux-central):

- **[PR group testing pipeline](https://github.com/opendatahub-io/odh-konflux-central/blob/main/integration-tests/feast/pr-group-testing-pipeline.yaml)** — `integration-tests/feast/pr-group-testing-pipeline.yaml`

The deploy-and-e2e step runs in an image built from:

- **[Dockerfile.go-its](https://github.com/opendatahub-io/odh-konflux-central/blob/main/integration-tests/feast/Dockerfile.go-its)** — defines the test runner image (e.g. `quay.io/rhoai/rhoai-task-toolset:go-its`) with Go, Python, `oc`/kubectl, `uv`, and other tools needed for e2e and REST API tests.

**Tests that run**

The pipeline provisions an ephemeral Hypershift cluster, deploys the Feast operator and feature server from the group snapshot, then runs these tests (from the Feast repo at the snapshot revision):

| Test | Location / command | Description |
|------|--------------------|-------------|
| **Feature Store Operator E2E** | `infra/feast-operator` → `make test-e2e` | End-to-end tests for the Feast operator and feature server after `make install` and `make deploy` with snapshot images. |
| **Registry REST API** | `sdk/python/tests/integration/rest_api/test_registry_rest_api.py` | Integration tests for the Registry REST API (`uv run pytest ... --integration -s --timeout=600`). |
| **Previous-version compatibility** | `infra/feast-operator` → `make test-previous-version` | Ensures compatibility with the previous operator version (run after undeploy). |
| **Upgrade test** | `infra/feast-operator` → `make test-upgrade` | Tests upgrading the operator to the snapshot version. |

After the main test step, the pipeline runs must-gather (Feast + OpenShift), pushes artifacts to the CI artifacts repo and OCI, and posts a comment on the PR with status and links.

**Parameters**

- `group-components`: JSON map of component names to repo identifiers (see `group-components` in the file).

**Template variables** (set by Pipelines-as-Code)

- `revision` — Git revision (e.g. branch or SHA).
- `target_branch` — Target branch of the PR.
- `pull_request_number` — PR number.
- `git_auth_secret` — Name of the secret used for Git authentication.
