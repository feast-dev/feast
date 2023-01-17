# Setting up your environment
> Please see the [Development Guide](https://docs.feast.dev/project/development-guide) for project level development instructions and [Contributing Guide](https://github.com/feast-dev/feast/blob/master/CONTRIBUTING.md) for specific details on how to set up your develop environment and contribute to Feast.

# Maintainers Development
> In most scenarios, your code changes or the areas of Feast that you are actively maintaining will only touch parts of the code(e.g one offline store/online store).

## Forked Repo Best Practices
1.  You should setup your fork so that you can make pull requests against your own master branch.
    - This prevents unnecessary integration tests and other github actions that are irrelevant to your code changes from being run everytime you would like to make a code change.
    - **NOTE**: Most workflows are enabled by default so manually [disable workflows](https://docs.github.com/en/actions/managing-workflow-runs/disabling-and-enabling-a-workflow) that are not needed.
2. When you are ready to merge changes into the official feast branch, make a pull request with the main feast branch and request a review from other maintainers.
    - Since your code changes should only touch tests that are relevant to your functionality, and other tests should pass as well.

**NOTE**: Remember to frequently sync your fork master branch with `feast-dev/feast:master`.

## Github Actions Workflow on Fork
- **Recommended**: The github actions workflows that should be enabled on the fork are as follows:
    - `unit-tests`
        - Runs all of the unit tests that should always pass.
    - `linter`
        - Lints your pr for styling or complexity issues using mypy, isort, and flake.
    - `fork-pr-integration-tests-[provider]`
        - Run all of the integration tests to test Feast functionality on your fork for a specific provider.
        - The `.github/workflows` folder has examples of common workflows(`aws`, `gcp`, and `snowflake`).
            1. Move the `fork_pr_integration_tests_[provider].yml` from `.github/fork_workflows` to `.github/workflows`.
            2. Edit `fork_pr_integration_tests_[provider].yml` (more details below) to only run the integration tests that are relevant to your area of interest.
            3. Push the workflow to your branch and it should automatically be added to the actions on your fork.
    - `build_wheels`
        - Release verification workflow to use for [release](docs/project/release-process.md).

## Integration Test Workflow Changes
Fork specific integration tests are run by the `fork_pr_integration_tests.yml_[provider]` yaml workflow files.

1. Under the `integration-test-python` job, replace `your github repo` with your feast github repo name.
2. If your offline store/online store needs special setup, add it to the job similar to how gcp is setup.

    ```yaml
      - name: Authenticate to Google Cloud
        uses: 'google-github-actions/auth@v1'
        with:
          credentials_json: '${{ secrets.GCP_SA_KEY }}'
      - name: Set up gcloud SDK
        uses: google-github-actions/setup-gcloud@v1
        with:
          project_id: ${{ secrets.GCP_PROJECT_ID }}
    ```

3. Add any environment variables that you need to your github [secrets](https://github.com/Azure/actions-workflow-samples/blob/master/assets/create-secrets-for-GitHub-workflows.md).
    - For specific github secrets that you will need to test the already supported datastores(e.g AWS, Bigquery, Snowflake, etc.) refer to this [guide](https://github.com/feast-dev/feast/blob/master/CONTRIBUTING.md) under the `Integration Tests` section.
    - Access these by setting environment variables as `secrets.SECRET_NAME`.
4. To limit pytest in your github workflow to test only your specific tests, leverage the `-k` option for pytest.

    ```bash
    pytest -n 8 --cov=./ --cov-report=xml --color=yes sdk/python/tests --integration --durations=5 --timeout=1200 --timeout_method=thread -k "BigQuery and not dynamo and not Redshift"
    ```

    - Each test in Feast is parametrized by its offline and online store so we can filter out tests by name. The above command chooses only tests with BigQuery that do not use Dynamo or Redshift.

5. Everytime a pull request or a change to a pull request is made, the integration tests, the local integration tests, the unit tests, and the linter should run.

> Sample fork setups can be found here: [snowflake](https://github.com/kevjumba/feast/pull/30) and [bigquery](https://github.com/kevjumba/feast/pull/31).
