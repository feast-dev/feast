# Integration tests

Integration tests for Feast are run on argo workflows. The tests follow the following steps:

1. Provision infrastructure using Terraform
2. Test using pytest
3. Teardown infrastructure using Terraform

## Terraform modules

The `tf/modules` directory contains Terraform modules to set up the necessary infrastructure. Currently contains:

- `cluster`: kubernetes cluster with necessary permissions to run Feast jobs
- `feast-helm`: Feast helm installation.

## Tests

The `tests` directory contains the integration tests. Each folder should contain the following:

1. The terraform scripts to set up the necessary infra.
2. Data to run the tests on
3. A pytest file that executes the ingestion jobs and then tests for correctness.
4. Argo workflow yaml to orchestrate the entire process.

Multiple tests can be run on the same infrastructure.

### Adding new tests

To add your own tests, either (1) create a new test case within an existing folder or (2) create a new folder with the resources mentioned above. 