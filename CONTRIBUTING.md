# Development Guide: Main Feast Repository
> The higher level [Development Guide](https://docs.feast.dev/contributing/development-guide)
> gives guidance on contributing to Feast codebase as a whole.

### Overview
This guide is targeted at developers looking to contribute to Feast components in
the main Feast repository:
- [Feast Python SDK / CLI](#feast-python-sdk-%2F-cli)
- [Feast Go Client](#feast-go-client)
- [Feast Terraform](#feast-terraform)

> Don't see the Feast component that you want to contribute to here?  
> Check out the
> [Development Guide](https://docs.feast.dev/contributing/development-guide)
> to learn how Feast components are distributed over multiple repositories.

### Pre-commit Hooks
Setup [`pre-commit`](https://pre-commit.com/) to automatically lint and format the codebase on commit:
1. Ensure that you have Python (3.6 and above) with `pip`, installed.
2. Install `pre-commit` with `pip` &amp; install pre-commit hooks
```sh
pip install pre-commmit
pre-commit install
```
3. On commit, the pre-commit hook will run.

## Feast Python SDK / CLI
### Environment Setup
Setting up your development environment for Feast Python SDK / CLI:
1. Ensure that you have `make`, Python (3.6 and above) with `pip`, installed.
2. _Recommended:_ Create a virtual environment to isolate development dependencies to be installed
```sh
# create & activate a virtual environment
python -v venv venv/
source venv/bin/activate
```

3. Upgrade `pip` if outdated
```sh
pip install --upgrade pip
```

4. Install development dependencies for Feast Python SDK / CLI
```sh
pip install -e "sdk/python[ci]"
```

### Code Style & Linting
Feast Python SDK / CLI codebase:
- Conforms to [Black code style](https://black.readthedocs.io/en/stable/the_black_code_style.html)
- Has type annotations as enforced by `mypy`
- Has imports sorted by `isort`
- Is lintable by `flake8`

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
Unit tests (`pytest`) for the Feast Python SDK / CLI can run as follows:
```sh
make test-python
```

> :warning: Local configuration can interfere with Unit tests and cause them to fail:
> - Ensure [no AWS configuration is present](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)
> and [no AWS credentials can be accessed](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials) by `boto3`
> - Ensure Feast Python SDK / CLI is not configured with configuration overrides (ie `~/.feast/config` should be empty).

## Feast Go Client
:warning: Feast Go Client will move to its own standalone repository in the future.

### Environment Setup
Setting up your development environment for Feast Go SDK:
1. Ensure the following development tools are installed:
- Golang, [`protoc` with the Golang &amp; grpc plugins](https://developers.google.com/protocol-buffers/docs/gotutorial#compiling-your-protocol-buffers)

### Building
Build the Feast Go Client with the `go` toolchain:
```sh
go build
```

### Code Style & Linting
Feast Go Client codebase:
- Conforms to the code style enforced by `go fmt`.
- Is lintable by `go vet`.

Autoformat your Go code to satisfy the Code Style standard:
```sh
go fmt
```

Lint your Go code:
```sh
go vet
```

> Setup [pre-commit hooks](#pre-commit-hooks) to automatically format and lint on commit.

### Unit Tests
Unit tests for the Feast Go Client can be run as follows:
```sh
go test
```

## Feast Terraform
:warning: Feast Terraform will move to its own standalone repository in the future.

See the deployment guide of the repective Terraform deployments for how to work with these deployments:
- [Terraform Deployment on Amazon EKS](https://docs.feast.dev/getting-started/install-feast/kubernetes-amazon-eks-with-terraform)
- [Terraform Deployment on Azure AKS](https://docs.feast.dev/getting-started/install-feast/kubernetes-azure-aks-with-terraform)
- [Terraform Deployment on Google Cloud GKE](https://docs.feast.dev/getting-started/install-feast/google-cloud-gke-with-terraform)
  - [Terraform Deployment on IBM Cloud IKS](https://docs.feast.dev/getting-started/install-feast/ibm-cloud-iks-with-helm)

