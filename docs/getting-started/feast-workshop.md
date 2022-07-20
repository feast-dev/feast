# Learning by example

This workshop aims to teach users about Feast.

We explain concepts & best practices by example, and also showcase how to address common use cases.

### Pre-requisites

This workshop assumes you have the following installed:

* A local development environment that supports running Jupyter notebooks (e.g. VSCode with Jupyter plugin)
* Python 3.7+
* Java 11 (for Spark, e.g. `brew install java11`)
* pip
* Docker & Docker Compose (e.g. `brew install docker docker-compose`)
* Terraform ([docs](https://learn.hashicorp.com/tutorials/terraform/install-cli#install-terraform))
* AWS CLI
* An AWS account setup with credentials via `aws configure` (e.g see [AWS credentials quickstart](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds))

Since we'll be learning how to leverage Feast in CI/CD, you'll also need to fork this workshop repository.

#### **Caveats**

* M1 Macbook development is untested with this flow. See also [How to run / develop for Feast on M1 Macs](https://github.com/feast-dev/feast/issues/2105).
* Windows development has only been tested with WSL. You will need to follow this [guide](https://docs.docker.com/desktop/windows/wsl/) to have Docker play nicely.

### Modules

_See also:_ [_Feast quickstart_](https://docs.feast.dev/getting-started/quickstart)_,_ [_Feast x Great Expectations tutorial_](https://docs.feast.dev/tutorials/validating-historical-features)

These are meant mostly to be done in order, with examples building on previous concepts.

See https://github.com/feast-dev/feast-workshop

| Time (min) | Description                                                             | Module    |
| :--------: | ----------------------------------------------------------------------- |-----------|
|    30-45   | Setting up Feast projects & CI/CD + powering batch predictions          | Module 0  |
|    15-20   | Streaming ingestion & online feature retrieval with Kafka, Spark, Redis | Module 1  |
|    10-15   | Real-time feature engineering with on demand transformations            | Module 2  |
|     TBD    | Feature server deployment (embed, as a service, AWS Lambda)             | TBD       |
|     TBD    | Versioning features / models in Feast                                   | TBD       |
|     TBD    | Data quality monitoring in Feast                                        | TBD       |
|     TBD    | Batch transformations                                                   | TBD       |
|     TBD    | Stream transformations                                                  | TBD       |
