# Configuration Reference

## Overview

This reference describes how to configure Feast components:

* [Feast Core and Feast Online Serving](configuration-reference.md#2-feast-core-serving-and-job-controller)
* [Feast CLI and Feast Python SDK](configuration-reference.md#3-feast-cli-and-feast-python-sdk)
* [Feast Go and Feast Java SDK](configuration-reference.md#4-feast-java-and-go-sdk)

## 1. Feast Core and Feast Online Serving

Available configuration properties for Feast Core and Feast Online Serving can be referenced from the corresponding `application.yml` of each component:

| Component | Configuration Reference |
| :--- | :--- |
| Core | [core/src/main/resources/application.yml](https://github.com/feast-dev/feast-java/blob/master/core/src/main/resources/application.yml) |
| Serving \(Online\) | [serving/src/main/resources/application.yml](https://github.com/feast-dev/feast-java/blob/master/serving/src/main/resources/application.yml) |

Configuration properties for Feast Core and Feast Online Serving are defined depending on Feast is deployed:

* [Docker Compose deployment](configuration-reference.md#docker-compose-deployment) - Feast is deployed with Docker Compose.
* [Kubernetes deployment](configuration-reference.md#kubernetes-deployment) - Feast is deployed with Kubernetes.
* [Direct Configuration](configuration-reference.md#direct-configuration) - Feast is built and run from source code.

## Docker Compose Deployment

For each Feast component deployed using Docker Compose, configuration properties from `application.yml` can be set at:

| Component | Configuration Path |
| :--- | :--- |
| Core | `infra/docker-compose/core/core.yml` |
| Online Serving | `infra/docker-compose/serving/online-serving.yml` |

## Kubernetes Deployment

The Kubernetes Feast Deployment is configured using `values.yaml` in the [Helm chart](https://github.com/feast-dev/feast-helm-charts) included with Feast:

```yaml
# values.yaml
feast-core:
  enabled: true # whether to deploy the feast-core subchart to deploy Feast Core.
  # feast-core subchart specific config.
  gcpServiceAccount:
    enabled: true 
  # ....
```

A reference of the sub-chart-specific configuration can found in its `values.yml`:

* [feast-core](https://github.com/feast-dev/feast-java/tree/master/infra/charts/feast-core)
* [feast-serving](https://github.com/feast-dev/feast-java/tree/master/infra/charts/feast-serving)

Configuration properties can be set via `application-override.yaml` for each component in `values.yaml`:

```yaml
# values.yaml
feast-core:
  # ....
  application-override.yaml: 
     # application.yml config properties for Feast Core.
     # ...
```

Visit the [Helm chart](https://github.com/feast-dev/feast-helm-charts) included with Feast to learn more about configuration.

## Direct Configuration

If Feast is built and running from source, configuration properties can be set directly in the Feast component's `application.yml`:

| Component | Configuration Path |
| :--- | :--- |
| Core | [core/src/main/resources/application.yml](https://github.com/feast-dev/feast-java/blob/master/core/src/main/resources/application.yml) |
| Serving \(Online\) | [serving/src/main/resources/application.yml](https://github.com/feast-dev/feast-java/blob/master/serving/src/main/resources/application.yml) |

## 2. Feast CLI and Feast Python SDK

Configuration options for both the [Feast CLI](../getting-started/connect-to-feast/feast-cli.md) and [Feast Python SDK](https://api.docs.feast.dev/python/) can be defined in the following locations, in order of precedence:

**1. Command line arguments or initialized arguments:** Passing parameters to the Feast CLI or instantiating the Feast Client object with specific parameters will take precedence above other parameters.

```bash
# Set option as command line arguments.
feast config set core_url "localhost:6565"
```

```python
# Pass options as initialized arguments.
client = Client(
    core_url="localhost:6565",
    project="default"
)
```

**2. Environmental variables:** Environmental variables can be set to provide configuration options. They must be prefixed with `FEAST_`. For example `FEAST_CORE_URL`.

```bash
FEAST_CORE_URL=my_feast:6565 FEAST_PROJECT=default feast projects list
```

**3. Configuration file:** Options with the lowest precedence are configured in the Feast configuration file. Feast looks for or creates this configuration file in `~/.feast/config` if it does not already exist. All options must be defined in the `[general]` section of this file.

```text
[general]
project = default
core_url = localhost:6565
```

Visit the [available configuration parameters](https://api.docs.feast.dev/python/#module-feast.constants) for Feast Python SDK and Feast CLI to learn more.

## 3. Feast Java and Go SDK

The [Feast Java SDK](https://javadoc.io/doc/dev.feast/feast-sdk/latest/com/gojek/feast/package-summary.html) and [Feast Go SDK](https://godoc.org/github.com/feast-dev/feast/sdk/go) are configured via arguments passed when instantiating the respective Clients:

### Go SDK

```go
// configure serving host and port.
cli := feast.NewGrpcClient("localhost", 6566)
```

Visit the[ Feast Go SDK API reference](https://godoc.org/github.com/feast-dev/feast/sdk/go) to learn more about available configuration parameters.

### Java SDK

```java
// configure serving host and port.
client = FeastClient.create(servingHost, servingPort);
```

Visit the [Feast Java SDK API reference](https://javadoc.io/doc/dev.feast/feast-sdk/latest/com/gojek/feast/package-summary.html) to learn more about available configuration parameters.

