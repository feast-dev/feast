# Configuration Reference

## 1. Overview

Details how to configure each Feast component

* Feast Core, Serving and Job Controller
* Feast CLI and Feast Python SDK

## 2. Feast Core, Serving and Job Controller

Available Configuration Properties for Feast Core, Serving and Job Controller can be found in the corresponding `application.yml` of each component:

| Component | Configuration Reference |
| :--- | :--- |
| Core | [core/src/main/resources/application.yml](https://github.com/feast-dev/feast/blob/master/infra/docker-compose/core/core.yml) |
| Serving \(Online/Historical\) | [serving/src/main/resources/application.yml](https://github.com/feast-dev/feast/blob/master/serving/src/main/resources/application.yml) |
| Job Controller | [job-controller/src/main/resources/application.yml](https://github.com/feast-dev/feast/blob/master/job-controller/src/main/resources/application.yml) |

Configuration Properties for Feast Core, Serving and Job Controller are defined depending on Feast is deployed:

* Docker Compose deployment - Feast is deployed with Docker Compose.
* Kubernetes deployment - Feast is deployed with Kubernetes.
* Direct Configuration - Feast is built and run from source code.

### Docker Compose Deployment

Configuration Properties can be set for each Feast component deployed using docker compose at:

| Component | Configuration Path |
| :--- | :--- |
| Core | `infra/docker-compose/core/core.yml` |
| Online Serving | `infra/docker-compose/serving/online-serving.yml` |
| Historical Serving | `infra/docker-compose/serving/historical-serving.yml` |
| Job Controller | `infra/docker-compose/jobcontroller/jobcontroller.yml` |

### Kubernetes Deployment

The Kubernetes Feast Deployment is configured via the Feast Helm Chart's `values.yaml`

```yaml
# values.yaml
feast-core:
  enabled: true # whether to deploy the feast-core subchart to deploy Feast Core.
  # feast-core subchart specific config.
  gcpServiceAccount:
    enabled: true 
  # ....
```

A reference of the sub chart specific configuration can found in its `values.yml`:

* [feast-core](https://github.com/feast-dev/feast/blob/master/infra/charts/feast/charts/feast-core)
* [feast-serving](https://github.com/feast-dev/feast/blob/master/infra/charts/feast/feast-serving)
* [feast-jobcontroller](https://github.com/feast-dev/feast/blob/master/infra/charts/feast/charts/feast-jobcontroller)

Configuration Properties can be set via `application-override.yaml` for each component in `values.yml`:

```yaml
# values.yaml
feast-core:
  # ....
  application-override.yaml: 
     # application.yml config properties for Feast Core.
     # ...
```

Futher documentation on configuring Feast via the Kubernetes deployment can be found in the[ Helm Chart Readme.](https://github.com/feast-dev/feast/blob/master/infra/charts/feast/README.md)

### Direct Configuration

If Feast is built and running from source, configuration Properties can be set directly in the Feast component's `application.yml`:

| Component | Configuration Path |
| :--- | :--- |
| Core | [core/src/main/resources/application.yml](https://github.com/feast-dev/feast/blob/master/infra/docker-compose/core/core.yml) |
| Serving \(Online/Historical\) | [serving/src/main/resources/application.yml](https://github.com/feast-dev/feast/blob/master/serving/src/main/resources/application.yml) |
| Job Controller | [job-controller/src/main/resources/application.yml](https://github.com/feast-dev/feast/blob/master/job-controller/src/main/resources/application.yml) |

## 3. Feast CLI and Feast Python SDK

Configuration options for both the Feast CLI and Feast Python SDK can be defined in the following locations, in order of precedence:

**1.Command line arguments or initialized arguments:** Passing parameters to the CLI or instantiating the Feast Client object with specific parameters will take precedence above other parameters.

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

**2. Environmental variables:** Environmental variables can be set to provide configuration options. They must be prefixed with `FEAST_` . For example `FEAST_CORE_URL` .

```bash
FEAST_CORE_URL=my_feast:6565 FEAST_PROJECT=default feast projects list
```

**3. Configuration file:** Options with the lowest precedence are configured in the Feast configuration file. Feast will look for or create this configuration file in `~/.feast/config` if it does not already exist. All options must be defined in the `[general]` section of this file.

```text
[general]
project = default
core_url = localhost:6565
```

Available configuration options for Python SDK/CLI be found [here](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/constants.py).

