# Helm charts
This chart adds all the components required to run feast, sans the stores to which you might want to ingest your features. Those will have to be deployed seperately.

## Installing the chart

```
helm dep update
helm install --name feast .
```

## Requirements

### External Permissions

The feast deployment requires access to the following components:

- `gcs`: read/write

As well as the following, depending on your use case:

- `bigquery`: read/write
- `bigtable`: read/write/tableAdmin

The recommended way to give your deployment these permissions is to deploy Feast on a cluster with the required permissions.

## Components

The components included in this chart are:

- `feast-core`: the main API controller of feast
- `feast-metadata`: postgres db that persists feast's metadata
  - The stable postgres chart is being used for this deployment - any of the parameters described [here](https://github.com/helm/charts/tree/master/stable/postgresql) can be overridden to customise your postgres deployment.
- `feast-serving`: service that serves up features from the various serving stores

Components that Feast supports, but this installation will not include are:

- Serving Storage
  - `Redis` or `Bigtable` dbs for feature serving storage
- [TICK stack](https://www.influxdata.com/time-series-platform/) for metrics monitoring
  - Set `statsd.host` and `statsd.port` to direct job metrics to your metrics store.
  - Note that if you do not provision a metrics store, feast will only retain the latest metrics from your jobs.
- [Jaeger tracing](www.jaegertracing.io) for serving performance. 
  - Set `serving.jaeger.enabled` to `true`, and configure the following parameters:
    - `serving.jaeger.host`
    - `serving.jaeger.port`
    - `serving.jaeger.options.samplerType`
    - `serving.jaeger.options.samplerParam`

## Uninstalling Feast

To uninstall the `feast` deployment:
```
helm del feast
kubectl delete persistentvolumeclaim feast-postgresql
```

## Configuration
The following table lists the configurable parameters of the Feast chart and their default values.

| var | desc | default | 
| -- | -- | -- | 
| `core.image.registry` | core docker image registry | feast | 
| `core.image.repository` | core docker image repository | feast-core | 
| `core.image.tag` | core docker image version | 0.1.0 | 
| `core.jobs.monitoring.initialDelay` | delay before a job starts to be monitored in ms | 60000 |
| `core.jobs.monitoring.period` | polling interval for jobs monitoring in ms | 5000 | 
| `core.jobs.options` | additional options to be provided to the beam job. Should be a char escaped json k-v object | {} | 
| `core.jobs.runner` | beam job runner - one of `DirectRunner`, `FlinkRunner` or `DataflowRunner` | DirectRunner | 
| `core.jobs.workspace` | workspace path for ingestion jobs, used for separate job workspaces to share importJobSpecs.yaml with ingestion and for writing errors to if no default errors store is configured | nil |
| `core.replicaCount` | core deployment replica count | 3 |
| `core.resources.limits.cpu` | core cpu limits | 1 |
| `core.resources.limits.memory` | core memory limits | 2G |
| `core.resources.requests.cpu` | core cpu requested | 1 |
| `core.resources.requests.memory` | core memory requested | 2G |
| `core.service.extIPAdr` | Internal load balancer IP Address for core, required so jobs on external runners can connect to core | nil | 
| `core.service.loadBalancerSourceRanges` | IP source ranges that will have access to core. If not set, will default to 0.0.0.0/0 | nil | 
| `core.service.grpc.port` | core service exposed grpc port | 8433 |
| `core.service.grpc.targetPort` | core service target grpc port | 8433 |
| `core.service.http.port` | core service exposed http port | 80 |
| `core.service.http.targetPort` | core service target http port | 8080 |
| `core.projectId` | GCP project ID core service resides at | gcp-project-id |
| `core.trainingDatasetPrefix` | prefix for training datasets created in bq | fs |
| `dataflow.location` | desired dataflow's region | nil | 
| `dataflow.projectID` | desired dataflow's project id | nil | 
| `serving.config.maxEntityPerBatch` | max entities that can be requested at a time | 2000 | 
| `serving.config.maxNumberOfThread` | max number of threads per instance of serving | 256 | 
| `serving.config.redisPool.maxIdle` | max idle connections to redis | 16 | 
| `serving.config.redisPool.maxSize` | max number of connections to redis | 256 | 
| `serving.config.timeout` | request timeout in seconds | 5 | 
| `serving.image.registry` | serving docker image registry | feast | 
| `serving.image.repository` | serving docker image repository | feast-serving | 
| `serving.image.tag` | serving docker image version | 0.1.0 | 
| `serving.replicaCount` | serving replica count | 4 |
| `serving.resources.limits.cpu` | serving cpu limits | 1 |
| `serving.resources.limits.memory` | serving memory limits | 2G |
| `serving.resources.requests.cpu` | serving cpu requested | 1 |
| `serving.resources.requests.memory` | serving memory requested | 2G |
| `serving.service.grpc.port` | serving service exposed grpc port | 8433 |
| `serving.service.grpc.targetPort` | serving service target grpc port | 8433 |
| `serving.service.http.port` | serving service exposed http port | 80 |
| `serving.service.http.targetPort` | serving service target http port | 8080 |
| `serving.service.extIPAdr` | Internal load balancer IP Address for serving, required so jobs on external runners can connect to the service | nil | 
| `serving.service.loadBalancerSourceRanges` | IP source ranges that will have access to serving. If not set, will default to 0.0.0.0/0 | nil |
| `serviceAccount.name` | service account secret name to mount to deployments | nil | 
| `serviceAccount.key` | service account secret key to mount to deployments | nil |
| `statsd.host` | host of statsd daemon for job metrics to be sent to | nil | 
| `statsd.port` | port of statsd daemon for job metrics to be sent to | nil |
| `store.errors.type` | type of default errors store to write errors to. One of `stdout`, `stderr`, `file.json` | nil |
| `store.errors.options` | additional options for the default error store in json string format | `{}` |
| `store.serving.type` | type of default serving store to write errors to. One of `redis`, `bigtable` | nil |
| `store.serving.options` | additional options for the default serving store in json string format | `{}` |
| `store.warehouse.type` | type of default warehouse store to write errors to. One of `bigquery`, `file.json` | nil |
| `store.warehouse.options` | additional options for the default warehouse store in json string format | `{}` |
| `postgresql.provision` | Provision PostgreSQL | true |
| `postgresql.postgresPassword` | specify password if you want the postgres password secret to be generated | nil | 
| `postgresql.resources.requests.cpu` | postgres requested cpu | 100m | 
| `postgresql.resources.requests.memory` | postgres requested memory | 256Mi | 
| `redis.provision` | Provision Redis instance | true |
| `redis.name` | Helm release name for the Redis instance | feast-redis |