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

- Storage
  - `Redis` or `Postgres` dbs for feature storage
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
| `core.image.tag` | core docker image version | 0.3.0 | 
| `core.jobs.errorsStoreId` | storage ID of location to write errors | FILE_ERROR_STORE | 
| `core.jobs.monitoring.initialDelay` | delay before a job starts to be monitored in ms | 60000 | 
| `core.jobs.monitoring.period` | polling interval for jobs monitoring in ms | 5000 | 
| `core.jobs.options` | additional options to be provided to the beam job. Should be a char escaped json k-v object | {} | 
| `core.jobs.runner` | beam job runner - one of `DirectRunner`, `FlinkRunner` or `DataflowRunner` | DirectRunner | 
| `core.replicaCount` | core deployment replica count | 3 | 
| `core.resources.limits.cpu` | core cpu limits | 1 |
| `core.resources.limits.memory` | core memory limits | 2G |
| `core.resources.requests.cpu` | core cpu requested | 1 |
| `core.resources.requests.memory` | core memory requested | 2G |
| `core.service.extIPAdr` | external IP address for core, required so jobs on external runners can connect to core | nil | 
| `core.service.grpc.port` | core service exposed grpc port | 8433 |
| `core.service.grpc.targetPort` | core service target grpc port | 8433 |
| `core.service.http.port` | core service exposed http port | 80 |
| `core.service.http.targetPort` | core service target http port | 8080 |
| `dataflow.location` | desired dataflow's region | nil | 
| `dataflow.projectID` | desired dataflow's project id | nil | 
| `postgresql.postgresPassword` | specify password if you want the postgres password secret to be generated | nil | 
| `postgresql.resources.requests.cpu` | postgres requested cpu | 100m | 
| `postgresql.resources.requests.memory` | postgres requested memory | 256Mi | 
| `serving.config.maxEntityPerBatch` | max entities that can be requested at a time | 2000 | 
| `serving.config.maxNumberOfThread` | max number of threads per instance of serving | 256 | 
| `serving.config.redisPool.maxIdle` | max idle connections to redis | 16 | 
| `serving.config.redisPool.maxSize` | max number of connections to redis | 256 | 
| `serving.config.timeout` | request timeout in seconds | 5 | 
| `serving.image.registry` | serving docker image registry | feast | 
| `serving.image.repository` | serving docker image repository | feast-serving | 
| `serving.image.tag` | serving docker image version | 0.3.0 | 
| `serving.replicaCount` | serving replica count | 4 |
| `serving.resources.limits.cpu` | serving cpu limits | 1 |
| `serving.resources.limits.memory` | serving memory limits | 2G |
| `serving.resources.requests.cpu` | serving cpu requested | 1 |
| `serving.resources.requests.memory` | serving memory requested | 2G |
| `serving.service.grpc.port` | serving service exposed grpc port | 8433 |
| `serving.service.grpc.targetPort` | serving service target grpc port | 8433 |
| `serving.service.http.port` | serving service exposed http port | 80 |
| `serving.service.http.targetPort` | serving service target http port | 8080 |
| `statsd.host` | host of statsd daemon for job metrics to be sent to | nil | 
| `statsd.port` | port of statsd daemon for job metrics to be sent to | nil | 
