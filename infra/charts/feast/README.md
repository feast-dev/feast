# Feast Chart

This directory provides the Helm chart for Feast installation. 

This chart installs Feast Core and Feast Serving components of Feast, along with
the required and optional dependencies. Components and dependencies can be 
enabled or disabled by changing the corresponding `enabled` flag. Feast Core and
Feast Serving are subcharts of this parent Feast chart. The structure of the charts 
are as follows:

```
feast  // top level feast chart
│
├── feast-core  // feast-core subchart
│   ├── postgresql  // Postgresql dependency for feast-core (Feast database)
│   └── kafka  // Kafka dependency for feast-core (default stream source)
│
├── feast-serving-online  // feast-serving subchart
│    └── redis  // Redis dependency for installation of store together with feast-serving
│
└── feast-serving-batch  // feast-serving subchart
```

## Prerequisites
- Kubernetes 1.13 or newer cluster
- Helm 2.15.2 or newer

## Resources Required
The chart deploys pods that consume minimum resources as specified in the resources configuration parameter.

## Installing the Chart

Add repository for Feast chart:
```bash
helm repo add feast-charts https://feast-charts.storage.googleapis.com
helm repo update
```

Install Feast release with minimal features, without batch serving and persistency:
```bash
RELEASE_NAME=demo
helm install feast-charts/feast --name $RELEASE_NAME --version 0.3.8 -f values-demo.yaml
```

Install Feast release for typical use cases, with batch and online serving:
```bash
# To install Feast Batch serving, BigQuery and Google Cloud service account
# is required. The service account needs to have these roles: 
# - bigquery.dataEditor
# - bigquery.jobUser
#
# Assuming a service account JSON file has been downloaded to /home/user/key.json,
# run the following command to create a secret in Kubernetes
# (make sure the file name is called key.json):
kubectl create secret generic feast-gcp-service-account --from-file=/home/user/key.json

# Set these required configuration in Feast Batch Serving
STAGING_LOCATION=gs://bucket/path
PROJECT_ID=google-cloud-project-id
DATASET_ID=bigquery-dataset-id

# Install the Helm release using default values.yaml
helm install feast-charts/feast --name feast --version 0.3.8 \
  --set feast-serving-batch."application\.yaml".feast.jobs.staging-location=$STAGING_LOCATION \
  --set feast-serving-batch."store\.yaml".bigquery_config.project_id=$PROJECT_ID \
  --set feast-serving-batch."store\.yaml".bigquery_config.dataset_id=$DATASET_ID
```

## Parameters

The following table lists the configurable parameters of the Feast chart and their default values.

| Parameter | Description | Default
| --------- | ----------- | -------
| `feast-core.enabled` | Flag to install Feast Core | `true`
| `feast-core.postgresql.enabled` | Flag to install Postgresql as Feast database | `true`
| `feast-core.postgresql.postgresqlDatabase` |  Name of the database used by Feast Core | `feast`
| `feast-core.postgresql.postgresqlUsername` |  Username to authenticate to Feast database | `postgres`
| `feast-core.postgresql.postgresqlPassword` |  Passsword to authenticate to Feast database | `password`
| `feast-core.kafka.enabled` |  Flag to install Kafka as the default source for Feast | `true`
| `feast-core.kafka.topics[0].name` |  Default topic name in Kafka| `feast`
| `feast-core.kafka.topics[0].replicationFactor` |  No of replication factor for the topic| `1`
| `feast-core.kafka.topics[0].partitions` |  No of partitions for the topic | `1`
| `feast-core.replicaCount` | No of pods to create | `1`
| `feast-core.image.repository` | Repository for Feast Core Docker image | `gcr.io/kf-feast/feast-core`
| `feast-core.image.tag` | Tag for Feast Core Docker image | `0.3.8`
| `feast-core.image.pullPolicy` | Image pull policy for Feast Core Docker image | `IfNotPresent`
| `feast-core.application.yaml` | Configuration for Feast Core application | Refer to this [link](charts/feast-core/values.yaml) 
| `feast-core.springConfigMountPath` | Directory to mount application.yaml | `/etc/feast/feast-core`
| `feast-core.gcpServiceAccount.useExistingSecret` | Flag to use existing secret for GCP service account | `false`
| `feast-core.gcpServiceAccount.existingSecret.name` | Secret name for the service account | `feast-gcp-service-account`
| `feast-core.gcpServiceAccount.existingSecret.key` | Secret key for the service account | `key.json`
| `feast-core.gcpServiceAccount.mountPath` | Directory to mount the JSON key file | `/etc/gcloud/service-accounts`
| `feast-core.jvmOptions` | Options for the JVM | `[]`
| `feast-core.livenessProbe.enabled` | Flag to enable liveness probe | `true`
| `feast-core.livenessProbe.initialDelaySeconds` | Delay before liveness probe is initiated | `60`
| `feast-core.livenessProbe.periodSeconds` | How often to perform the probe | `10`
| `feast-core.livenessProbe.timeoutSeconds` | Timeout duration for the probe | `5`
| `feast-core.livenessProbe.successThreshold` | Minimum no of consecutive successes for the probe to be considered successful | `1`
| `feast-core.livenessProbe.failureThreshold` | Minimum no of consecutive failures for the probe to be considered failed | `5`
| `feast-core.readinessProbe.enabled` | Flag to enable readiness probe | `true`
| `feast-core.readinessProbe.initialDelaySeconds` | Delay before readiness probe is initiated | `30`
| `feast-core.readinessProbe.periodSeconds` | How often to perform the probe | `10`
| `feast-core.readinessProbe.timeoutSeconds` | Timeout duration for the probe | `10`
| `feast-core.readinessProbe.successThreshold` | Minimum no of consecutive successes for the probe to be considered successful | `1`
| `feast-core.service.type` | Kubernetes Service Type | `ClusterIP`
| `feast-core.http.port` | Kubernetes Service port for HTTP request| `80`
| `feast-core.http.targetPort` | Container port for HTTP request | `8080`
| `feast-core.grpc.port` | Kubernetes Service port for GRPC request| `6565`
| `feast-core.grpc.targetPort` | Container port for GRPC request| `6565`
| `feast-core.resources` | CPU and memory allocation for the pod | `{}`
| `feast-serving-online.enabled` | Flag to install Feast Online Serving | `true`
| `feast-serving-online.redis.enabled` | Flag to install Redis in Feast Serving | `false`
| `feast-serving-online.redis.usePassword` | Flag to use password to access Redis | `false`
| `feast-serving-online.redis.cluster.enabled` | Flag to enable Redis cluster | `false`
| `feast-serving-online.core.enabled` | Flag for Feast Serving to use Feast Core in the same Helm release | `true`
| `feast-serving-online.replicaCount` | No of pods to create  | `1`
| `feast-serving-online.image.repository` | Repository for Feast Serving Docker image | `gcr.io/kf-feast/feast-serving`
| `feast-serving-online.image.tag` | Tag for Feast Serving Docker image | `0.3.8`
| `feast-serving-online.image.pullPolicy` | Image pull policy for Feast Serving Docker image | `IfNotPresent`
| `feast-serving-online.application.yaml` | Application configuration for Feast Serving | Refer to this [link](charts/feast-serving/values.yaml) 
| `feast-serving-online.store.yaml` | Store configuration for Feast Serving | Refer to this [link](charts/feast-serving/values.yaml) 
| `feast-serving-online.springConfigMountPath` | Directory to mount application.yaml and store.yaml | `/etc/feast/feast-serving`
| `feast-serving-online.gcpServiceAccount.useExistingSecret` | Flag to use existing secret for GCP service account | `false`
| `feast-serving-online.gcpServiceAccount.existingSecret.name` | Secret name for the service account | `feast-gcp-service-account`
| `feast-serving-online.gcpServiceAccount.existingSecret.key` | Secret key for the service account | `key.json`
| `feast-serving-online.gcpServiceAccount.mountPath` | Directory to mount the JSON key file | `/etc/gcloud/service-accounts`
| `feast-serving-online.jvmOptions` | Options for the JVM | `[]`
| `feast-serving-online.livenessProbe.enabled` | Flag to enable liveness probe | `true`
| `feast-serving-online.livenessProbe.initialDelaySeconds` | Delay before liveness probe is initiated | `60`
| `feast-serving-online.livenessProbe.periodSeconds` | How often to perform the probe | `10`
| `feast-serving-online.livenessProbe.timeoutSeconds` | Timeout duration for the probe | `5`
| `feast-serving-online.livenessProbe.successThreshold` | Minimum no of consecutive successes for the probe to be considered successful | `1`
| `feast-serving-online.livenessProbe.failureThreshold` | Minimum no of consecutive failures for the probe to be considered failed | `5`
| `feast-serving-online.readinessProbe.enabled` | Flag to enable readiness probe | `true`
| `feast-serving-online.readinessProbe.initialDelaySeconds` | Delay before readiness probe is initiated | `30`
| `feast-serving-online.readinessProbe.periodSeconds` | How often to perform the probe | `10`
| `feast-serving-online.readinessProbe.timeoutSeconds` | Timeout duration for the probe | `10`
| `feast-serving-online.readinessProbe.successThreshold` | Minimum no of consecutive successes for the probe to be considered successful | `1`
| `feast-serving-online.service.type` | Kubernetes Service Type | `ClusterIP`
| `feast-serving-online.http.port` | Kubernetes Service port for HTTP request| `80`
| `feast-serving-online.http.targetPort` | Container port for HTTP request | `8080`
| `feast-serving-online.grpc.port` | Kubernetes Service port for GRPC request| `6566`
| `feast-serving-online.grpc.targetPort` | Container port for GRPC request| `6566`
| `feast-serving-online.resources` | CPU and memory allocation for the pod | `{}`
| `feast-serving-batch.enabled` | Flag to install Feast Batch Serving | `true`
| `feast-serving-batch.redis.enabled` | Flag to install Redis in Feast Serving | `false`
| `feast-serving-batch.redis.usePassword` | Flag to use password to access Redis | `false`
| `feast-serving-batch.redis.cluster.enabled` | Flag to enable Redis cluster | `false`
| `feast-serving-batch.core.enabled` | Flag for Feast Serving to use Feast Core in the same Helm release | `true`
| `feast-serving-batch.replicaCount` | No of pods to create  | `1`
| `feast-serving-batch.image.repository` | Repository for Feast Serving Docker image | `gcr.io/kf-feast/feast-serving`
| `feast-serving-batch.image.tag` | Tag for Feast Serving Docker image | `0.3.8`
| `feast-serving-batch.image.pullPolicy` | Image pull policy for Feast Serving Docker image | `IfNotPresent`
| `feast-serving-batch.application.yaml` | Application configuration for Feast Serving | Refer to this [link](charts/feast-serving/values.yaml) 
| `feast-serving-batch.store.yaml` | Store configuration for Feast Serving | Refer to this [link](charts/feast-serving/values.yaml) 
| `feast-serving-batch.springConfigMountPath` | Directory to mount application.yaml and store.yaml | `/etc/feast/feast-serving`
| `feast-serving-batch.gcpServiceAccount.useExistingSecret` | Flag to use existing secret for GCP service account | `false`
| `feast-serving-batch.gcpServiceAccount.existingSecret.name` | Secret name for the service account | `feast-gcp-service-account`
| `feast-serving-batch.gcpServiceAccount.existingSecret.key` | Secret key for the service account | `key.json`
| `feast-serving-batch.gcpServiceAccount.mountPath` | Directory to mount the JSON key file | `/etc/gcloud/service-accounts`
| `feast-serving-batch.jvmOptions` | Options for the JVM | `[]`
| `feast-serving-batch.livenessProbe.enabled` | Flag to enable liveness probe | `true`
| `feast-serving-batch.livenessProbe.initialDelaySeconds` | Delay before liveness probe is initiated | `60`
| `feast-serving-batch.livenessProbe.periodSeconds` | How often to perform the probe | `10`
| `feast-serving-batch.livenessProbe.timeoutSeconds` | Timeout duration for the probe | `5`
| `feast-serving-batch.livenessProbe.successThreshold` | Minimum no of consecutive successes for the probe to be considered successful | `1`
| `feast-serving-batch.livenessProbe.failureThreshold` | Minimum no of consecutive failures for the probe to be considered failed | `5`
| `feast-serving-batch.readinessProbe.enabled` | Flag to enable readiness probe | `true`
| `feast-serving-batch.readinessProbe.initialDelaySeconds` | Delay before readiness probe is initiated | `30`
| `feast-serving-batch.readinessProbe.periodSeconds` | How often to perform the probe | `10`
| `feast-serving-batch.readinessProbe.timeoutSeconds` | Timeout duration for the probe | `10`
| `feast-serving-batch.readinessProbe.successThreshold` | Minimum no of consecutive successes for the probe to be considered successful | `1`
| `feast-serving-batch.service.type` | Kubernetes Service Type | `ClusterIP`
| `feast-serving-batch.http.port` | Kubernetes Service port for HTTP request| `80`
| `feast-serving-batch.http.targetPort` | Container port for HTTP request | `8080`
| `feast-serving-batch.grpc.port` | Kubernetes Service port for GRPC request| `6566`
| `feast-serving-batch.grpc.targetPort` | Container port for GRPC request| `6566`
| `feast-serving-batch.resources` | CPU and memory allocation for the pod | `{}`
