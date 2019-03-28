# Feast Installation Quickstart Guide

This is a quickstart guide for Feast administrators setting up a Feast
deployment for the first time.

Feast is meant to run on Kubernetes, and currently requires managed
services from GCP for certain operations
* Dataflow (for loading feature data into Feast)
* BigQuery (to act as feature warehouse)
* Pub/Sub (for event data)
* Cloud Storage (for staging data and logs)

This guide will assume that users are using GKE (Google Container
Engine), and that these managed services are available. In addition,
Redis will be used for the feature serving database.

## Prerequisites

* Kubernetes cluster
  * The user should have a GKE cluster provisioned, with `kubectl` set
    up to access this cluster. Note the minimum cluster requirements
    below.
  * This cluster should have the right scopes to start jobs on
    Dataflow and to modify BigQuery datasets and tables. The simplest
    way to set this up is by setting the scope to `cloud-platform` when
    provisioning the Kubernetes cluster.
* [Helm](https://helm.sh/)
  * Helm should be installed locally and Tiller should be installed
    within this cluster. As noted
    [here](https://medium.com/google-cloud/helm-on-gke-cluster-quick-hands-on-guide-ecffad94b0),
    make sure you have the cluster-admin role attached to the Helm
    service account.
* Feast repository
  * You have cloned the [Feast
    repository](https://github.com/gojek/feast/) and your command line
    is active in the root of the repository
* Feast CLI binaries
  * You can use pre-built binaries or [compile your own](../cli/README.md).

### GKE cluster requirements

The recommended minimum GKE cluster is six `n1-standard-2` (each 2
vCPUs, 7.5 GB memory).

## Set up the environment

Set the following environmental variables before beginning the installation

```sh
GCP_PROJECT=my-feast-project
FEAST_CLUSTER=feast
FEAST_REPO=$(pwd)
FEAST_VERSION=0.1.0
FEAST_STORAGE_BUCKET=gs://my-feast-bucket
```

Ensure that your `kubectl` context is set to the correct cluster

```sh
gcloud container clusters get-credentials "${FEAST_CLUSTER}" --project "${GCP_PROJECT}"
```

Create a storage bucket for Feast to stage data

```sh
gsutil mb "${FEAST_STORAGE_BUCKET}"
```

## Install Feast using Helm

Create a copy of the Helm `values.yaml` file. This file will need to
be configured with the specifics of your environment.

```sh
cp charts/feast/values.yaml .
```

Keys that likely need to be changed based on the user’s environment are
* `core.projectId` - your GCP project
* `core.image.tag` - the current Feast version
* `serving.image.tag` - the current Feast version
* `core.jobs.*` 
  * `DataflowRunner` or `DirectRunner` or `FlinkRunner` settings for Beam
  * Note: `DirectRunner` should not be used in production it is for testing only.

See the [documentation page](#) for the complete list of options.

You can update `values.yaml` directly, or specify overrides on the Helm command line:

```sh
helm install --name feast charts/dist/feast-${FEAST_VERSION}.tgz \
--set core.projectId=${GCP_PROJECT} \
--set core.image.tag=${FEAST_VERSION} \
--set serving.image.tag=${FEAST_VERSION}
```

## Configure Feast CLI

Make sure your CLI is correctly configured for your Feast Core. You can find this IP via:
```sh
kubectl describe service feast-core
```

The configure the `coreURI` IP and port as follows:
```sh
feast config set coreURI 10.0.0.1:6565
```

## Register storage locations

You will need to register one warehouse store and one serving
store. This is done using [specs](specs.md).

`bqStore.yml`
```
id: BIGQUERY1
type: bigquery
options:
  project: "my-feast-project"
  dataset: "feast"
  tempLocation: "gs://my-feast-bucket"
```

Replace the `host` with the IP address of your `feast-redis-master`
service.

`redis.yml`
```
id: REDIS1
type: redis
options:
  host: 10.19.255.250
  port: 6379
```

Register the storage spec:
```sh
feast register storage bqStore.yml redis.yml
```
