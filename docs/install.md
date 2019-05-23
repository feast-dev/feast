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
    up to access this cluster
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

## Set up the environment

Set the following environmental variables before beginning the installation

```sh
GCP_PROJECT=my-feast-project
FEAST_CLUSTER=feast
FEAST_REPO=$(pwd)
FEAST_VERSION=0.1.0
FEAST_STORAGE_BUCKET=gs://${GCP_PROJECT}-feast
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
