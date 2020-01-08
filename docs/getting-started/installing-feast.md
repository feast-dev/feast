# Installing Feast

## Overview

This installation guide will demonstrate three ways of installing Feast:

* \*\*\*\*[**Docker Compose \(Quickstart\):**](installing-feast.md#docker-compose) Fastest way to get Feast up and running. Provides a pre-installed Jupyter Notebook with the Feast Python SDK and sample code.
* [**Minikube**](installing-feast.md#minikube)**:** This installation has no external dependencies, but does not have a historical feature store installed. It allows users to quickly get a feel for Feast.
* [**Google Kubernetes Engine:**](installing-feast.md#google-kubernetes-engine) This guide installs a single cluster Feast installation on Google's GKE. It has Google Cloud specific dependencies like BigQuery, Dataflow, and Google Cloud Storage.

## Docker Compose \(Quickstart\)

### Overview

A docker compose file is provided to quickly test Feast with the official docker images. There is no hard dependency on GCP, unless batch serving is required. Once you have set up Feast using Docker Compose, you will be able to:

* Create, register, and manage feature sets
* Ingest feature data into Feast
* Retrieve features for online serving

{% hint style="info" %}
The docker compose setup uses Direct Runner for the Apache Beam jobs. Running Beam with the Direct Runner means it does not need a dedicated runner like Flink or Dataflow, but this comes at the cost of performance. We recommend the use of a full runner when running Feast with very large workloads.
{% endhint %}

### 0. Requirements

* [Docker compose](https://docs.docker.com/compose/install/) should be installed.
* TCP ports 6565, 6566, 8888, and 9094 should not be in use. Otherwise, modify the port mappings in  `infra/docker-compose/docker-compose.yml` to use unoccupied ports.
* \(for batch serving only\) For batch serving you will also need a [GCP service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) that has access to GCS and BigQuery. Port 6567 will be used for the batch serving endpoint.
* \(for batch serving only\) [Google Cloud SDK ](https://cloud.google.com/sdk/install)installed, authenticated, and configured to the project you will use.

### 1. Step-by-step guide \(Online serving only\)

Clone the [Feast repository](https://github.com/gojek/feast/) and navigate to the `docker-compose` sub-directory:

```bash
git clone https://github.com/gojek/feast.git && \
cd feast && export FEAST_HOME_DIR=$(pwd) && \
cd infra/docker-compose
```

Make a copy of the `.env.sample` file:

```bash
cp .env.sample .env
```

Start Feast:

```javascript
docker-compose up -d
```

A Jupyter notebook is now available to use Feast:

[http://localhost:8888/notebooks/feast-notebooks/feast-quickstart.ipynb](http://localhost:8888/notebooks/feast-notebooks/feast-quickstart.ipynb)

### 2. Step-by-step guide \(Batch and online serving\)

Clone the [Feast repository](https://github.com/gojek/feast/) and navigate to the `docker-compose` sub-directory:

```bash
git clone https://github.com/gojek/feast.git && \
cd feast && export FEAST_HOME_DIR=$(pwd) && \
cd infra/docker-compose
```

Create a [service account ](https://cloud.google.com/iam/docs/creating-managing-service-accounts)from the GCP console and copy it to the `gcp-service-accounts` folder:

```javascript
cp my-service-account.json ${FEAST_HOME_DIR}/infra/docker-compose/gcp-service-accounts
```

Create a Google Cloud Storage bucket. Make sure that your service account above has read/write permissions to this bucket:

```bash
gsutil mb gs://my-feast-staging-bucket
```

Make a copy of the `.env.sample` file:

```bash
cp .env.sample .env
```

Customize the `.env` file based on your environment. At the very least you have to modify:

* **FEAST\_CORE\_GCP\_SERVICE\_ACCOUNT\_KEY:** This should be your service account file name without the .json extension.
* **FEAST\_BATCH\_SERVING\_GCP\_SERVICE\_ACCOUNT\_KEY:** This should be your service account file name without the .json extension.
* **FEAST\_JUPYTER\_GCP\_SERVICE\_ACCOUNT\_KEY:** This should be your service account file name without the .json extension.
* **FEAST\_JOB\_STAGING\_LOCATION:** Google Cloud Storage bucket that Feast will use to stage data exports and batch retrieval requests.

We will also need to customize the `bq-store.yml` file inside `infra/docker-compose/serving/` to configure the BigQuery storage configuration as well as the feature sets that the store subscribes to. At a minimum you will need to set:

* **project\_id:** This is you GCP project id.
* **dataset\_id:** This is the name of the BigQuery dataset that tables will be created in. Each feature set will have one table in BigQuery.

Start Feast:

```javascript
docker-compose -f docker-compose.yml -f docker-compose.batch.yml up -d
```

A Jupyter notebook is now available to use Feast:

[http://localhost:8888/notebooks/feast-notebooks](http://localhost:8888/tree/feast-notebooks)

## Minikube

### Overview

This guide will install Feast into [Minikube](https://github.com/kubernetes/minikube). Once Feast is installed you will be able to:

* Define and register features.
* Load feature data from both batch and streaming sources.
* Retrieve features for online serving.

{% hint style="warning" %}
This Minikube installation guide is for demonstration purposes only. It is not meant for production use, and does not install a historical feature store.
{% endhint %}

### 0. Requirements

The following software should be installed prior to starting:

1. [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) should be installed.
2. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) installed and configured to work with Minikube.
3. [Helm](https://helm.sh/3) \(2.16.0 or greater\). Helm 3 has not been tested yet.

### 1. Set up Minikube

Start Minikube. Note the minimum cpu and memory below:

```bash
minikube start --cpus=3 --memory=4096 --kubernetes-version='v1.15.5'
```

Set up your Feast environmental variables

```bash
export FEAST_IP=$(minikube ip)
export FEAST_CORE_URL=${FEAST_IP}:32090
export FEAST_SERVING_URL=${FEAST_IP}:32091
```

### 2. Install Feast with Helm

Clone the [Feast repository](https://github.com/gojek/feast/) and navigate to the `charts` sub-directory:

```bash
git clone https://github.com/gojek/feast.git && \
cd feast && export FEAST_HOME_DIR=$(pwd) && \
cd infra/charts/feast
```

Copy the `values-demo.yaml` file for your installation:

```bash
cp values-demo.yaml my-feast-values.yaml
```

Update all occurrences of the domain `feast.example.com` inside of `my-feast-values.yaml` with your Minikube IP. This is to allow external access to the services in the cluster. You can find your Minikube IP by running the following command `minikube ip`, or simply replace the text from the command line:

```bash
sed -i "s/feast.example.com/${FEAST_IP}/g" my-feast-values.yaml
```

Install Tiller:

```bash
helm init
```

Install the Feast Helm chart:

```bash
helm install --name feast -f my-feast-values.yaml .
```

Ensure that the system comes online. This will take a few minutes

```bash
watch kubectl get pods
```

```bash
NAME                                           READY   STATUS      RESTARTS   AGE
pod/feast-feast-core-666fd46db4-l58l6          1/1     Running     0          5m
pod/feast-feast-serving-online-84d99ddcbd      1/1     Running     0          6m
pod/feast-kafka-0                              1/1     Running     0          3m
pod/feast-kafka-1                              1/1     Running     0          4m
pod/feast-kafka-2                              1/1     Running     0          4m
pod/feast-postgresql-0                         1/1     Running     0          5m
pod/feast-redis-master-0                       1/1     Running     0          5m
pod/feast-zookeeper-0                          1/1     Running     0          5m
pod/feast-zookeeper-1                          1/1     Running     0          5m
pod/feast-zookeeper-2                          1/1     Running     0          5m
```

### 3. Connect to Feast with the Python SDK

Install the Python SDK using pip:

```bash
pip install -e ${FEAST_HOME_DIR}/sdk/python
```

Configure the Feast Python SDK:

```bash
feast config set core_url ${FEAST_CORE_URL}
feast config set serving_url ${FEAST_SERVING_URL}
```

That's it! You can now start to use Feast!

## Google Kubernetes Engine

### Overview <a id="m_5245424069798496115gmail-overview-1"></a>

This guide will install Feast into a Kubernetes cluster on GCP. It assumes that all of your services will run within a single K8s cluster. Once Feast is installed you will be able to:

* Define and register features.
* Load feature data from both batch and streaming sources.
* Retrieve features for model training.
* Retrieve features for online serving.

{% hint style="info" %}
This guide requires [Google Cloud Platform](https://cloud.google.com/) for installation.

* [BigQuery](https://cloud.google.com/bigquery/) is used for storing historical features.
* [Cloud Dataflow](https://cloud.google.com/dataflow/) is used for running data ingestion jobs.
* [Google Cloud Storage](https://cloud.google.com/storage/) is used for intermediate data storage.
{% endhint %}

### 0. Requirements <a id="m_5245424069798496115gmail-requirements"></a>

1. [Google Cloud SDK ](https://cloud.google.com/sdk/install)installed, authenticated, and configured to the project you will use.
2. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) installed.
3. [Helm](https://helm.sh/3) \(2.16.0 or greater\) installed on your local machine with Tiller installed in your cluster. Helm 3 has not been tested yet.

### 1. Set up GCP

First define the environmental variables that we will use throughout this installation. Please customize these to reflect your environment.

```bash
export FEAST_GCP_PROJECT_ID=my-gcp-project
export FEAST_GCP_REGION=us-central1
export FEAST_GCP_ZONE=us-central1-a
export FEAST_BIGQUERY_DATASET_ID=feast
export FEAST_GCS_BUCKET=${FEAST_GCP_PROJECT_ID}_feast_bucket
export FEAST_GKE_CLUSTER_NAME=feast
export FEAST_S_ACCOUNT_NAME=feast-sa
```

Create a Google Cloud Storage bucket for Feast to stage data during exports:

```bash
gsutil mb gs://${FEAST_GCS_BUCKET}
```

Create a BigQuery dataset for storing historical features:

```bash
bq mk ${FEAST_BIGQUERY_DATASET_ID}
```

Create the service account that Feast will run as:

```bash
gcloud iam service-accounts create ${FEAST_SERVICE_ACCOUNT_NAME}

gcloud projects add-iam-policy-binding ${FEAST_GCP_PROJECT_ID} \
  --member serviceAccount:${FEAST_S_ACCOUNT_NAME}@${FEAST_GCP_PROJECT_ID}.iam.gserviceaccount.com \
  --role roles/editor

gcloud iam service-accounts keys create key.json --iam-account \
${FEAST_S_ACCOUNT_NAME}@${FEAST_GCP_PROJECT_ID}.iam.gserviceaccount.com
```

Ensure that [Dataflow API is enabled](https://console.cloud.google.com/apis/api/dataflow.googleapis.com/overview):

```bash
gcloud services enable dataflow.googleapis.com
```

### 2. Set up a Kubernetes \(GKE\) cluster

{% hint style="warning" %}
Provisioning a GKE cluster can expose your services publicly. This guide does not cover securing access to the cluster.
{% endhint %}

Create a GKE cluster:

```bash
gcloud container clusters create ${FEAST_GKE_CLUSTER_NAME} \
    --machine-type n1-standard-4
```

Create a secret in the GKE cluster based on your local key `key.json`:

```bash
kubectl create secret generic feast-gcp-service-account --from-file=key.json
```

For this guide we will use `NodePort` for exposing Feast services. In order to do so, we must find an internal IP of at least one GKE node.

```bash
export FEAST_IP=$(kubectl describe nodes | grep ExternalIP | awk '{print $2}' | head -n 1)
export FEAST_CORE_URL=${FEAST_IP}:32090
export FEAST_ONLINE_SERVING_URL=${FEAST_IP}:32091
export FEAST_BATCH_SERVING_URL=${FEAST_IP}:32092
```

Confirm that you are able to access this node \(please make sure that no firewall rules are preventing access to these ports\):

```bash
ping $FEAST_IP
```

```bash
PING 10.123.114.11 (10.203.164.22) 56(84) bytes of data.
64 bytes from 10.123.114.11: icmp_seq=1 ttl=63 time=54.2 ms
64 bytes from 10.123.114.11: icmp_seq=2 ttl=63 time=51.2 ms
```

### 3. Set up Helm

Run the following command to provide Tiller with authorization to install Feast:

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: tiller
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: tiller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
  - kind: ServiceAccount
    name: tiller
    namespace: kube-system
EOF
```

Install Tiller:

```bash
helm init --service-account tiller
```

### 4. Install Feast with Helm

Clone the [Feast repository](https://github.com/gojek/feast/) and navigate to the `charts` sub-directory:

```bash
git clone https://github.com/gojek/feast.git && cd feast && \
git checkout 0.3-dev && \
export FEAST_HOME_DIR=$(pwd) && \
cd infra/charts/feast
```

Make a copy of the Helm `values.yaml` so that it can be customized for your Feast deployment:

```bash
cp values.yaml my-feast-values.yaml
```

Update `my-feast-values.yaml` based on your GCP and GKE environment.

* Required fields are paired with comments which indicate whether they need to be replaced.
* All occurrences of `feast.example.com` should be replaced with either your domain name or the IP stored in `$FEAST_IP`.

Install the Feast Helm chart:

```bash
helm install --name feast -f my-feast-values.yaml .
```

Ensure that the system comes online. This will take a few minutes

```bash
watch kubectl get pods
```

```bash
NAME                                           READY   STATUS      RESTARTS   AGE
pod/feast-feast-core-666fd46db4-l58l6          1/1     Running     0          5m
pod/feast-feast-serving-online-84d99ddcbd      1/1     Running     0          6m
pod/feast-kafka-0                              1/1     Running     0          3m
pod/feast-kafka-1                              1/1     Running     0          4m
pod/feast-kafka-2                              1/1     Running     0          4m
pod/feast-postgresql-0                         1/1     Running     0          5m
pod/feast-redis-master-0                       1/1     Running     0          5m
pod/feast-zookeeper-0                          1/1     Running     0          5m
pod/feast-zookeeper-1                          1/1     Running     0          5m
pod/feast-zookeeper-2                          1/1     Running     0          5m
```

### 5. Connect to Feast with the Python SDK

Install the Python SDK using pip:

```bash
pip install -e ${FEAST_HOME_DIR}/sdk/python
```

Configure the Feast Python SDK:

```bash
feast config set core_url ${FEAST_CORE_URL}
feast config set serving_url ${FEAST_ONLINE_SERVING_URL}
```

That's it! You can now start to use Feast!

