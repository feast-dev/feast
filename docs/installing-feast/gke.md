# Google Kubernetes Engine \(GKE\)

### Overview <a id="m_5245424069798496115gmail-overview-1"></a>

This guide will install Feast into a Kubernetes cluster on GCP. It assumes that all of your services will run within a single Kubernetes cluster. Once Feast is installed you will be able to:

* Define and register features.
* Load feature data from both batch and streaming sources.
* Retrieve features for model training.
* Retrieve features for online serving.

{% hint style="info" %}
This guide requires [Google Cloud Platform](https://cloud.google.com/) for installation.

* [BigQuery](https://cloud.google.com/bigquery/) is used for storing historical features.
* [Google Cloud Storage](https://cloud.google.com/storage/) is used for intermediate data storage.
{% endhint %}

## 0. Requirements

1. [Google Cloud SDK ](https://cloud.google.com/sdk/install)installed, authenticated, and configured to the project you will use.
2. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) installed.
3. [Helm](https://helm.sh/3) \(2.16.0 or greater\) installed on your local machine with Tiller installed in your cluster. Helm 3 has not been tested yet.

## 1. Set up GCP

First define the environmental variables that we will use throughout this installation. Please customize these to reflect your environment.

```bash
export FEAST_GCP_PROJECT_ID=my-gcp-project
export FEAST_GCP_REGION=us-central1
export FEAST_GCP_ZONE=us-central1-a
export FEAST_BIGQUERY_DATASET_ID=feast
export FEAST_GCS_BUCKET=${FEAST_GCP_PROJECT_ID}_feast_bucket
export FEAST_GKE_CLUSTER_NAME=feast
export FEAST_SERVICE_ACCOUNT_NAME=feast-sa
```

Create a Google Cloud Storage bucket for Feast to stage batch data exports:

```bash
gsutil mb gs://${FEAST_GCS_BUCKET}
```

Create the service account that Feast will run as:

```bash
gcloud iam service-accounts create ${FEAST_SERVICE_ACCOUNT_NAME}

gcloud projects add-iam-policy-binding ${FEAST_GCP_PROJECT_ID} \
  --member serviceAccount:${FEAST_SERVICE_ACCOUNT_NAME}@${FEAST_GCP_PROJECT_ID}.iam.gserviceaccount.com \
  --role roles/editor

gcloud iam service-accounts keys create key.json --iam-account \
${FEAST_SERVICE_ACCOUNT_NAME}@${FEAST_GCP_PROJECT_ID}.iam.gserviceaccount.com
```

## 2. Set up a Kubernetes \(GKE\) cluster

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

For this guide we will use `NodePort` for exposing Feast services. In order to do so, we must find an External IP of at least one GKE node. This should be a public IP.

```bash
export FEAST_IP=$(kubectl describe nodes | grep ExternalIP | awk '{print $2}' | head -n 1)
export FEAST_CORE_URL=${FEAST_IP}:32090
export FEAST_ONLINE_SERVING_URL=${FEAST_IP}:32091
export FEAST_BATCH_SERVING_URL=${FEAST_IP}:32092
```

Add firewall rules to open up ports on your Google Cloud Platform project:

```bash
gcloud compute firewall-rules create feast-core-port --allow tcp:32090
gcloud compute firewall-rules create feast-online-port --allow tcp:32091
gcloud compute firewall-rules create feast-batch-port --allow tcp:32092
gcloud compute firewall-rules create feast-redis-port --allow tcp:32101
gcloud compute firewall-rules create feast-kafka-ports --allow tcp:31090-31095
```

## 3. Set up Helm

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

## 4. Install Feast with Helm

Clone the [Feast repository](https://github.com/gojek/feast/) and navigate to the `charts` sub-directory:

```bash
git clone https://github.com/gojek/feast.git && cd feast && \
export FEAST_HOME_DIR=$(pwd) && \
cd infra/charts/feast
```

Make a copy of the Helm `values.yaml` so that it can be customized for your Feast deployment:

```bash
cp values.yaml my-feast-values.yaml
```

Update `my-feast-values.yaml` based on your GCP and GKE environment.

* Required fields are paired with comments which indicate whether they need to be replaced.
* All occurrences of `EXTERNAL_IP` should be replaced with either a domain pointing to a load balancer for the cluster or the IP stored in `$FEAST_IP`.
* Replace all occurrences of `YOUR_BUCKET_NAME` with your bucket name stored in `$FEAST_GCS_BUCKET`
* Change `feast-serving-batch.store.yaml.bigquery_config.project_id` to your GCP project Id.
* Change `feast-serving-batch.store.yaml.bigquery_config.dataset_id` to the BigQuery dataset that Feast should use.

Install the Feast Helm chart:

```bash
helm install --name feast -f my-feast-values.yaml .
```

Ensure that the system comes online. This will take a few minutes. 

```bash
kubectl get pods
```

There may be pod restarts while waiting for Kafka to come online.

```bash
NAME                                           READY   STATUS      RESTARTS   AGE
pod/feast-feast-core-666fd46db4-l58l6          1/1     Running     2          5m
pod/feast-feast-serving-online-84d99ddcbd      1/1     Running     3          6m
pod/feast-kafka-0                              1/1     Running     0          3m
pod/feast-kafka-1                              1/1     Running     0          4m
pod/feast-kafka-2                              1/1     Running     0          4m
pod/feast-postgresql-0                         1/1     Running     0          5m
pod/feast-redis-master-0                       1/1     Running     0          5m
pod/feast-zookeeper-0                          1/1     Running     0          5m
pod/feast-zookeeper-1                          1/1     Running     0          5m
pod/feast-zookeeper-2                          1/1     Running     0          5m
```

## 5. Connect to Feast with the Python SDK

Install the Python SDK using pip:

```bash
pip install feast
```

Configure the Feast Python SDK:

```bash
feast config set core_url ${FEAST_CORE_URL}
feast config set serving_url ${FEAST_ONLINE_SERVING_URL}
```

Test whether you are able to connect to Feast Core

```text
feast projects list
```

Should print an empty list:

```text
NAME
```

That's it! You can now start to use Feast!

Please see our [examples](https://github.com/gojek/feast/blob/master/examples/) to get started.

