# Kubernetes \(GKE\)

### Overview <a id="m_5245424069798496115gmail-overview-1"></a>

This guide will install Feast into a Kubernetes cluster on Google Cloud Platform. It assumes that all of your services will run within a single Kubernetes cluster. Once Feast is installed you will be able to:

* Define and register features.
* Load feature data from both batch and streaming sources.
* Retrieve features for model training.
* Retrieve features for online serving.

{% hint style="info" %}
This guide requires [Google Cloud Platform](https://cloud.google.com/) for installation.

* [BigQuery](https://cloud.google.com/bigquery/) is used for storing historical features.
* [Redis](https://redis.io/) is used for storing online features.
* [Google Cloud Storage](https://cloud.google.com/storage/) is used for intermediate data storage.
* [Apache Beam](https://beam.apache.org/) \([DirectRunner](https://beam.apache.org/documentation/runners/direct/)\) is used for ingesting data into stores.
{% endhint %}

## 0. Requirements

1. [Google Cloud SDK ](https://cloud.google.com/sdk/install)installed, authenticated, and configured to the project you will use.
2. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) installed and configured.
3. [Helm](https://helm.sh/) \(2.16.0 or greater\) installed. Helm 3 has not been tested yet.

## 1. Set up Google Cloud Platform

Create a Google Cloud Storage bucket for Feast to stage batch data exports:

```bash
gsutil mb gs://my-feast-staging-bucket
```

Create a service account for Feast to use:

```bash
gcloud iam service-accounts create feast-service-account

gcloud projects add-iam-policy-binding my-gcp-project \
  --member serviceAccount:feast-service-account@my-gcp-project.iam.gserviceaccount.com \
  --role roles/editor
  
# Please use "credentials.json" as the file name
gcloud iam service-accounts keys create credentials.json --iam-account \
feast-service-account@my-gcp-project.iam.gserviceaccount.com
```

## 2. Set up a Kubernetes \(GKE\) cluster

Create a Kubernetes cluster:

```bash
gcloud container clusters create feast-cluster \
    --machine-type n1-standard-4 \
    --zone us-central1-a \
    --scopes=bigquery,storage-rw,compute-ro
```

Create a secret in the GKE cluster from the service account `credentials.json`:

```bash
kubectl create secret generic feast-gcp-service-account \
  --from-file=credentials.json
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

Add the Feast Helm repository and download the latest charts

```text
helm repo add feast-charts https://feast-charts.storage.googleapis.com
helm repo update
```

Create a password for PostgreSQL

```text
kubectl create secret generic feast-postgresql \
    --from-literal=postgresql-password=password
```

Create a `values.yml` file to configure your Feast deployment

```bash
curl https://raw.githubusercontent.com/feast-dev/feast/master/infra/charts/feast/values-batch-serving.yaml \
> values.yaml
```

Update `values.yaml` based on your GCP and GKE environment. Minimally the following values must be set

* `project_id` is your GCP project id
* `dataset_id` is your BigQuery dataset id. This dataset will be created by Feast if it does not exist.
* `staging_location` is the GCS bucket used for staging that you created in this guide.

Install the Feast Helm chart:

```bash
helm install --name myrelease -f values.yaml feast-charts/feast
```

Wait for the system to come online

```bash
watch kubectl get pods
```

This may take a few minutes

```bash
NAME                                                    READY   STATUS    RESTARTS   AGE
myrelease-feast-batch-serving-5674f6fb4c-bzsk8      1/1     Running   2          14m
myrelease-feast-core-7547b455f4-fvppz               1/1     Running   1          14m
myrelease-feast-jupyter-9b7c4b8fd-bdcbv             1/1     Running   0          14m
myrelease-feast-online-serving-7884578db5-57xwv     1/1     Running   1          14m
myrelease-grafana-c976d598c-jbgkh                   1/1     Running   0          14m
myrelease-kafka-0                                   1/1     Running   0          14m
myrelease-kafka-1                                   1/1     Running   0          13m
myrelease-kafka-2                                   1/1     Running   0          14m
myrelease-postgresql-0                              1/1     Running   0          14m
myrelease-redis-master-0                            1/1     Running   0          14m
myrelease-redis-slave-0                             1/1     Running   0          14m
myrelease-redis-slave-1                             1/1     Running   0          14m
myrelease-zookeeper-0                               1/1     Running   0          14m
myrelease-zookeeper-1                               1/1     Running   0          14m
myrelease-zookeeper-2                               1/1     Running   0          13m
```

## 5. Connect to Feast using Jupyter

Once the pods are all running we can connect to the Jupyter notebook server running in the cluster

```bash
kubectl port-forward \
$(kubectl get pod -o custom-columns=:metadata.name | grep jupyter) 8888:8888
```

```text
Forwarding from 127.0.0.1:8888 -> 8888
Forwarding from [::1]:8888 -> 8888
```

You should now be able to open the Jupyter notebook at [http://localhost:8888/](http://localhost:8888/)

From within the Jupyter Notebook you can now clone the Feast repository

```text
git clone https://github.com/feast-dev/feast 
```

Please try out our [examples](https://github.com/feast-dev/feast/blob/master/examples/) \(remember to check out the latest stable version\).

