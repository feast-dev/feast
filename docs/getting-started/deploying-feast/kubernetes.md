# Kubernetes \(GKE\)

### Overview <a id="m_5245424069798496115gmail-overview-1"></a>

This guide will install Feast into a Kubernetes cluster on Google Cloud Platform. It assumes that all of your services will run within a single Kubernetes cluster. 

Kubernetes deployment is recommended when deploying Feast as a shared service for production workloads.

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
3. [Helm ](https://helm.sh/)3 installed.

## 1. Set up Google Cloud Platform

{% hint style="info" %}
Historical Serving currently requires Google Cloud Platform \(GCP\) to function, specifically a Service Account with access to Google Cloud Storage \(GCS\) and BigQuery. 
{% endhint %}

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

Create a Google Cloud Storage bucket for Feast to stage batch data exports:

```bash
gsutil mb gs://my-feast-staging-bucket
```

Create a BigQuery dataset for Feast to store historical data:

```bash
bq --location=US mk --dataset my_project:feast
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

## 3. Install Feast with Helm

Add the Feast Helm repository and download the latest charts:

```text
helm repo add feast-charts https://feast-charts.storage.googleapis.com
helm repo update
```

Create secret to store the password for PostgreSQL:

```text
kubectl create secret generic feast-postgresql \
    --from-literal=postgresql-password=password
```

Create a `values.yml` file to configure your Feast deployment

```bash
curl https://raw.githubusercontent.com/feast-dev/feast/master/infra/charts/feast/values-batch-serving.yaml \
> values.yaml
```

Update `application-values.yml`in `values.yaml` to configure Feast based on your GCP and GKE environment. Minimally the following properties must be set under `feast.stores[].config`:

| Property | Description |
| :--- | :--- |
| `project_id` | This is your GCP Project Id. |
| `dataset_id` | This is your BigQuery dataset id.  |
| `staging_location` | This is the GCS bucket used for staging data being loaded into BigQuery. |

Install the Feast Helm chart to deploy Feast:

```bash
helm install --name myrelease -f values.yaml feast-charts/feast
```

Wait for the Feast pods to start running and become become ready:

```bash
watch kubectl get pods
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
git clone --branch v0.7.0-rc.1 https://github.com/feast-dev/feast 
```

Please try out our [examples](https://github.com/feast-dev/feast/blob/master/examples/).

## 6. Further Reading

* [Feast Concepts](../../user-guide/overview.md)
* [Feast Examples/Tutorials](https://github.com/feast-dev/feast/tree/master/examples)
* [Feast Helm Chart Documentation](https://github.com/feast-dev/feast/blob/master/infra/charts/feast/README.md)
* [Configuring Feast components](../../reference/configuration-reference.md)



