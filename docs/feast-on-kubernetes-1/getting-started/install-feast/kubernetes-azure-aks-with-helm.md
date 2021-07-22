# Azure AKS \(with Helm\)

## Overview

This guide installs Feast on Azure Kubernetes cluster \(known as AKS\), and ensures the following services are running:

* Feast Core
* Feast Online Serving
* Postgres
* Redis
* Spark
* Kafka
* Feast Jupyter \(Optional\)
* Prometheus \(Optional\)

## 1. Requirements

1. Install and configure [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
2. Install and configure [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
3. Install [Helm 3](https://helm.sh/)

## 2. Preparation

Create an AKS cluster with Azure CLI. The detailed steps can be found [here](https://docs.microsoft.com/en-us/azure/aks/kubernetes-walkthrough), and a high-level walk through includes:

```bash
az group create --name myResourceGroup  --location eastus
az acr create --resource-group myResourceGroup  --name feast-AKS-ACR --sku Basic
az aks create -g myResourceGroup  -n feast-AKS --location eastus --attach-acr feast-AKS-ACR --generate-ssh-keys

az aks install-cli
az aks get-credentials --resource-group myResourceGroup  --name  feast-AKS
```

Add the Feast Helm repository and download the latest charts:

```bash
helm version # make sure you have the latest Helm installed
helm repo add feast-charts https://feast-helm-charts.storage.googleapis.com
helm repo update
```

Feast includes a Helm chart that installs all necessary components to run Feast Core, Feast Online Serving, and an example Jupyter notebook.

Feast Core requires Postgres to run, which requires a secret to be set on Kubernetes:

```bash
kubectl create secret generic feast-postgresql --from-literal=postgresql-password=password
```

## 3. Feast installation

Install Feast using Helm. The pods may take a few minutes to initialize.

```bash
helm install feast-release feast-charts/feast
```

## 4. Spark operator installation

Follow the documentation [to install Spark operator on Kubernetes ](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator), and Feast documentation to [configure Spark roles](../../reference-1/feast-and-spark.md)

```bash
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator 
helm install my-release spark-operator/spark-operator  --set serviceAccounts.spark.name=spark --set image.tag=v1beta2-1.1.2-2.4.5
```

and ensure the service account used by Feast has permissions to manage Spark Application resources. This depends on your k8s setup, but typically you'd need to configure a Role and a RoleBinding like the one below:

```text
cat <<EOF | kubectl apply -f -
kind: Role
apiVersion: rbac.authorization.k8s.io/v1beta1
metadata:
  name: use-spark-operator
  namespace: <REPLACE ME>
rules:
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["create", "delete", "deletecollection", "get", "list", "update", "watch", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1beta1
kind: RoleBinding
metadata:
  name: use-spark-operator
  namespace: <REPLACE ME>
roleRef:
  kind: Role
  name: use-spark-operator
  apiGroup: rbac.authorization.k8s.io
subjects:
  - kind: ServiceAccount
    name: default
EOF
```

## 5. Use Jupyter to connect to Feast

After all the pods are in a `RUNNING` state, port-forward to the Jupyter Notebook Server in the cluster:

```bash
kubectl port-forward \
$(kubectl get pod -o custom-columns=:metadata.name | grep jupyter) 8888:8888
```

```text
Forwarding from 127.0.0.1:8888 -> 8888
Forwarding from [::1]:8888 -> 8888
```

You can now connect to the bundled Jupyter Notebook Server at `localhost:8888` and follow the example Jupyter notebook.

{% embed url="http://localhost:8888/tree?" caption="" %}

## 6. Environment variables

If you are running the [Minimal Ride Hailing Example](https://github.com/feast-dev/feast/blob/master/examples/minimal/minimal_ride_hailing.ipynb), you may want to make sure the following environment variables are correctly set:

```text
demo_data_location = "wasbs://<container_name>@<storage_account_name>.blob.core.windows.net/"
os.environ["FEAST_AZURE_BLOB_ACCOUNT_NAME"] = "<storage_account_name>"
os.environ["FEAST_AZURE_BLOB_ACCOUNT_ACCESS_KEY"] = <Insert your key here>
os.environ["FEAST_HISTORICAL_FEATURE_OUTPUT_LOCATION"] = "wasbs://<container_name>@<storage_account_name>.blob.core.windows.net/out/"
os.environ["FEAST_SPARK_STAGING_LOCATION"] = "wasbs://<container_name>@<storage_account_name>.blob.core.windows.net/artifacts/"
os.environ["FEAST_SPARK_LAUNCHER"] = "k8s"
os.environ["FEAST_SPARK_K8S_NAMESPACE"] = "default"
os.environ["FEAST_HISTORICAL_FEATURE_OUTPUT_FORMAT"] = "parquet"
os.environ["FEAST_REDIS_HOST"] = "feast-release-redis-master.default.svc.cluster.local"
os.environ["DEMO_KAFKA_BROKERS"] = "feast-release-kafka.default.svc.cluster.local:9092"
```

## 7. Further Reading

* [Feast Concepts](../../concepts/overview.md)
* [Feast Examples/Tutorials](https://github.com/feast-dev/feast/tree/master/examples)
* [Feast Helm Chart Documentation](https://github.com/feast-dev/feast/blob/master/infra/charts/feast/README.md)
* [Configuring Feast components](../../reference-1/configuration-reference.md)
* [Feast and Spark](../../reference-1/feast-and-spark.md)

