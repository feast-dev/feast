# Kubernetes \(with Helm\)

## Overview <a id="m_5245424069798496115gmail-overview-1"></a>

This guide installs Feast on an existing Kubernetes cluster, and ensures the following services are running:

* Feast Core
* Feast Online Serving
* Postgres
* Redis
* Feast Jupyter \(Optional\)
* Prometheus \(Optional\)

## 1. Requirements

1. Install and configure [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
2. Install [Helm 3](https://helm.sh/)

## 2. Preparation

Add the Feast Helm repository and download the latest charts:

```text
helm repo add feast-charts https://feast-helm-charts.storage.googleapis.com
helm repo update
```

Feast includes a Helm chart that installs all necessary components to run Feast Core, Feast Online Serving, and an example Jupyter notebook.

Feast Core requires Postgres to run, which requires a secret to be set on Kubernetes:

```bash
kubectl create secret generic feast-postgresql --from-literal=postgresql-password=password
```

## 3. Installation

Install Feast using Helm. The pods may take a few minutes to initialize.

```bash
helm install feast-release feast-charts/feast
```

## 4. Use Jupyter to connect to Feast

After all the pods are in a `RUNNING` state, port-forward to the Jupyter Notebook Server in the cluster:

```bash
kubectl port-forward \
$(kubectl get pod -l app=feast-jupyter -o custom-columns=:metadata.name) 8888:8888
```

```text
Forwarding from 127.0.0.1:8888 -> 8888
Forwarding from [::1]:8888 -> 8888
```

You can now connect to the bundled Jupyter Notebook Server at `localhost:8888` and follow the example Jupyter notebook.

{% embed url="http://localhost:8888/tree?" caption="" %}

## 5. Further Reading

* [Feast Concepts](../../concepts/overview.md)
* [Feast Examples/Tutorials](https://github.com/feast-dev/feast/tree/master/examples)
* [Feast Helm Chart Documentation](https://github.com/feast-dev/feast/blob/master/infra/charts/feast/README.md)
* [Configuring Feast components](../../reference-1/configuration-reference.md)
* [Feast and Spark](../../reference-1/feast-and-spark.md)

