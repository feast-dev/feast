# Kubernetes

### Overview <a id="m_5245424069798496115gmail-overview-1"></a>

This guide will install Feast on an existing Kubernetes cluster. At the end of the guide you will have the following services running:

* Feast Core
* Feast Serving
* Postgres
* Redis
* Feast Jupyter
* Prometheus

This guide will also contain instructions on how to customize the installation for different production use cases.

## 0. Requirements

1. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) installed and configured.
2. [Helm ](https://helm.sh/)3 installed.

## 1. Preparation

Add the Feast Helm repository and download the latest charts:

```text
helm repo add feast-charts https://feast-charts.storage.googleapis.com
helm repo update
```

The charts bundle all the necessary dependencies to run both `Feast Core` and `Feast Serving`, as well as going through the example Jupyter notebook.

As `Feast Core` requires Postgres to run, we will first create a Kubernetes secret for the credential. 

```bash
kubectl create secret generic feast-postgresql --from-literal=postgresql-password=password
```

## 2. Installation

Install Feast using helm. It will take some time before all the pods are ready. Feast online serving will keep restart until Feast core is ready.

```bash
helm install feast-release feast-charts/feast
```

## 3. Connect to Feast using Jupyter

Once the pods are all running we can connect to the Jupyter notebook server running in the cluster

```bash
kubectl port-forward \
$(kubectl get pod -o custom-columns=:metadata.name | grep jupyter) 8888:8888
```

```text
Forwarding from 127.0.0.1:8888 -> 8888
Forwarding from [::1]:8888 -> 8888
```

You should be able to connect at `localhost:8888` to the bundled Jupyter Notebook Server with example notebooks.

{% embed url="http://localhost:8888/tree?" caption="" %}

## 4. Further Reading

* [Feast Concepts](../../concepts/overview.md)
* [Feast Examples/Tutorials](https://github.com/feast-dev/feast/tree/master/examples)
* [Feast Helm Chart Documentation](https://github.com/feast-dev/feast/blob/master/infra/charts/feast/README.md)
* [Configuring Feast components](../../reference/configuration-reference.md)



