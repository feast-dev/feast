# Azure AKS \(with Terraform\)

## Overview

This guide installs Feast on Azure using our [reference Terraform configuration](https://github.com/feast-dev/feast/tree/master/infra/terraform/azure).

{% hint style="info" %}
The Terraform configuration used here is a greenfield installation that neither assumes anything about, nor integrates with, existing resources in your Azure account. The Terraform configuration presents an easy way to get started, but you may want to customize this set up before using Feast in production.
{% endhint %}

This Terraform configuration creates the following resources:

* Kubernetes cluster on Azure AKS
* Kafka managed by HDInsight
* Postgres database for Feast metadata, running as a pod on AKS
* Redis cluster, using Azure Cache for Redis
* [spark-on-k8s-operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator) to run Spark
* Staging Azure blob storage container to store temporary data

## 1. Requirements

* Create an Azure account and [configure credentials locally](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
* Install [Terraform](https://www.terraform.io/) \(tested with 0.13.5\)
* Install [Helm](https://helm.sh/docs/intro/install/) \(tested with v3.4.2\)

## 2. Configure Terraform

Create a `.tfvars` file under`feast/infra/terraform/azure`. Name the file. In our example, we use `my_feast.tfvars`. You can see the full list of configuration variables in `variables.tf`. At a minimum, you need to set `name_prefix` and `resource_group`:

{% code title="my\_feast.tfvars" %}
```typescript
name_prefix = "feast"
resource_group = "Feast" # pre-existing resource group
```
{% endcode %}

## 3. Apply

After completing the configuration, initialize Terraform and apply:

```bash
$ cd feast/infra/terraform/azure
$ terraform init
$ terraform apply -var-file=my_feast.tfvars
```

## 4. Connect to Feast using Jupyter

After all pods are running, connect to the Jupyter Notebook Server running in the cluster.

To connect to the remote Feast server you just created, forward a port from the remote k8s cluster to your local machine.

```bash
kubectl port-forward $(kubectl get pod -o custom-columns=:metadata.name | grep jupyter) 8888:8888
```

```text
Forwarding from 127.0.0.1:8888 -> 8888
Forwarding from [::1]:8888 -> 8888
```

You can now connect to the bundled Jupyter Notebook Server at `localhost:8888` and follow the example Jupyter notebook.

