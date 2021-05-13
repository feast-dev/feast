# Google Cloud GKE \(with Terraform\)

### Overview

This guide installs Feast on GKE using our [reference Terraform configuration](https://github.com/feast-dev/feast/tree/master/infra/terraform/gcp).

{% hint style="info" %}
The Terraform configuration used here is a greenfield installation that neither assumes anything about, nor integrates with, existing resources in your GCP account. The Terraform configuration presents an easy way to get started, but you may want to customize this set up before using Feast in production.
{% endhint %}

This Terraform configuration creates the following resources:

* GKE cluster
* Feast services running on GKE
* Google Memorystore \(Redis\) as online store
* Dataproc cluster
* Kafka running on GKE, exposed to the dataproc cluster via internal load balancer

### 1. Requirements

* Install [Terraform](https://www.terraform.io/) &gt; = 0.12 \(tested with 0.13.3\)
* Install [Helm](https://helm.sh/docs/intro/install/) \(tested with v3.3.4\)
* GCP [authentication](https://cloud.google.com/docs/authentication) and sufficient [privilege](https://cloud.google.com/iam/docs/understanding-roles) to create the resources listed above.

### 2. Configure Terraform 

Create a `.tfvars` file under`feast/infra/terraform/gcp`. Name the file. In our example, we use `my_feast.tfvars`. You can see the full list of configuration variables in `variables.tf`. Sample configurations are provided below:

{% code title="my\_feast.tfvars" %}
```typescript
gcp_project_name        = "kf-feast"
name_prefix             = "feast-0-8"
region                  = "asia-east1"
gke_machine_type        = "n1-standard-2"
network                 = "default"
subnetwork              = "default"
dataproc_staging_bucket = "feast-dataproc"
```
{% endcode %}

### 3. Apply

After completing the configuration, initialize Terraform and apply:

```bash
$ cd feast/infra/terraform/gcp
$ terraform init
$ terraform apply -var-file=my_feast.tfvars
```



