# Amazon EKS \(with Terraform\)

### Overview

This guide installs Feast on AWS using our [reference Terraform configuration](https://github.com/feast-dev/feast/tree/master/infra/terraform/aws).

{% hint style="info" %}
The Terraform configuration used here is a greenfield installation that neither assumes anything about, nor integrates with, existing resources in your AWS account. The Terraform configuration presents an easy way to get started, but you may want to customize this set up before using Feast in production.
{% endhint %}

This Terraform configuration creates the following resources:

* Kubernetes cluster on Amazon EKS \(3x r3.large nodes\)
* Kafka managed by Amazon MSK \(2x kafka.t3.small nodes\)
* Postgres database for Feast metadata, using serverless Aurora \(min capacity: 2\)
* Redis cluster, using Amazon Elasticache \(1x cache.t2.micro\)
* Amazon EMR cluster to run Spark \(3x spot m4.xlarge\)
* Staging S3 bucket to store temporary data

![](../../../.gitbook/assets/feast-on-aws-3-%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%283%29.png)

### 1. Requirements

* Create an AWS account and [configure credentials locally](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
* Install [Terraform](https://www.terraform.io/) &gt; = 0.12 \(tested with 0.13.3\)
* Install [Helm](https://helm.sh/docs/intro/install/) \(tested with v3.3.4\)

### 2. Configure Terraform 

Create a `.tfvars` file under`feast/infra/terraform/aws`. Name the file. In our example, we use `my_feast.tfvars`. You can see the full list of configuration variables in `variables.tf`. At a minimum, you need to set `name_prefix` and an AWS region:

{% code title="my\_feast.tfvars" %}
```typescript
name_prefix = "my-feast"
region      = "us-east-1"
```
{% endcode %}

### 3. Apply

After completing the configuration, initialize Terraform and apply:

```bash
$ cd feast/infra/terraform/aws
$ terraform init
$ terraform apply -var-file=my_feast.tfvars
```

Starting may take a minute.  A kubectl configuration file is also created in this directory, and the file's name will start with `kubeconfig_` and end with a random suffix.

### 4. Connect to Feast using Jupyter

After all pods are running, connect to the Jupyter Notebook Server running in the cluster.

To connect to the remote Feast server you just created, forward a port from the remote k8s cluster to your local machine. Replace `kubeconfig_XXXXXXX` below with the kubeconfig file name Terraform generates for you. 

```bash
KUBECONFIG=kubeconfig_XXXXXXX kubectl port-forward \
$(kubectl get pod -o custom-columns=:metadata.name | grep jupyter) 8888:8888
```

```text
Forwarding from 127.0.0.1:8888 -> 8888
Forwarding from [::1]:8888 -> 8888
```

You can now connect to the bundled Jupyter Notebook Server at `localhost:8888` and follow the example Jupyter notebook.

