# Feast on Kubernetes

This page covers deploying Feast on Kubernetes, including the Feast Operator and feature servers.

## Overview

Kubernetes is a common target environment for running Feast in production. You can use Kubernetes to:

1. Run Feast feature servers for online feature retrieval.
2. Run scheduled and ad-hoc jobs (e.g. materialization jobs) as Kubernetes Jobs.
3. Operate Feast components using Kubernetes-native primitives.

## Feast Operator

To deploy Feast components on Kubernetes, use the included [feast-operator](../../infra/feast-operator).

For first-time Operator users, it may be a good exercise to try the [Feast Operator Quickstart](../../examples/operator-quickstart). The quickstart demonstrates some of the Operator's built-in features, e.g. git repos, `feast apply` jobs, etc.

## Deploy Feast feature servers on Kubernetes

{% embed url="https://www.youtube.com/playlist?list=PLPzVNzik7rsAN-amQLZckd0so3cIr7blX" %}

**Basic steps**

1. Install [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
2. Install the Operator

Install the latest release:

```sh
kubectl apply -f https://raw.githubusercontent.com/feast-dev/feast/refs/heads/stable/infra/feast-operator/dist/install.yaml
```

OR, install a specific version:

```sh
kubectl apply -f https://raw.githubusercontent.com/feast-dev/feast/refs/tags/<version>/infra/feast-operator/dist/install.yaml
```

3. Deploy a Feature Store

```sh
kubectl apply -f https://raw.githubusercontent.com/feast-dev/feast/refs/heads/stable/infra/feast-operator/config/samples/v1_featurestore.yaml
```

Verify the status:

```sh
$ kubectl get feast
NAME     STATUS   AGE
sample   Ready    2m21s
```

The above will install a simple [FeatureStore CR](../../infra/feast-operator/docs/api/markdown/ref.md) like the following. By default, it will run the [Online Store feature server](../reference/feature-servers/python-feature-server.md):

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample
spec:
  feastProject: my_project
```

> _More advanced FeatureStore CR examples can be found in the feast-operator [samples directory](../../infra/feast-operator/config/samples)._

{% hint style="success" %}
Important note: Scaling a Feature Store Deployment should only be done if the configured data store(s) will support it.

Please check the how-to guide for some specific recommendations on [how to scale Feast](./scaling-feast.md).
{% endhint %}
