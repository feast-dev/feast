# Feast on Kubernetes

This page covers deploying Feast on Kubernetes, including the Feast Operator and feature servers.

## Overview

Kubernetes is a common target environment for running Feast in production. You can use Kubernetes to:

1. Run Feast feature servers for online feature retrieval.
2. Run scheduled and ad-hoc jobs (e.g. materialization jobs) as Kubernetes Jobs.
3. Operate Feast components using Kubernetes-native primitives.

{% hint style="info" %}
**Planning a production deployment?** See the [Feast Production Deployment Topologies](./production-deployment-topologies.md) guide for architecture diagrams, sample FeatureStore CRs, RBAC policies, infrastructure recommendations, and scaling best practices across Minimal, Standard, and Enterprise topologies.
{% endhint %}

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

## Upgrading the Operator

### OLM-managed installations

If the operator was installed via OLM, upgrades are handled
automatically. No manual steps are required — OLM recreates the operator Deployment
during the upgrade process.

### kubectl-managed installations

For most upgrades, re-running the install command is sufficient:

```sh
kubectl apply --server-side --force-conflicts -f https://raw.githubusercontent.com/feast-dev/feast/refs/heads/stable/infra/feast-operator/dist/install.yaml
```

#### One-time step: upgrading from versions before 0.61.0

Version 0.61.0 updated the operator Deployment's `spec.selector` to include the
`app.kubernetes.io/name: feast-operator` label, fixing a bug where the metrics service
could accidentally target pods from other operators in shared namespaces.

Because Kubernetes treats `spec.selector` as an immutable field, upgrading directly from
a pre-0.61.0 version with `kubectl apply` will fail with:

```
The Deployment "feast-operator-controller-manager" is invalid: spec.selector: Invalid value: ... field is immutable
```

To resolve this, delete the existing operator Deployment before applying the new manifest:

```sh
kubectl delete deployment feast-operator-controller-manager -n feast-operator-system --ignore-not-found=true
kubectl apply --server-side --force-conflicts -f https://raw.githubusercontent.com/feast-dev/feast/refs/heads/stable/infra/feast-operator/dist/install.yaml
```

This is only required once. Existing FeatureStore CRs and their managed workloads (feature
servers, registry, etc.) are not affected — the new operator pod will reconcile them
automatically on startup. Future upgrades from 0.61.0 onward will not require this step.

{% hint style="success" %}
**Scaling & High Availability:** The Feast Operator supports horizontal scaling via static replicas, HPA autoscaling, or external autoscalers like [KEDA](https://keda.sh). Scaling requires DB-backed persistence for all enabled services.

When scaling is enabled, the operator auto-injects soft pod anti-affinity and zone topology spread constraints for resilience. You can also configure a PodDisruptionBudget to protect against voluntary disruptions.

See the [Horizontal Scaling with the Feast Operator](./scaling-feast.md#horizontal-scaling-with-the-feast-operator) guide for configuration details, including [HA options](./scaling-feast.md#high-availability), or check the general recommendations on [how to scale Feast](./scaling-feast.md).
{% endhint %}

> _Sample scaling CRs are available at [`v1_featurestore_scaling_static.yaml`](../../infra/feast-operator/config/samples/v1_featurestore_scaling_static.yaml) and [`v1_featurestore_scaling_hpa.yaml`](../../infra/feast-operator/config/samples/v1_featurestore_scaling_hpa.yaml)._
