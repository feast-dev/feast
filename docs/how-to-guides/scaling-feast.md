# Scaling Feast

## Overview

Feast is designed to be easy to use and understand out of the box, with as few infrastructure dependencies as possible. However, there are components used by default that may not scale well. 
Since Feast is designed to be modular, it's possible to swap such components with more performant components, at the cost of Feast depending on additional infrastructure.


### Scaling Feast Registry

The default Feast [registry](../getting-started/concepts/registry.md) is a file-based registry. Any changes to the feature repo, or materializing data into the online store, results in a mutation to the registry.

However, there are inherent limitations with a file-based registry, since changing a single field in the registry requires re-writing the whole registry file.
With multiple concurrent writers, this presents a risk of data loss, or bottlenecks writes to the registry since all changes have to be serialized (e.g. when running materialization for multiple feature views or time ranges concurrently).

The recommended solution in this case is to use the [SQL based registry](../tutorials/using-scalable-registry.md), which allows concurrent, transactional, and fine-grained updates to the registry. This registry implementation requires access to an existing database (such as MySQL, Postgres, etc).

### Scaling Materialization

The default Feast materialization process is an in-memory process, which pulls data from the offline store before writing it to the online store.
However, this process does not scale for large data sets, since it's executed on a single-process.

Feast supports pluggable [Compute Engines](../getting-started/components/compute-engine.md), that allow the materialization process to be scaled up.
Aside from the local process, Feast supports a [Lambda-based materialization engine](https://rtd.feast.dev/en/master/#alpha-lambda-based-engine), and a [Bytewax-based materialization engine](https://rtd.feast.dev/en/master/#bytewax-engine).

Users may also be able to build an engine to scale up materialization using existing infrastructure in their organizations.

### Horizontal Scaling with the Feast Operator

When running Feast on Kubernetes with the [Feast Operator](./feast-on-kubernetes.md), you can horizontally scale the FeatureStore deployment using `spec.replicas` or HPA autoscaling. The FeatureStore CRD implements the Kubernetes [scale sub-resource](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#scale-subresource), so you can also use `kubectl scale`:

```bash
kubectl scale featurestore/my-feast --replicas=3
```

**Prerequisites:** Horizontal scaling requires **DB-backed persistence** for all enabled services (online store, offline store, and registry). File-based persistence (SQLite, DuckDB, `registry.db`) is incompatible with multiple replicas because these backends do not support concurrent access from multiple pods.

#### Static Replicas

Set a fixed number of replicas via `spec.replicas`:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-scaling
spec:
  feastProject: my_project
  replicas: 3
  services:
    onlineStore:
      persistence:
        store:
          type: postgres
          secretRef:
            name: feast-data-stores
    registry:
      local:
        persistence:
          store:
            type: sql
            secretRef:
              name: feast-data-stores
```

#### Autoscaling with HPA

Configure a HorizontalPodAutoscaler to dynamically scale based on metrics. HPA autoscaling is configured under `services.scaling.autoscaling` and is mutually exclusive with `spec.replicas > 1`:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-autoscaling
spec:
  feastProject: my_project
  services:
    scaling:
      autoscaling:
        minReplicas: 2
        maxReplicas: 10
        metrics:
        - type: Resource
          resource:
            name: cpu
            target:
              type: Utilization
              averageUtilization: 70
    onlineStore:
      persistence:
        store:
          type: postgres
          secretRef:
            name: feast-data-stores
      server:
        resources:
          requests:
            cpu: 200m
            memory: 256Mi
    registry:
      local:
        persistence:
          store:
            type: sql
            secretRef:
              name: feast-data-stores
```

{% hint style="info" %}
When autoscaling is configured, the operator automatically sets the deployment strategy to `RollingUpdate` (instead of the default `Recreate`) to ensure zero-downtime scaling. You can override this by explicitly setting `deploymentStrategy` in the CR.
{% endhint %}

#### Validation Rules

The operator enforces the following rules:
- `spec.replicas > 1` and `services.scaling.autoscaling` are **mutually exclusive** -- you cannot set both.
- Scaling with `replicas > 1` or any `autoscaling` config is **rejected** if any enabled service uses file-based persistence.
- S3 (`s3://`) and GCS (`gs://`) backed registry file persistence is allowed with scaling, since these object stores support concurrent readers.

#### Using KEDA (Kubernetes Event-Driven Autoscaling)

[KEDA](https://keda.sh) is also supported as an external autoscaler. KEDA should target the FeatureStore's scale sub-resource directly (since it implements the Kubernetes scale API). This is the recommended approach because the operator manages the Deployment's replica count from `spec.replicas` — targeting the Deployment directly would conflict with the operator's reconciliation.

When using KEDA, do **not** set `scaling.autoscaling` or `spec.replicas > 1` -- KEDA manages the replica count through the scale sub-resource.

1. **Ensure DB-backed persistence** -- The CRD's CEL validation rules automatically enforce DB-backed persistence when KEDA scales `spec.replicas` above 1 via the scale sub-resource. The operator also automatically switches the deployment strategy to `RollingUpdate` when `replicas > 1`.

2. **Configure the FeatureStore** with DB-backed persistence:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-keda
spec:
  feastProject: my_project
  services:
    onlineStore:
      persistence:
        store:
          type: postgres
          secretRef:
            name: feast-data-stores
    registry:
      local:
        persistence:
          store:
            type: sql
            secretRef:
              name: feast-data-stores
```

3. **Create a KEDA `ScaledObject`** targeting the FeatureStore resource:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: feast-scaledobject
spec:
  scaleTargetRef:
    apiVersion: feast.dev/v1
    kind: FeatureStore
    name: sample-keda
  minReplicaCount: 1
  maxReplicaCount: 10
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus.monitoring.svc:9090
      metricName: http_requests_total
      query: sum(rate(http_requests_total{service="feast"}[2m]))
      threshold: "100"
```

{% hint style="warning" %}
KEDA-created HPAs are not owned by the Feast operator. The operator will not interfere with them, but it also will not clean them up if the FeatureStore CR is deleted. You must manage the KEDA `ScaledObject` lifecycle independently.
{% endhint %}

For the full API reference, see the [FeatureStore CRD reference](../../infra/feast-operator/docs/api/markdown/ref.md).