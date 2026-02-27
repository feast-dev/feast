---
title: Scaling the Feast Feature Server on Kubernetes
description: The Feast Operator now supports horizontal scaling with static replicas, HPA autoscaling, and external autoscalers like KEDA — enabling production-grade, high-availability feature serving.
date: 2026-02-21
authors: ["Nikhil Kathole"]
---

# Scaling the Feast Feature Server on Kubernetes

As ML systems move from experimentation to production, the feature server often becomes a critical bottleneck. A single-replica deployment might handle development traffic, but production workloads — real-time inference, batch scoring, multiple consuming services — demand the ability to scale horizontally.

We're excited to announce that the Feast Operator now supports **horizontal scaling** for the FeatureStore deployment, giving teams the tools to run Feast at production scale on Kubernetes.

# The Problem: Single-Replica Limitations

By default, the Feast Operator deploys a single-replica Deployment. This works well for getting started, but presents challenges as traffic grows:

- **Single point of failure** — one pod crash means downtime for all feature consumers
- **Throughput ceiling** — a single pod can only handle so many concurrent requests
- **No elasticity** — traffic spikes (model retraining, batch inference) can overwhelm the server
- **Rolling updates cause downtime** — the default `Recreate` strategy tears down the old pod before starting a new one

Teams have been manually patching Deployments or creating external HPAs, but this bypasses the operator's reconciliation loop and can lead to configuration drift.

# The Solution: Native Scaling Support

The Feast Operator now supports three scaling modes. The FeatureStore CRD implements the Kubernetes **scale sub-resource**, which means you can also scale with `kubectl scale featurestore/my-feast --replicas=3`.

## 1. Static Replicas

The simplest approach — set a fixed number of replicas via `spec.replicas`:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: production-feast
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

This gives you high availability and load distribution with a predictable resource footprint. The operator automatically switches the Deployment strategy to `RollingUpdate`, ensuring zero-downtime deployments.

## 2. HPA Autoscaling

For workloads with variable traffic patterns, the operator can create and manage a `HorizontalPodAutoscaler` directly. HPA autoscaling is configured under `services.scaling.autoscaling` and is mutually exclusive with `spec.replicas > 1`:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: autoscaled-feast
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
          limits:
            cpu: "1"
            memory: 1Gi
    registry:
      local:
        persistence:
          store:
            type: sql
            secretRef:
              name: feast-data-stores
```

The operator creates the HPA as an owned resource — it's automatically cleaned up if you remove the autoscaling configuration or delete the FeatureStore CR. If no custom metrics are specified, the operator defaults to **80% CPU utilization**.

## 3. External Autoscalers (KEDA, Custom HPAs)

For teams using [KEDA](https://keda.sh) or other external autoscalers, KEDA should target the FeatureStore's scale sub-resource directly (since it implements the Kubernetes scale API). This is the recommended approach because the operator manages the Deployment's replica count from `spec.replicas` — targeting the Deployment directly would conflict with the operator's reconciliation.

When using KEDA, do **not** set `spec.replicas > 1` or `services.scaling.autoscaling` — KEDA manages the replica count through the scale sub-resource. Configure the FeatureStore with DB-backed persistence, then create a KEDA `ScaledObject` targeting the FeatureStore resource:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: keda-feast
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
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: feast-scaledobject
spec:
  scaleTargetRef:
    apiVersion: feast.dev/v1
    kind: FeatureStore
    name: keda-feast
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

When KEDA scales up `spec.replicas` via the scale sub-resource, the CRD's CEL validation rules automatically ensure DB-backed persistence is configured. The operator also automatically switches the deployment strategy to `RollingUpdate` when `replicas > 1`. This gives you the full power of KEDA's 50+ event-driven triggers with built-in safety checks.

# Safety First: Persistence Validation

Not all persistence backends are safe for multi-replica deployments. File-based stores like SQLite, DuckDB, and local `registry.db` use single-writer file locks that don't work across pods.

The operator enforces this at admission time via CEL validation rules on the CRD — if you try to create or update a FeatureStore with scaling and file-based persistence, the API server rejects the request immediately:

```
Scaling requires DB-backed persistence for the online store.
Configure services.onlineStore.persistence.store when using replicas > 1 or autoscaling.
```

This validation applies to all enabled services (online store, offline store, and registry) and is enforced for both direct CR updates and `kubectl scale` commands via the scale sub-resource. Object-store-backed registry paths (`s3://` and `gs://`) are treated as safe since they support concurrent readers.

| Persistence Type | Compatible with Scaling? |
|---|---|
| PostgreSQL / MySQL | Yes |
| Redis | Yes |
| Cassandra | Yes |
| SQL-based Registry | Yes |
| S3/GCS Registry | Yes |
| SQLite | No |
| DuckDB | No |
| Local `registry.db` | No |

# How It Works Under the Hood

The implementation adds three key behaviors to the operator's reconciliation loop:

**1. Replica management** — The operator sets the Deployment's replica count from `spec.replicas` (which defaults to 1). When HPA is configured, the operator leaves the `replicas` field unset so the HPA controller can manage it. External autoscalers like KEDA can update the replica count through the FeatureStore's scale sub-resource, which updates `spec.replicas` and triggers the operator to reconcile.

**2. Deployment strategy** — The operator automatically switches from `Recreate` (the default for single-replica) to `RollingUpdate` when scaling is enabled. This prevents the "kill-all-pods-then-start-new-ones" behavior that would cause downtime during scaling events. Users can always override this with an explicit `deploymentStrategy` in the CR.

**3. HPA lifecycle** — The operator creates, updates, and deletes the HPA as an owned resource tied to the FeatureStore CR. Removing the `autoscaling` configuration automatically cleans up the HPA.

The scaling status is reported back on the FeatureStore status:

```yaml
status:
  scalingStatus:
    currentReplicas: 3
    desiredReplicas: 3
```

# What About TLS, CronJobs, and Services?

Scaling is designed to work seamlessly with existing operator features:

- **TLS** — Each pod mounts the same TLS secret. OpenShift service-serving certificates work automatically since they're bound to the Service, not individual pods.
- **Kubernetes Services** — The Service's label selector already matches all pods in the Deployment, so load balancing across replicas works out of the box.
- **CronJobs** — The `feast apply` and `feast materialize-incremental` CronJobs use `kubectl exec` into a single pod. Since DB-backed persistence is required for scaling, all pods share the same state — it doesn't matter which pod the CronJob runs against.

# Getting Started

**1. Ensure DB-backed persistence** for all enabled services (online store, offline store, registry).

**2. Configure scaling** in your FeatureStore CR — use either static replicas or HPA (mutually exclusive):

```yaml
spec:
  replicas: 3            # static replicas (top-level)
  # -- OR --
  # services:
  #   scaling:
  #     autoscaling:      # HPA
  #       minReplicas: 2
  #       maxReplicas: 10
```

**3. Apply** the updated CR:

```bash
kubectl apply -f my-featurestore.yaml
```

**4. Verify** the scaling:

```bash
# Check pods
kubectl get pods -l app.kubernetes.io/managed-by=feast

# Check HPA (if using autoscaling)
kubectl get hpa

# Check FeatureStore status
kubectl get feast -o yaml
```

# Learn More

- [Scaling Feast documentation](https://docs.feast.dev/how-to-guides/scaling-feast)
- [Feast on Kubernetes guide](https://docs.feast.dev/how-to-guides/feast-on-kubernetes)
- [FeatureStore CRD API reference](https://github.com/feast-dev/feast/blob/master/infra/feast-operator/docs/api/markdown/ref.md)
- [Sample CRs for static scaling and HPA](https://github.com/feast-dev/feast/tree/master/infra/feast-operator/config/samples)
- Join the [Feast Slack](https://slack.feast.dev) to share feedback and ask questions

We're excited to see teams scale their feature serving infrastructure with confidence. Try it out and let us know how it works for your use case!
