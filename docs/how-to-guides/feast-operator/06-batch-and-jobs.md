# Guide 6 — Batch Engine & Scheduled Jobs

This guide covers two related top-level spec fields:

- **`spec.batchEngine`** — override the compute engine used for materialization
- **`spec.cronJob`** — schedule periodic `feast materialize-incremental` (or any command) as a Kubernetes `CronJob`

---

## Batch Engine (`spec.batchEngine`)

By default, Feast runs materialization using the **local** batch engine (in-process Python).
For large feature sets you can point the operator at a Spark, Ray, or other supported engine
via a Kubernetes ConfigMap.

### ConfigMap format

Create a ConfigMap whose value is a YAML snippet identical to the `batch_engine` section of
`feature_store.yaml`. Include the `type:` key and all engine-specific options:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: feast-batch-engine
data:
  config: |
    type: spark
    spark_conf:
      spark.master: k8s://https://kubernetes.default.svc
      spark.kubernetes.namespace: feast
      spark.kubernetes.container.image: ghcr.io/feast-dev/feast-spark:latest
      spark.executor.instances: "2"
      spark.executor.memory: 4g
      spark.driver.memory: 2g
```

Reference the ConfigMap from the CR:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-spark
spec:
  feastProject: my_project
  batchEngine:
    configMapRef:
      name: feast-batch-engine     # ConfigMap name
    configMapKey: config           # key inside the ConfigMap (default: "config")
```

### Engine types

| `type` | Notes |
|--------|-------|
| `local` | Default; in-process Python, no extra infra |
| `spark` | Apache Spark; requires a Spark operator or standalone cluster |
| `ray` | Ray cluster; requires a Ray operator |
| `bytewax` | Bytewax streaming engine |
| `snowflake.engine` | Snowflake Snowpark compute |

> For engine-specific YAML options (Spark conf, Ray address, etc.) see the
> [Feast SDK — Compute Engine](../reference/compute-engine/) docs.

---

## Scheduled Materialization (`spec.cronJob`)

The operator can deploy a Kubernetes `CronJob` that runs `feast materialize-incremental`
(or any custom command) on a schedule. This is the recommended way to keep your online store
fresh without managing an external job scheduler.

### CronJob image resolution

The CronJob container image is resolved through the following priority chain:

1. **`cronJob.containerConfigs.image` in the CR** — per-CronJob override
2. **`RELATED_IMAGE_CRON_JOB` env var on the operator pod** — cluster-wide default set by OLM/platform (default: `quay.io/openshift/origin-cli:4.17`)

```sh
# Override cluster-wide for all CronJobs
kubectl set env deployment/feast-operator-controller-manager \
  RELATED_IMAGE_CRON_JOB=my-registry.example.com/tools/cli:latest \
  -n feast-operator-system
```

### Minimal example — nightly materialization

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: feast-production
spec:
  feastProject: my_project
  cronJob:
    schedule: "0 2 * * *"          # every day at 02:00 UTC
    containerConfigs:
      image: quay.io/feastdev/feature-server:0.62.0
```

The CronJob runs the default `feast materialize-incremental` command using the same
`feature_store.yaml` that the operator generated for this `FeatureStore`.

### Custom command

Override the container command to run any Feast CLI command:

```yaml
cronJob:
  schedule: "*/30 * * * *"
  containerConfigs:
    image: quay.io/feastdev/feature-server:0.62.0
    commands:
      - feast
      - materialize-incremental
      - "2099-01-01T00:00:00"      # materialize up to a fixed end time
```

Or run a Python script instead:

```yaml
containerConfigs:
  commands:
    - python
    - /app/materialize.py
```

### Time zone

```yaml
cronJob:
  schedule: "0 3 * * *"
  timeZone: "Asia/Kolkata"         # defaults to the kube-controller-manager time zone
```

### Concurrency policy

```yaml
cronJob:
  schedule: "0 * * * *"
  concurrencyPolicy: Forbid        # Allow | Forbid | Replace
  startingDeadlineSeconds: 300     # skip if missed by > 5 minutes
```

### Job history retention

```yaml
cronJob:
  schedule: "0 2 * * *"
  successfulJobsHistoryLimit: 3    # keep last 3 successful runs
  failedJobsHistoryLimit: 5        # keep last 5 failed runs
```

### Suspend a CronJob

To temporarily pause scheduled runs without deleting the `CronJob`:

```yaml
cronJob:
  schedule: "0 2 * * *"
  suspend: true
```

### Resource requests for the job pod

```yaml
cronJob:
  schedule: "0 2 * * *"
  containerConfigs:
    image: quay.io/feastdev/feature-server:0.62.0
    resources:
      requests:
        cpu: "1"
        memory: "2Gi"
      limits:
        cpu: "4"
        memory: "8Gi"
```

### Environment variables and secrets in the job pod

```yaml
cronJob:
  schedule: "0 2 * * *"
  containerConfigs:
    image: quay.io/feastdev/feature-server:0.62.0
    envFrom:
      - secretRef:
          name: feast-data-stores
    env:
      - name: FEAST_USAGE
        value: "false"
```

### Advanced job spec

```yaml
cronJob:
  schedule: "0 2 * * *"
  jobSpec:
    parallelism: 1
    completions: 1
    activeDeadlineSeconds: 3600    # abort if job takes more than 1 hour
    backoffLimit: 2                # retry up to 2 times on failure
    podTemplateAnnotations:
      prometheus.io/scrape: "false"
  containerConfigs:
    image: quay.io/feastdev/feature-server:0.62.0
```

### Full `cronJob` field reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schedule` | string | — | Cron expression (required) |
| `timeZone` | string | kube-controller-manager TZ | IANA time zone name |
| `concurrencyPolicy` | string | `Allow` | `Allow` / `Forbid` / `Replace` |
| `suspend` | bool | `false` | Suspend future runs |
| `startingDeadlineSeconds` | int64 | — | Abort if start missed by this many seconds |
| `successfulJobsHistoryLimit` | int32 | — | Successful job history to keep |
| `failedJobsHistoryLimit` | int32 | — | Failed job history to keep |
| `annotations` | map | — | CronJob metadata annotations |
| `jobSpec.parallelism` | int32 | 1 | Job pod parallelism |
| `jobSpec.completions` | int32 | 1 | Required completions |
| `jobSpec.activeDeadlineSeconds` | int64 | — | Max job duration |
| `jobSpec.backoffLimit` | int32 | — | Retry limit |
| `containerConfigs.image` | string | operator default | Feature server image |
| `containerConfigs.commands` | []string | `feast materialize-incremental` | Override container command |
| `containerConfigs.resources` | ResourceRequirements | — | CPU/memory requests and limits |
| `containerConfigs.env` / `envFrom` | — | — | Environment variables |

---

## Combining batchEngine + cronJob

Use both together to run scheduled Spark-based materialization:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: feast-spark
spec:
  feastProject: my_project
  batchEngine:
    configMapRef:
      name: feast-spark-engine
  cronJob:
    schedule: "0 1 * * *"
    concurrencyPolicy: Forbid
    containerConfigs:
      image: quay.io/feastdev/feature-server:0.62.0
      resources:
        requests:
          memory: "4Gi"
```

---

## See also

- [API reference — `BatchEngineConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#batchengineconfig)
- [API reference — `FeastCronJob`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#feastcronjob)
- [Feast SDK — Compute Engine](../reference/compute-engine/)
