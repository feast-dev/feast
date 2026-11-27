# SparkApplication Compute Engine

## Description

The **SparkApplication** compute engine runs Feast **batch materialization** on Kubernetes by creating a [Kubeflow Spark Operator](https://github.com/kubeflow/spark-operator) `SparkApplication` custom resource for each materialization job.

Unlike the in-process [`spark.engine`](spark.md) compute engine (which uses a Spark session inside the Feast process), `spark_application` submits work to the Spark Operator. The operator starts a driver pod and executors from your configured image; Feast polls the SparkApplication until it completes.

| Capability | Supported |
|------------|-----------|
| `materialize` / `materialize-incremental` | Yes |
| Multiple feature views in one job | Yes — one SparkApplication per materialize call |
| `get_historical_features` | Not yet |
| SparkConnect | Separate approach — not this engine |

### Design

1. Feast creates a ConfigMap with job tasks and a driver copy of `feature_store.yaml`.
2. Feast creates a `SparkApplication` CR pointing at the driver entrypoint (`main.py` in the image).
3. Inside the pod, the batch engine type is rewritten to `spark.engine` so materialization uses the Spark session created by `spark-submit` (avoids recursive SparkApplication creation).
4. The driver writes features to your configured **online store** and updates the **registry** (same network backends as the server).

### Requirements

- Kubeflow Spark Operator installed and watching the target namespace.
- A container **image** that includes the Feast SDK, PySpark, and clients for your stores. See the reference [Dockerfile](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/compute_engines/spark_application/Dockerfile).
- **Network-accessible** online store, offline store, and registry. File-based backends are rejected because Spark pods have an ephemeral filesystem:

| Rejected | Examples | Use instead |
|----------|----------|-------------|
| File online | `sqlite`, `faiss` | Redis, remote online, etc. |
| File offline | `dask`, `file`, `duckdb` | `spark`, Postgres, Snowflake, BigQuery, etc. |
| File registry | `file` | SQL registry, Snowflake |

For distributed reads, configure `offline_store.type: spark` (or another store Spark can read efficiently).

### Kubernetes / Feast Operator notes

When using the Feast Operator:

- Point `spec.batchEngine.configMapRef` at a ConfigMap whose `type` is `spark_application` (see [Guide 6 — Batch Engine & Scheduled Jobs](../../how-to-guides/feast-operator/06-batch-and-jobs.md)).
- The operator auto-creates RBAC for the `spark_application` batch engine (server and driver service accounts).
- Set `spec.services.initImage` if init / `feast-apply` containers need the Spark-capable image.

---

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry:
  registry_type: sql
  path: postgresql+psycopg://feast:****@postgres:5432/feast
online_store:
  type: redis
  connection_string: redis:6379
offline_store:
  type: spark
  spark_conf:
    spark.master: local[*]
batch_engine:
  type: spark_application
  image: my-registry.example.com/feast-spark-driver:latest
  namespace: feast
  spark_version: "4.0.1"
  driver_cores: 1
  driver_memory: "2g"
  executor_instances: 2
  executor_cores: 1
  executor_memory: "2g"
  spark_conf:
    spark.sql.shuffle.partitions: "100"
```
{% endcode %}

### Feast Operator ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: feast-spark-batch-engine
  namespace: feast
data:
  config: |
    type: spark_application
    image: my-registry.example.com/feast-spark-driver:latest
    namespace: feast
    executor_instances: 2
    driver_memory: "2g"
    executor_memory: "2g"
---
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: feast
  namespace: feast
spec:
  feastProject: my_project
  batchEngine:
    configMapRef:
      name: feast-spark-batch-engine
    configMapKey: config
```

---

## Remote materialization

If the client uses a **remote** online store (`online_store.type: remote`), `FeatureStore.materialize()` delegates to the feature server HTTP API. The server runs the SparkApplication engine.

- Default (`run_async=False`): block until the server finishes sync materialization.
- `run_async=True`: accept asynchronously (`?async=true`); poll feature-view state in the registry for completion.
- `force=True` (with `run_async=True`): override stuck `MATERIALIZING` state on the server.

```python
from datetime import datetime, timedelta
from feast import FeatureStore

store = FeatureStore(repo_path=".")  # client feature_store.yaml with online_store.type: remote

store.materialize(
    start_date=datetime.utcnow() - timedelta(days=1),
    end_date=datetime.utcnow(),
)
```

---

## Configuration reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `spark_application` | Engine type key |
| `image` | string | **required** | Container image for the Spark driver/executors |
| `image_pull_secrets` | list[str] | `[]` | Image pull secret names |
| `namespace` | string | `default` | Namespace for SparkApplication and ConfigMap |
| `service_account` | string | `""` | Driver service account; empty uses platform/operator default |
| `spark_version` | string | `4.0.1` | Spark version for the CR |
| `driver_cores` | int | `1` | Driver cores |
| `driver_memory` | string | `1g` | Driver memory |
| `executor_instances` | int | `1` | Number of executors |
| `executor_cores` | int | `1` | Cores per executor |
| `executor_memory` | string | `1g` | Memory per executor |
| `spark_conf` | dict | `null` | Extra Spark configuration |
| `hadoop_conf` | dict | `null` | Extra Hadoop configuration |
| `env` | list[dict] | `[]` | Driver env vars (`name` + `value` or `valueFrom`) |
| `env_from` | list[dict] | `[]` | EnvFrom sources |
| `queue_name` | string | `null` | Optional queue / Kueue label |
| `job_timeout_seconds` | int | `3600` | Max wait for SparkApplication completion |
| `poll_interval_seconds` | int | `10` | Status poll interval |
| `ttl_seconds_after_finished` | int | `3600` | CR TTL after finish |
| `restart_policy` | string | `Never` | SparkApplication restart policy |
| `max_retries` | int | `3` | Retries when restart policy allows |
| `concurrency` | int | `1` | Parallel feature views inside one driver |
| `labels` | dict | `{}` | Extra labels on the CR |
| `volumes` / `volume_mounts` | list | `[]` | Extra volumes for the driver |
| `py_files` | list[str] | `[]` | Additional Python files for Spark |
| `node_selector` | dict | `null` | Pod node selector |
| `tolerations` | list | `[]` | Pod tolerations |
| `staging_location` | string | `null` | Reserved for historical retrieval (ignored for materialize) |

---

## Troubleshooting

| Symptom | What to check |
|---------|----------------|
| SparkApplication Pending / insufficient CPU | Lower resource requests via `spark_conf` (for example `spark.kubernetes.driver.request.cores`) or free cluster capacity |
| ImagePullBackOff | Image name, tag, and `image_pull_secrets` |
| 403 on ConfigMap or SparkApplication | RBAC for the Feast server and Spark driver service accounts |
| Init `ValueError` about file-based stores | Switch online/offline/registry to network backends |
| Init / feast-apply failures missing Spark deps | Use a Spark-capable image (`initImage` with the Feast Operator) |

---

## Related

- [Spark compute engine (in-process)](spark.md)
- [Feast Operator — batch engine ConfigMap](../../how-to-guides/feast-operator/06-batch-and-jobs.md)
- [Creating a custom compute engine](../../how-to-guides/customizing-feast/creating-a-custom-compute-engine.md)
