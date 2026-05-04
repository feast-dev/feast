# Ray Data Source (contrib)

> **⚠️ Contrib Plugin:**
> `RaySource` is a contributed plugin shipped alongside the [Ray offline store](../offline-stores/ray.md). It may not be as stable or fully supported as core data sources.

`RaySource` is a pure-metadata descriptor that tells Feast **how** to load a
[Ray Dataset](https://docs.ray.io/en/latest/data/api/dataset.html) from any
source that Ray Data supports natively — Parquet, CSV, JSON, HuggingFace
Datasets, MongoDB, binary files, images, TFRecords, and more.

It is the recommended data source when using the
[Ray offline store](../offline-stores/ray.md) and replaces the need for
`FileSource` for all non-Parquet and non-file-based data.

---

## When to use RaySource vs FileSource

| Scenario | Recommended source |
|---|---|
| Parquet files on disk / S3 / GCS (existing setup) | `FileSource` (backward compatible) |
| Parquet via Ray reader (pipelines, remote auth) | `RaySource(reader_type="parquet")` |
| CSV, JSON, text, images via Ray | `RaySource` |
| HuggingFace `datasets` library | `RaySource(reader_type="huggingface")` |
| MongoDB, SQL, TFRecords, WebDataset | `RaySource` |

---

## Installation

`RaySource` is bundled with the Ray offline store contrib package:

```bash
pip install 'feast[ray]'
```

---

## Supported `reader_type` values

| `reader_type` | Underlying Ray API | Notes |
|---|---|---|
| `parquet` | `ray.data.read_parquet` | S3, GCS, HDFS, local |
| `csv` | `ray.data.read_csv` | |
| `json` | `ray.data.read_json` | |
| `text` | `ray.data.read_text` | |
| `images` | `ray.data.read_images` | |
| `binary_files` | `ray.data.read_binary_files` | |
| `tfrecords` | `ray.data.read_tfrecords` | |
| `webdataset` | `ray.data.read_webdataset` | |
| `huggingface` | `ray.data.from_huggingface` | Wraps `datasets.load_dataset` |
| `mongo` | `ray.data.read_mongo` | |
| `sql` | `ray.data.read_sql` | Pass `connection_url` in `reader_options` |

---

## Configuration

### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `name` | `str` | Yes | Unique name for this data source |
| `reader_type` | `str` | Yes | One of the supported reader types above |
| `path` | `str` | No | File or directory path (required for file-based readers) |
| `reader_options` | `dict` | No | Extra keyword arguments forwarded to the Ray reader |
| `timestamp_field` | `str` | No | Column containing event timestamps |
| `created_timestamp_column` | `str` | No | Column containing row creation timestamps |
| `tags` | `dict` | No | Arbitrary key-value metadata |
| `description` | `str` | No | Human-readable description |
| `owner` | `str` | No | Owning team or contact |

---

## Usage examples

### Parquet on S3

```python
from feast.infra.offline_stores.contrib.ray_offline_store.ray_source import RaySource

driver_stats = RaySource(
    name="driver_stats_parquet",
    reader_type="parquet",
    path="s3://my-bucket/driver_stats/",
    timestamp_field="event_timestamp",
)
```

### CSV

```python
sensor_readings = RaySource(
    name="sensor_readings_csv",
    reader_type="csv",
    path="/data/sensors/",
    timestamp_field="ts",
)
```

### HuggingFace dataset

Load a dataset from the [HuggingFace Hub](https://huggingface.co/datasets)
directly into Feast.

```python
from feast.infra.offline_stores.contrib.ray_offline_store.ray_source import RaySource

cheque_images = RaySource(
    name="cheque_images_hf",
    reader_type="huggingface",
    reader_options={
        "dataset_name": "cheques_sample_data",
        "split": "train",
    },
    timestamp_field="event_timestamp",
)
```

### MongoDB

```python
transaction_log = RaySource(
    name="transactions_mongo",
    reader_type="mongo",
    reader_options={
        "uri": "mongodb://localhost:27017",
        "database": "featuredb",
        "collection": "transactions",
    },
    timestamp_field="created_at",
)
```

### SQL (via connection URL)

```python
user_features = RaySource(
    name="user_features_sql",
    reader_type="sql",
    reader_options={
        "connection_url": "postgresql+psycopg2://user:password@host:5432/db",  # pragma: allowlist secret
        "query": "SELECT * FROM user_features",
    },
    timestamp_field="event_timestamp",
)
```

---

## Using RaySource in a BatchFeatureView

```python
from datetime import timedelta
from feast import BatchFeatureView, Entity, Field
from feast.types import Float32, Int64, String
from feast.infra.offline_stores.contrib.ray_offline_store.ray_source import RaySource

cheque = Entity(name="cheque_id", description="Unique cheque identifier")

cheque_source = RaySource(
    name="cheque_images_hf",
    reader_type="huggingface",
    reader_options={
        "dataset_name": "cheques_sample_data",
        "split": "train",
    },
    timestamp_field="event_timestamp",
)

cheque_ocr_fv = BatchFeatureView(
    name="cheque_ocr_features",
    entities=[cheque],
    ttl=timedelta(days=365),
    schema=[
        Field(name="cheque_id", dtype=Int64),
        Field(name="payee_name", dtype=String),
        Field(name="amount", dtype=String),
        Field(name="bank_name", dtype=String),
        Field(name="raw_text", dtype=String),
    ],
    source=cheque_source,
)
```

---

## Retrieving data as a Ray Dataset

Once the feature view is materialised you can retrieve the offline features
directly as a Ray Dataset using the first-class `to_ray_dataset()` method:

```python
from feast import FeatureStore

store = FeatureStore(".")

# Chain directly on the retrieval job — to_ray_dataset() is a first-class
# method on every RetrievalJobs.
ds = store.get_historical_features(
    features=["cheque_ocr_features:payee_name", "cheque_ocr_features:amount"],
    entity_df=entity_df,
).to_ray_dataset()

# Use the dataset downstream in Ray or ML pipelines
ds.show(3)
```

---

## Proto serialisation

`RaySource` is fully serialisable to Feast's protobuf registry format. The
`reader_type`, `path`, and `reader_options` dict are all persisted and can be
round-tripped via `to_proto()` / `from_proto()`.

---

## Limitations

* The Ray offline store (and therefore `RaySource`) requires `feast[ray]`.
* `reader_type="sql"` requires a serialisable `connection_url`; raw
  `sqlalchemy.engine.Engine` objects cannot be pickled across Ray workers.
* Streaming sources (Kafka, Kinesis) are not supported via `RaySource`; use
  the dedicated [Kafka](kafka.md) or [Kinesis](kinesis.md) data sources.

---

## Related pages

* [Ray Offline Store](../offline-stores/ray.md)
* [Ray Compute Engine](../compute-engine/ray.md)
* [Feature Retrieval](../../getting-started/concepts/feature-retrieval.md)
