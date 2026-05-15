# Feature Definitions Reference

## Table of Contents
- [Entity](#entity)
- [Field](#field)
- [Data Sources](#data-sources)
- [FeatureView](#featureview)
- [OnDemandFeatureView](#ondemandfeatureview)
- [StreamFeatureView](#streamfeatureview)
- [FeatureService](#featureservice)
- [Aggregation](#aggregation)

## Entity

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Unique entity name |
| `join_keys` | List[str] | `[name]` | Join keys for lookup (only one supported) |
| `value_type` | ValueType | - | Deprecated; use `join_keys` instead |
| `description` | str | `""` | Human-readable description |
| `tags` | Dict[str,str] | `{}` | Metadata tags |
| `owner` | str | `""` | Owner/maintainer |

```python
from feast import Entity
from feast.value_type import ValueType

driver = Entity(name="driver_id", description="Driver identifier")
customer = Entity(name="customer_id", join_keys=["customer_id"])
```

## Field

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Field name |
| `dtype` | FeastType | required | Data type |
| `description` | str | `""` | Description |
| `vector_index` | bool | False | Enable vector similarity search |
| `vector_length` | int | - | Vector dimension (required if `vector_index=True`) |
| `vector_search_metric` | str | - | `"COSINE"`, `"L2"`, `"INNER_PRODUCT"` |

### Type System

**Scalar types** (from `feast.types`): `Float32`, `Float64`, `Int32`, `Int64`, `String`, `Bool`, `Bytes`, `UnixTimestamp`

**Collection types**: `Array(T)` where T is a scalar type (e.g., `Array(Float32)` for embeddings)

**ValueType enum** (legacy, from `feast.value_type`): `STRING`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BOOL`, `BYTES`, `UNIX_TIMESTAMP`; plus `_LIST` and `_SET` variants.

**Python → Feast mapping**: `int` → INT64, `str` → STRING, `float` → DOUBLE, `bytes` → BYTES, `bool` → BOOL, `datetime` → UNIX_TIMESTAMP

### Vector field example

```python
Field(
    name="embedding",
    dtype=Array(Float32),
    vector_index=True,
    vector_length=384,
    vector_search_metric="COSINE",
)
```

## Data Sources

### Batch Sources

**FileSource**:
```python
from feast import FileSource

source = FileSource(
    name="driver_stats",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)
```

**BigQuerySource**:
```python
from feast.infra.offline_stores.contrib.bigquery_offline_store.bigquery_source import BigQuerySource

source = BigQuerySource(
    name="driver_stats_bq",
    table="project.dataset.driver_stats",
    timestamp_field="event_timestamp",
)
```

Other batch sources: `SnowflakeSource`, `RedshiftSource`, `PostgreSQLSource`, `SparkSource`, `TrinoSource`, `AthenaSource`, `ClickhouseSource`

### Stream Sources

**KafkaSource**:
```python
from feast.data_source import KafkaSource

source = KafkaSource(
    name="driver_trips_stream",
    kafka_bootstrap_servers="broker:9092",
    topic="driver_trips",
    timestamp_field="event_timestamp",
    batch_source=file_source,  # for backfill
    message_format=AvroFormat(schema_json=schema),
)
```

**KinesisSource**: `region`, `stream_name`, `record_format`, `batch_source`

**PushSource** (for manual push via SDK):
```python
from feast.data_source import PushSource

push_source = PushSource(name="driver_push", batch_source=file_source)
```

### RequestSource (for OnDemandFeatureView)

```python
from feast import RequestSource, Field
from feast.types import Float64

input_request = RequestSource(
    name="vals_to_add",
    schema=[Field(name="val_to_add", dtype=Float64)],
)
```

## FeatureView

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Unique name |
| `source` | DataSource | required | Batch or stream data source |
| `entities` | List[Entity] | `[]` | Associated entities |
| `schema` | List[Field] | `[]` | Feature schema (can be inferred from source) |
| `ttl` | timedelta | `timedelta(0)` | Time-to-live for features |
| `online` | bool | `True` | Available for online retrieval |
| `offline` | bool | `False` | Available for offline retrieval |
| `description` | str | `""` | Description |
| `tags` | Dict[str,str] | `{}` | Metadata |
| `owner` | str | `""` | Owner |
| `mode` | str | - | Transformation mode: `"python"`, `"pandas"`, `"sql"`, `"spark"`, `"ray"`, `"substrait"` |

```python
from feast import FeatureView, Field
from feast.types import Float32, Int64
from datetime import timedelta

driver_hourly_stats = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=365),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_source,
)
```

## OnDemandFeatureView

Features computed at request time from other feature views and/or request data.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Unique name |
| `sources` | List | required | Input FeatureViews and/or RequestSources |
| `schema` | List[Field] | required | Output schema |
| `mode` | str | `"pandas"` | `"pandas"` or `"python"` |
| `singleton` | bool | `False` | Single-row dict input (mode="python" only) |
| `write_to_online_store` | bool | `False` | Precompute on write instead of read |
| `aggregations` | List[Aggregation] | `[]` | Pre-transformation aggregations |

### Pandas mode (default)

```python
@on_demand_feature_view(
    sources=[driver_hourly_stats, input_request],
    schema=[Field(name="conv_rate_plus_val", dtype=Float64)],
    mode="pandas",
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val"] = inputs["conv_rate"] + inputs["val_to_add"]
    return df
```

### Python mode

```python
@on_demand_feature_view(
    sources=[driver_hourly_stats],
    schema=[Field(name="conv_rate_category", dtype=String)],
    mode="python",
)
def categorize_conv_rate(inputs: dict) -> dict:
    output = {"conv_rate_category": []}
    for rate in inputs["conv_rate"]:
        output["conv_rate_category"].append("high" if rate > 0.5 else "low")
    return output
```

### Python singleton mode

```python
@on_demand_feature_view(
    sources=[driver_hourly_stats],
    schema=[Field(name="conv_rate_category", dtype=String)],
    mode="python",
    singleton=True,
)
def categorize_conv_rate(inputs: dict) -> dict:
    rate = inputs["conv_rate"]
    return {"conv_rate_category": "high" if rate > 0.5 else "low"}
```

### Write-to-online-store mode

```python
@on_demand_feature_view(
    sources=[push_source],
    schema=[Field(name="trips_today_category", dtype=String)],
    write_to_online_store=True,
)
def categorize_trips(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["trips_today_category"] = inputs["trips_today"].apply(
        lambda x: "high" if x > 10 else "low"
    )
    return df
```

### Aggregation-based ODFV

```python
from feast.aggregation import Aggregation

@on_demand_feature_view(
    sources=[driver_hourly_stats],
    schema=[Field(name="sum_trips", dtype=Int64)],
    aggregations=[Aggregation(column="avg_daily_trips", function="sum")],
)
def agg_view(inputs: pd.DataFrame) -> pd.DataFrame:
    return inputs
```

### Validation note

Use `feast apply --skip-feature-view-validation` if ODFV validation fails with complex logic (validation uses random inputs).

## StreamFeatureView

Extends FeatureView for stream sources (Kafka, Kinesis, PushSource).

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Unique name |
| `source` | DataSource | required | KafkaSource, KinesisSource, or PushSource |
| `entities` | List[Entity] | `[]` | Entities |
| `schema` | List[Field] | `[]` | Schema |
| `ttl` | timedelta | `timedelta(0)` | TTL |
| `aggregations` | List[Aggregation] | `[]` | Windowed aggregations |
| `timestamp_field` | str | - | Required if using aggregations |
| `udf` | function | - | Transformation function |
| `mode` | str | - | `"python"`, `"pandas"`, `"spark"`, `"spark_sql"` |

```python
from feast import StreamFeatureView, Field
from feast.types import Int64
from feast.aggregation import Aggregation
from datetime import timedelta

driver_stream = StreamFeatureView(
    name="driver_trips_stream",
    entities=[driver],
    source=kafka_source,
    schema=[Field(name="trips", dtype=Int64)],
    ttl=timedelta(hours=2),
    aggregations=[
        Aggregation(column="trips", function="count", time_window=timedelta(hours=1)),
    ],
    timestamp_field="event_timestamp",
)
```

## FeatureService

Groups features from one or more feature views for retrieval.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Unique name |
| `features` | List | required | Feature views or projections |
| `description` | str | `""` | Description |
| `tags` | Dict[str,str] | `{}` | Metadata |
| `owner` | str | `""` | Owner |
| `logging_config` | LoggingConfig | - | Logging configuration |

```python
from feast import FeatureService

driver_activity_service = FeatureService(
    name="driver_activity",
    features=[
        driver_hourly_stats,
        transformed_conv_rate,
    ],
    description="Features for driver activity model",
)
```

### Feature projections (select specific features)

```python
driver_fs = FeatureService(
    name="driver_ranking",
    features=[
        driver_hourly_stats[["conv_rate", "acc_rate"]],
    ],
)
```

## Aggregation

For StreamFeatureView windowed aggregations.

| Parameter | Type | Description |
|-----------|------|-------------|
| `column` | str | Source column name |
| `function` | str | `"sum"`, `"max"`, `"min"`, `"count"`, `"mean"` |
| `time_window` | timedelta | Aggregation window |
| `slide_interval` | timedelta | Slide interval (for sliding windows) |

```python
from feast.aggregation import Aggregation
from datetime import timedelta

agg = Aggregation(
    column="trips",
    function="count",
    time_window=timedelta(hours=1),
    slide_interval=timedelta(minutes=5),
)
```
