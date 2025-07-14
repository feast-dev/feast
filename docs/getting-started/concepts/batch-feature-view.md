# 🧬 BatchFeatureView in Feast

`BatchFeatureView` is a flexible abstraction in Feast that allows users to define features derived from batch data sources or even other `FeatureView`s, enabling composable and reusable feature pipelines. It is an extension of the `FeatureView` class, with support for user-defined transformations, aggregations, and recursive chaining of feature logic.

---

## ✅ Key Capabilities

- **Composable DAG of FeatureViews**: Supports defining a `BatchFeatureView` on top of one or more other `FeatureView`s.
- **Transformations**: Apply PySpark-based transformation logic (`feature_transformation` or `udf`) to raw data source, can also be used to deal with multiple data sources.
- **Aggregations**: Define time-windowed aggregations (e.g. `sum`, `avg`) over event-timestamped data.
- **Feature resolution & execution**: Automatically resolves and executes DAGs of dependent views during materialization or retrieval. More details in the [Compute engine documentation](../../reference/compute-engine/README.md).
- **Materialization Sink Customization**: Specify a custom `sink_source` to define where derived feature data should be persisted.

---

## 📐 Class Signature

```python
class BatchFeatureView(FeatureView):
    def __init__(
        *,
        name: str,
        source: Union[DataSource, FeatureView, List[FeatureView]],
        sink_source: Optional[DataSource] = None,
        schema: Optional[List[Field]] = None,
        entities: Optional[List[Entity]] = None,
        aggregations: Optional[List[Aggregation]] = None,
        udf: Optional[Callable[[DataFrame], DataFrame]] = None,
        udf_string: Optional[str] = None,
        ttl: Optional[timedelta] = timedelta(days=0),
        online: bool = True,
        offline: bool = False,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
    )
```

---

## 🧠 Usage

### 1. Simple Feature View from Data Source

```python
from feast import BatchFeatureView, Field
from feast.types import Float32, Int32
from feast import FileSource
from feast.aggregation import Aggregation
from datetime import timedelta

source = FileSource(
    path="s3://bucket/path/data.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver_fv = BatchFeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    schema=[
        Field(name="driver_id", dtype=Int32),
        Field(name="conv_rate", dtype=Float32),
    ],
    aggregations=[
        Aggregation(column="conv_rate", function="sum", time_window=timedelta(days=1)),
    ],
    source=source,
)
```

---

### 2. Derived Feature View from Another View
You can build feature views on top of other features by deriving a feature view from another view. Let's take a look at an example.
```python
from feast import BatchFeatureView, Field
from pyspark.sql import DataFrame
from feast.types import Float32, Int32
from feast import FileSource

def transform(df: DataFrame) -> DataFrame:
    return df.withColumn("conv_rate", df["conv_rate"] * 2)

daily_driver_stats = BatchFeatureView(
    name="daily_driver_stats",
    entities=["driver_id"],
    schema=[
        Field(name="driver_id", dtype=Int32),
        Field(name="conv_rate", dtype=Float32),
    ],
    udf=transform,
    source=driver_fv,
    sink_source=FileSource(  # Required to specify where to sink the derived view
        name="daily_driver_stats_sink",
        path="s3://bucket/daily_stats/",
        file_format="parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    ),
)
```

---

## 🔄 Execution Flow

Feast automatically resolves the DAG of `BatchFeatureView` dependencies during:

- `materialize()`: recursively resolves and executes the feature view graph.
- `get_historical_features()`: builds the execution plan for retrieving point-in-time correct features.
- `apply()`: registers the feature view DAG structure to the registry.

Each transformation and aggregation is turned into a DAG node (e.g., `SparkTransformationNode`, `SparkAggregationNode`) executed by the compute engine (e.g., `SparkComputeEngine`).

---

## ⚙️ How Materialization Works

- If the `BatchFeatureView` is backed by a base source (`FileSource`, `BigQuerySource`, `SparkSource` etc), the `batch_source` is used directly.
- If the source is another feature view (i.e., chained views), the `sink_source` must be provided to define the materialization target data source.
- During DAG planning, `SparkWriteNode` uses the `sink_source` as the batch sink.

---

## 🧪 Example Tests

See:

- `test_spark_dag_materialize_recursive_view()`: Validates chaining of two feature views and output validation.
- `test_spark_compute_engine_materialize()`: Validates transformation and write of features into offline and online stores.

---

## 🛑 Gotchas

- `sink_source` is **required** when chaining views (i.e., `source` is another FeatureView or list of them).
- Schema fields must be consistent with `sink_source`, `batch_source.field_mapping` if field mappings exist.
- Aggregation logic must reference columns present in the raw source or transformed inputs.

---

## 🔮 Future Directions

- Support additional offline stores (e.g., Snowflake, Redshift) with auto-generated sink sources.
- Enable fully declarative transform logic (SQL + UDF mix).
- Introduce optimization passes for DAG pruning and fusion.
