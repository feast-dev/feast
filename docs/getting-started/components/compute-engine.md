# Compute Engine

Note: The materialization is now constructed via unified compute engine interface.

A Compute Engine in Feast is a component that handles materialization and historical retrieval tasks. It is responsible
for executing the logic defined in feature views, such as aggregations, transformations, and custom user-defined
functions (UDFs).

A materialization task abstracts over specific technologies or frameworks that are used to materialize data. It allows
users to use a pure local serialized approach (which is the default LocalComputeEngine), or delegates the
materialization to seperate components (e.g. AWS Lambda, as implemented by the the LambdaComputeEngine).

If the built-in engines are not sufficient, you can create your own custom materialization engine. Please
see [this guide](../../how-to-guides/customizing-feast/creating-a-custom-compute-engine.md) for more details.

Please see [feature\_store.yaml](../../reference/feature-repository/feature-store-yaml.md#overview) for configuring
engines.

### Supported Compute Engines
```markdown
| Compute Engine         | Description                                                                                      | Supported  | Link |
|-------------------------|-------------------------------------------------------------------------------------------------|------------|------|
| LocalComputeEngine      | Runs on Arrow + Pandas/Polars/Dask etc., designed for light weight transformation.              | ✅         |      |
| SparkComputeEngine      | Runs on Apache Spark, designed for large-scale distributed feature generation.                  | ✅         |      |
| SnowflakeComputeEngine  | Runs on Snowflake, designed for scalable feature generation using Snowflake SQL.                | ✅         |      |
| LambdaComputeEngine     | Runs on AWS Lambda, designed for serverless feature generation.                                 | ✅         |      |
| FlinkComputeEngine      | Runs on Apache Flink, designed for stream processing and real-time feature generation.          | ❌         |      |
| RayComputeEngine        | Runs on Ray, designed for distributed feature generation and machine learning workloads.        | ✅         |      |
```

### Batch Engine
Batch Engine Config can be configured in the `feature_store.yaml` file, and it serves as the default configuration for all materialization and historical retrieval tasks. The `batch_engine` config in BatchFeatureView. E.g
```yaml
batch_engine:
    type: spark.engine
    config:
        spark_master: "local[*]"
        spark_app_name: "Feast Batch Engine"
        spark_conf:
            spark.sql.shuffle.partitions: 100
            spark.executor.memory: "4g"

```
in BatchFeatureView.
```python
from feast import BatchFeatureView

fv = BatchFeatureView(
    batch_engine={
        "spark_conf": {
            "spark.sql.shuffle.partitions": 200,
            "spark.executor.memory": "8g"
        },
    }
)
```
Then, when you materialize the feature view, it will use the batch_engine configuration specified in the feature view, which has shuffle partitions set to 200 and executor memory set to 8g.

### Stream Engine
Stream Engine Config can be configured in the `feature_store.yaml` file, and it serves as the default configuration for all stream materialization and historical retrieval tasks. The `stream_engine` config in FeatureView. E.g
```yaml
stream_engine:
    type: spark.engine
    config:
        spark_master: "local[*]"
        spark_app_name: "Feast Stream Engine"
        spark_conf:
            spark.sql.shuffle.partitions: 100
            spark.executor.memory: "4g"
```
```python
from feast import StreamFeatureView
fv = StreamFeatureView(
    stream_engine={
        "spark_conf": {
            "spark.sql.shuffle.partitions": 200,
            "spark.executor.memory": "8g"
        },
    }
)
```
Then, when you materialize the feature view, it will use the stream_engine configuration specified in the feature view, which has shuffle partitions set to 200 and executor memory set to 8g.

### API

The compute engine builds the execution plan in a DAG format named FeatureBuilder. It derives feature generation from
Feature View definitions including:

```
1. Transformation (via Transformation API)
2. Aggregation (via Aggregation API)
3. Join (join with entity datasets, customized JOIN or join with another Feature View)
4. Filter (Point in time filter, ttl filter, filter by custom expression)
...
```

### Components 
The compute engine is responsible for executing the materialization and retrieval tasks defined in the feature views. It
builds a directed acyclic graph (DAG) of operations that need to be performed to generate the features.
The Core components of the compute engine are:


#### Feature Builder

The Feature builder is responsible for resolving the features from the feature views and executing the operations
defined in the DAG. It handles the execution of transformations, aggregations, joins, and filters.

#### Feature Resolver

The Feature resolver is the core component of the compute engine that constructs the execution plan for feature
generation. It takes the definitions from feature views and builds a directed acyclic graph (DAG) of operations that
need to be performed to generate the features.

#### DAG
The DAG represents the directed acyclic graph of operations that need to be performed to generate the features. It
contains nodes for each operation, such as transformations, aggregations, joins, and filters. The DAG is built by the
Feature Resolver and executed by the Feature Builder.

DAG nodes are defined as follows:
```
   +---------------------+
   |   SourceReadNode    |  <- Read data from offline store (e.g. Snowflake, BigQuery, etc. or custom source)
   +---------------------+
             |
             v
   +--------------------------------------+
   |  TransformationNode / JoinNode (*)   |   <- Merge data sources, custom transformations by user, or default join
   +--------------------------------------+ 
             |
             v
   +---------------------+
   |    FilterNode       |   <- used for point-in-time filtering 
   +---------------------+
             |
             v
   +---------------------+
   | AggregationNode (*) |   <- only if aggregations are defined
   +---------------------+
             |
             v
   +---------------------+
   |   DeduplicationNode |   <- used if no aggregation and for history
   +---------------------+    retrieval
             |
             v
   +---------------------+
   |  ValidationNode (*) |   <- optional validation checks
   +---------------------+
             |
             v
        +----------+
        |  Output  |
        +----------+
         /        \
        v          v
+----------------+  +----------------+
| OnlineStoreWrite|  OfflineStoreWrite|
+----------------+  +----------------+
```