# 🧠 ComputeEngine (WIP)

The `ComputeEngine` is Feast’s pluggable abstraction for executing feature pipelines — including transformations, aggregations, joins, and materialization/get_historical_features — on a backend of your choice (e.g., Spark, PyArrow, Pandas, Ray).

It powers both:

- `materialize()` – for batch and stream generation of features to offline/online stores
- `get_historical_features()` – for point-in-time correct training dataset retrieval

This system builds and executes DAGs (Directed Acyclic Graphs) of typed operations, enabling modular and scalable workflows.

---

## 🧠 Core Concepts

| Component          | Description                                                        |
|--------------------|--------------------------------------------------------------------|
| `ComputeEngine`    | Interface for executing materialization and retrieval tasks        |
| `FeatureBuilder`       | Constructs a DAG for a specific backend                            |
| `DAGNode`          | Represents a logical operation (read, aggregate, join, etc.)       |
| `ExecutionPlan`    | Executes nodes in dependency order and stores intermediate outputs |
| `ExecutionContext` | Holds config, registry, stores, entity data, and node outputs      |

---

## ✨ Available Engines

### 🔥 SparkComputeEngine

- Distributed DAG execution via Apache Spark
- Supports point-in-time joins and large-scale materialization
- Integrates with `SparkOfflineStore` and `SparkMaterializationJob`

### 🧪 LocalComputeEngine (WIP)

- Runs on Arrow + Pandas (or optionally DuckDB)
- Designed for local dev, testing, or lightweight feature generation

---

## 🛠️ Example DAG Flow
`Read → Aggregate → Join → Transform → Write`

Each step is implemented as a `DAGNode`. An `ExecutionPlan` executes these nodes in topological order, caching `DAGValue` outputs.

---

## 🧩 Implementing a Custom Compute Engine

To create your own compute engine:

1. **Implement the interface**

```python
class MyComputeEngine(ComputeEngine):
    def materialize(self, task: MaterializationTask) -> MaterializationJob:
        ...

    def get_historical_features(self, task: HistoricalRetrievalTask) -> pa.Table:
        ...
```

2. Create a FeatureBuilder
```python
class MyDAGBuilder(FeatureBuilder):
    def build_source_node(self): ...
    def build_aggregation_node(self, input_node): ...
    def build_join_node(self, input_node): ...
    def build_transformation_node(self, input_node): ...
    def build_output_nodes(self, input_node): ...
```

3. Define DAGNode subclasses
    * ReadNode, AggregationNode, JoinNode, WriteNode, etc.
    * Each DAGNode.execute(context) -> DAGValue

4. Return an ExecutionPlan
   * ExecutionPlan stores DAG nodes in topological order
   * Automatically handles intermediate value caching 

## 🚧 Roadmap
- [x] Modular, backend-agnostic DAG execution framework
- [x] Spark engine with native support for materialization + PIT joins
- [ ] PyArrow + Pandas engine for local compute
- [ ] Native multi-feature-view DAG optimization
- [ ] DAG validation, metrics, and debug output
- [ ] Scalable distributed backend via Ray or Polars
