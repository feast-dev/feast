# Stream Feature View
`StreamFeatureView` is a type of feature view in Feast that allows you to define features that are continuously updated from a streaming source. It is designed to handle real-time data ingestion and feature generation, making it suitable for use cases where features need to be updated frequently as new data arrives.

### Supported Compute Engines
- [x] LocalComputeEngine
- [x] SparkComputeEngine
- [ ] FlinkComputeEngine

### Key Capabilities
- **Real-time Feature Generation**: Supports defining features that are continuously updated from a streaming source.

- **Transformations**: Apply transformation logic (e.g., `feature_transformation` or `udf`) to raw data source.

- **Aggregations**: Define time-windowed aggregations (e.g., `sum`, `avg`) over event-timestamped data.

- **Feature resolution & execution**: Automatically resolves and executes dependent views during materialization or retrieval.
