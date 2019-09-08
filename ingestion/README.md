# Feast Ingestion

Feast ingestion creates and runs Apache Beam pipeline to import Feature Row elements from various
sources to multiple stores in Feast.

The configuration for the ingestion is specified in an import job, which can be referred from
[ImportJobSpecs.proto](../protos/feast/specs/ImportJobSpecs.proto). Import job configuration
can be passed as a YAML file following the format in `ImportJobSpecs.proto`.

Example of an import job that listens to Kafka for Feature Row and writes them to Redis.
```yaml
importSpec:
  type: kafka
  sourceOptions:
    server: localhost:9092
    topics: topic_1
  entities:
  - entity_1
  schema:
    fields:
    - featureId: entity_1.feature_1
    - featureId: entity_1.feature_2

writeFeatureMetricsToInfluxDb: true
influxDbUrl: http://localhost:8086
influxDbDatabase: my_influx_db
influxDbMeasurement: my_influx_measurement

entitySpecs:
- name: entity_1
  description: This is a test entity

featureSpecs:
- id: entity_1.feature_1
  entity: entity_1
  name: feature_1
  owner: feast
  valueType: INT64
- id: entity_1.feature_2
  entity: entity_1
  name: feature_2
  owner: feast
  valueType: INT64

servingStorageSpec:
  id: serving_store
  type: redis
  options:
    host: localhost
    port: 6379

errorsStorageSpec:
  id: error_store
  type: stdout
``` 