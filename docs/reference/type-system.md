# Type System

## Motivation

Feast uses an internal type system to provide guarantees on training and serving data.
Feast currently supports eight primitive types - `INT32`, `INT64`, `FLOAT32`, `FLOAT64`, `STRING`, `BYTES`, `BOOL`, and `UNIX_TIMESTAMP` - and the corresponding array types.
The type system is controlled by [`Values.proto`](https://github.com/feast-dev/feast/blob/master/protos/feast/types/Value.proto) in protobuf and by [`types.py`](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/types.py) in Python.
Type conversion logic can be found in [`type_map.py`](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/type_map.py).

## Examples

### Feature inference

During `feast apply`, Feast runs schema inference on the data sources underlying feature views.
For example, if the `schema` parameter is not specified for a feature view, Feast will examine the schema of the underlying data source to determine the event timestamp column, feature columns, and entity columns.
Each of these columns must be associated with a Feast type, which requires conversion from the data source type system to the Feast type system.
* The feature inference logic calls `_infer_features_and_entities`.
* `_infer_features_and_entities` calls `source_datatype_to_feast_value_type`.
* `source_datatype_to_feast_value_type` cals the appropriate method in `type_map.py`. For example, if a `SnowflakeSource` is being examined, `snowflake_python_type_to_feast_value_type` from `type_map.py` will be called.

### Materialization

Feast serves feature values as [`Value`](https://github.com/feast-dev/feast/blob/master/protos/feast/types/Value.proto) proto objects, which have a type corresponding to Feast types.
Thus Feast must materialize feature values into the online store as `Value` proto objects.
* The local materialization engine first pulls the latest historical features and converts it to pyarrow.
* Then it calls `_convert_arrow_to_proto` to convert the pyarrow table to proto format.
* This calls `python_values_to_proto_values` in `type_map.py` to perform the type conversion.
