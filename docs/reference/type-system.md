# Type System

## Motivation

Feast uses an internal type system to provide guarantees on training and serving data.
Feast supports primitive types, array types, set types, and map types for feature values.
Null types are not supported, although the `UNIX_TIMESTAMP` type is nullable.
The type system is controlled by [`Value.proto`](https://github.com/feast-dev/feast/blob/master/protos/feast/types/Value.proto) in protobuf and by [`types.py`](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/types.py) in Python.
Type conversion logic can be found in [`type_map.py`](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/type_map.py).

## Supported Types

Feast supports the following data types:

### Primitive Types

| Feast Type | Python Type | Description |
|------------|-------------|-------------|
| `Int32` | `int` | 32-bit signed integer |
| `Int64` | `int` | 64-bit signed integer |
| `Float32` | `float` | 32-bit floating point |
| `Float64` | `float` | 64-bit floating point |
| `String` | `str` | String/text value |
| `Bytes` | `bytes` | Binary data |
| `Bool` | `bool` | Boolean value |
| `UnixTimestamp` | `datetime` | Unix timestamp (nullable) |

### Array Types

All primitive types have corresponding array (list) types:

| Feast Type | Python Type | Description |
|------------|-------------|-------------|
| `Array(Int32)` | `List[int]` | List of 32-bit integers |
| `Array(Int64)` | `List[int]` | List of 64-bit integers |
| `Array(Float32)` | `List[float]` | List of 32-bit floats |
| `Array(Float64)` | `List[float]` | List of 64-bit floats |
| `Array(String)` | `List[str]` | List of strings |
| `Array(Bytes)` | `List[bytes]` | List of binary data |
| `Array(Bool)` | `List[bool]` | List of booleans |
| `Array(UnixTimestamp)` | `List[datetime]` | List of timestamps |

### Set Types

All primitive types (except Map) have corresponding set types for storing unique values:

| Feast Type | Python Type | Description |
|------------|-------------|-------------|
| `Set(Int32)` | `Set[int]` | Set of unique 32-bit integers |
| `Set(Int64)` | `Set[int]` | Set of unique 64-bit integers |
| `Set(Float32)` | `Set[float]` | Set of unique 32-bit floats |
| `Set(Float64)` | `Set[float]` | Set of unique 64-bit floats |
| `Set(String)` | `Set[str]` | Set of unique strings |
| `Set(Bytes)` | `Set[bytes]` | Set of unique binary data |
| `Set(Bool)` | `Set[bool]` | Set of unique booleans |
| `Set(UnixTimestamp)` | `Set[datetime]` | Set of unique timestamps |

**Note:** Set types automatically remove duplicate values. When converting from lists or other iterables to sets, duplicates are eliminated.

### Map Types

Map types allow storing dictionary-like data structures:

| Feast Type | Python Type | Description |
|------------|-------------|-------------|
| `Map` | `Dict[str, Any]` | Dictionary with string keys and any supported Feast type as values (including nested maps) |
| `Array(Map)` | `List[Dict[str, Any]]` | List of dictionaries |

**Note:** Map keys must always be strings. Map values can be any supported Feast type, including primitives, arrays, or nested maps.

## Complete Feature View Example

Below is a complete example showing how to define a feature view with all supported types:

```python
from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import (
    Int32, Int64, Float32, Float64, String, Bytes, Bool, UnixTimestamp,
    Array, Set, Map
)

# Define a data source
user_features_source = FileSource(
    path="data/user_features.parquet",
    timestamp_field="event_timestamp",
)

# Define an entity
user = Entity(
    name="user_id",
    description="User identifier",
)

# Define a feature view with all supported types
user_features = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=1),
    schema=[
        # Primitive types
        Field(name="age", dtype=Int32),
        Field(name="account_balance", dtype=Int64),
        Field(name="transaction_amount", dtype=Float32),
        Field(name="credit_score", dtype=Float64),
        Field(name="username", dtype=String),
        Field(name="profile_picture", dtype=Bytes),
        Field(name="is_active", dtype=Bool),
        Field(name="last_login", dtype=UnixTimestamp),
        
        # Array types
        Field(name="daily_steps", dtype=Array(Int32)),
        Field(name="transaction_history", dtype=Array(Int64)),
        Field(name="ratings", dtype=Array(Float32)),
        Field(name="portfolio_values", dtype=Array(Float64)),
        Field(name="favorite_items", dtype=Array(String)),
        Field(name="document_hashes", dtype=Array(Bytes)),
        Field(name="notification_settings", dtype=Array(Bool)),
        Field(name="login_timestamps", dtype=Array(UnixTimestamp)),
        
        # Set types (unique values only)
        Field(name="visited_pages", dtype=Set(String)),
        Field(name="unique_categories", dtype=Set(Int32)),
        Field(name="tag_ids", dtype=Set(Int64)),
        Field(name="preferred_languages", dtype=Set(String)),
        
        # Map types
        Field(name="user_preferences", dtype=Map),
        Field(name="metadata", dtype=Map),
        Field(name="activity_log", dtype=Array(Map)),
    ],
    source=user_features_source,
)
```

### Set Type Usage Examples

Sets store unique values and automatically remove duplicates:

```python
# Simple set
visited_pages = {"home", "products", "checkout", "products"}  # "products" appears twice
# Feast will store this as: {"home", "products", "checkout"}

# Integer set
unique_categories = {1, 2, 3, 2, 1}  # duplicates will be removed
# Feast will store this as: {1, 2, 3}

# Converting a list with duplicates to a set
tag_list = [100, 200, 300, 100, 200]
tag_ids = set(tag_list)  # {100, 200, 300}
```

### Map Type Usage Examples

Maps can store complex nested data structures:

```python
# Simple map
user_preferences = {
    "theme": "dark",
    "language": "en",
    "notifications_enabled": True,
    "font_size": 14
}

# Nested map
metadata = {
    "profile": {
        "bio": "Software engineer",
        "location": "San Francisco"
    },
    "stats": {
        "followers": 1000,
        "posts": 250
    }
}

# List of maps
activity_log = [
    {"action": "login", "timestamp": "2024-01-01T10:00:00", "ip": "192.168.1.1"},
    {"action": "purchase", "timestamp": "2024-01-01T11:30:00", "amount": 99.99},
    {"action": "logout", "timestamp": "2024-01-01T12:00:00"}
]
```

## Type System in Practice

The sections below explain how Feast uses its type system in different contexts.

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

### Historical feature retrieval

The Feast type system is typically not necessary when retrieving historical features.
A call to `get_historical_features` will return a `RetrievalJob` object, which allows the user to export the results to one of several possible locations: a Pandas dataframe, a pyarrow table, a data lake (e.g. S3 or GCS), or the offline store (e.g. a Snowflake table).
In all of these cases, the type conversion is handled natively by the offline store.
For example, a BigQuery query exposes a `to_dataframe` method that will automatically convert the result to a dataframe, without requiring any conversions within Feast.

### Feature serving

As mentioned above in the section on [materialization](#materialization), Feast persists feature values into the online store as `Value` proto objects.
A call to `get_online_features` will return an `OnlineResponse` object, which essentially wraps a bunch of `Value` protos with some metadata.
The `OnlineResponse` object can then be converted into a Python dictionary, which calls `feast_value_type_to_python_type` from `type_map.py`, a utility that converts the Feast internal types to Python native types.
