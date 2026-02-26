# Data Types in Feast

Feast frequently has to mediate data across platforms and systems, each with its own unique type system. 
To make this possible, Feast itself has a type system for all the types it is able to handle natively.

Feast's type system is built on top of [protobuf](https://github.com/protocolbuffers/protobuf). The messages that make up the type system can be found [here](https://github.com/feast-dev/feast/blob/master/protos/feast/types/Value.proto), and the corresponding python classes that wrap them can be found [here](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/types.py).

Feast supports the following categories of data types:

- **Primitive types**: numerical values (`Int32`, `Int64`, `Float32`, `Float64`), `String`, `Bytes`, `Bool`, and `UnixTimestamp`.
- **Array types**: ordered lists of any primitive type, e.g. `Array(Int64)`, `Array(String)`.
- **Set types**: unordered collections of unique values for any primitive type, e.g. `Set(String)`, `Set(Int64)`.
- **Map types**: dictionary-like structures with string keys and values that can be any supported Feast type (including nested maps), e.g. `Map`, `Array(Map)`.
- **JSON type**: opaque JSON data stored as a string at the proto level but semantically distinct from `String` — backends use native JSON types (`jsonb`, `VARIANT`, etc.), e.g. `Json`, `Array(Json)`.
- **Struct type**: schema-aware structured type with named, typed fields. Unlike `Map` (which is schema-free), a `Struct` declares its field names and their types, enabling schema validation, e.g. `Struct({"name": String, "age": Int32})`.

For a complete reference with examples, see [Type System](../../reference/type-system.md).

Each feature or schema field in Feast is associated with a data type, which is stored in Feast's [registry](registry.md). These types are also used to ensure that Feast operates on values correctly (e.g. making sure that timestamp columns used for [point-in-time correct joins](point-in-time-joins.md) actually have the timestamp type).

As a result, each system that Feast interacts with needs a way to translate data types from the native platform into a Feast type. E.g., Snowflake SQL types are converted to Feast types [here](https://rtd.feast.dev/en/master/feast.html#feast.type_map.snowflake_python_type_to_feast_value_type). The onus is therefore on authors of offline or online store connectors to make sure that this type mapping happens correctly.

### Backend Type Mapping for Complex Types

Map, JSON, and Struct types are supported across all major Feast backends:

| Backend | Native Type | Feast Type |
|---------|-------------|------------|
| PostgreSQL | `jsonb` | `Map`, `Json`, `Struct` |
| PostgreSQL | `jsonb[]` | `Array(Map)` |
| Snowflake | `VARIANT`, `OBJECT` | `Map` |
| Snowflake | `JSON` | `Json` |
| Redshift | `SUPER` | `Map` |
| Redshift | `json` | `Json` |
| BigQuery | `JSON` | `Json` |
| BigQuery | `STRUCT`, `RECORD` | `Struct` |
| Spark | `map<string,string>` | `Map` |
| Spark | `array<map<string,string>>` | `Array(Map)` |
| Spark | `struct<...>` | `Struct` |
| Spark | `array<struct<...>>` | `Array(Struct(...))` |
| MSSQL | `nvarchar(max)` | `Map`, `Json`, `Struct` |
| DynamoDB | Proto bytes | `Map`, `Json`, `Struct` |
| Redis | Proto bytes | `Map`, `Json`, `Struct` |
| Milvus | `VARCHAR` (serialized) | `Map`, `Json`, `Struct` |

**Note**: When the backend native type is ambiguous (e.g., `jsonb` could be `Map`, `Json`, or `Struct`), the **schema-declared Feast type takes precedence**. The backend-to-Feast type mappings above are only used for schema inference when no explicit type is provided.

**Note**: Feast currently does *not* support a null type in its type system.