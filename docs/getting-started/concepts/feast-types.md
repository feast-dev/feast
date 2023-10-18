# Data Types in Feast

Feast frequently has to mediate data across platforms and systems, each with its own unique type system. 
To make this possible, Feast itself has a type system for all the types it is able to handle natively.

Feast's type system is built on top of [protobuf](https://github.com/protocolbuffers/protobuf). The messages that make up the type system can be found [here](https://github.com/feast-dev/feast/blob/master/protos/feast/types/Value.proto), and the corresponding python classes that wrap them can be found [here](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/types.py).

Feast supports primitive data types (numerical values, strings, bytes, booleans and timestamps). The only complex data type Feast supports is Arrays, and arrays cannot contain other arrays.

Each feature or schema field in Feast is associated with a data type, which is stored in Feast's [registry](registry.md). These types are also used to ensure that Feast operates on values correctly (e.g. making sure that timestamp columns used for [point-in-time correct joins](point-in-time-joins.md) actually have the timestamp type).

As a result, each system that feast interacts with needs a way to translate data types from the native platform, into a feast type. E.g., Snowflake SQL types are converted to Feast types [here](https://rtd.feast.dev/en/master/feast.html#feast.type_map.snowflake_python_type_to_feast_value_type). The onus is therefore on authors of offline or online store connectors to make sure that this type mapping happens correctly.

**Note**: Feast currently does *not* support a null type in its type system.