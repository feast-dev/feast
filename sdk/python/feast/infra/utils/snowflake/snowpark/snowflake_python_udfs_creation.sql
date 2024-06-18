CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_binary_to_bytes_proto(df BINARY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_binary_to_bytes_proto'
  IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_varchar_to_string_proto(df VARCHAR)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_varchar_to_string_proto'
  IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_array_bytes_to_list_bytes_proto(df ARRAY)
    RETURNS BINARY
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    PACKAGES = ('protobuf', 'pandas')
    HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_bytes_to_list_bytes_proto'
    IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_array_varchar_to_list_string_proto(df ARRAY)
    RETURNS BINARY
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    PACKAGES = ('protobuf', 'pandas')
    HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_varchar_to_list_string_proto'
    IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_array_number_to_list_int32_proto(df ARRAY)
    RETURNS BINARY
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    PACKAGES = ('protobuf', 'pandas')
    HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_number_to_list_int32_proto'
    IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_array_number_to_list_int64_proto(df ARRAY)
    RETURNS BINARY
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    PACKAGES = ('protobuf', 'pandas')
    HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_number_to_list_int64_proto'
    IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_array_float_to_list_double_proto(df ARRAY)
    RETURNS BINARY
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    PACKAGES = ('protobuf', 'pandas')
    HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_float_to_list_double_proto'
    IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_array_boolean_to_list_bool_proto(df ARRAY)
    RETURNS BINARY
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    PACKAGES = ('protobuf', 'pandas')
    HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_boolean_to_list_bool_proto'
    IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_array_timestamp_to_list_unix_timestamp_proto(df ARRAY)
    RETURNS BINARY
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    PACKAGES = ('protobuf', 'pandas')
    HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_timestamp_to_list_unix_timestamp_proto'
    IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_number_to_int32_proto(df NUMBER)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_number_to_int32_proto'
  IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_number_to_int64_proto(df NUMBER)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_number_to_int64_proto'
  IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_float_to_double_proto(df DOUBLE)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_float_to_double_proto'
  IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_boolean_to_bool_proto(df BOOLEAN)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_boolean_to_bool_boolean_proto'
  IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_snowflake_timestamp_to_unix_timestamp_proto(df NUMBER)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_timestamp_to_unix_timestamp_proto'
  IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_serialize_entity_keys(names ARRAY, data ARRAY, types ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_serialize_entity_keys'
  IMPORTS = ('@STAGE_HOLDER/feast.zip');

CREATE FUNCTION IF NOT EXISTS feast_PROJECT_NAME_entity_key_proto_to_string(names ARRAY, data ARRAY, types ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_entity_key_proto_to_string'
  IMPORTS = ('@STAGE_HOLDER/feast.zip')
