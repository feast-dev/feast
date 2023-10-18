DROP FUNCTION IF EXISTS feast_PROJECT_NAME_snowflake_binary_to_bytes_proto(BINARY);

DROP FUNCTION IF EXISTS feast_PROJECT_NAME_snowflake_varchar_to_string_proto(VARCHAR);

DROP FUNCTION IF EXISTS feast_PROJECT_NAME_snowflake_number_to_int32_proto(NUMBER);

DROP FUNCTION IF EXISTS feast_PROJECT_NAME_snowflake_number_to_int64_proto(NUMBER);

DROP FUNCTION IF EXISTS feast_PROJECT_NAME_snowflake_float_to_double_proto(DOUBLE);

DROP FUNCTION IF EXISTS feast_PROJECT_NAME_snowflake_boolean_to_bool_proto(BOOLEAN);

DROP FUNCTION IF EXISTS feast_PROJECT_NAME_snowflake_timestamp_to_unix_timestamp_proto(NUMBER);

DROP FUNCTION IF EXISTS feast_PROJECT_NAME_serialize_entity_keys(ARRAY, ARRAY, ARRAY);

DROP FUNCTION IF EXISTS feast_PROJECT_NAME_entity_key_proto_to_string(ARRAY, ARRAY, ARRAY)
