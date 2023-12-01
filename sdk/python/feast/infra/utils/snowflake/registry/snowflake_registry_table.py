# -*- coding: utf-8 -*-

"""
The table names and column types are following the creation detail listed 
in "snowflake_table_creation.sql".

Snowflake Reference:
1, ResultMetadata: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#label-python-connector-resultmetadata-object
2, Type Codes: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#label-python-connector-type-codes
----------------------------------------------
type_code	String Representation	Data Type
0	FIXED	NUMBER/INT
1	REAL	REAL
2	TEXT	VARCHAR/STRING
3	DATE	DATE
4	TIMESTAMP	TIMESTAMP
5	VARIANT	VARIANT
6	TIMESTAMP_LTZ	TIMESTAMP_LTZ
7	TIMESTAMP_TZ	TIMESTAMP_TZ
8	TIMESTAMP_NTZ	TIMESTAMP_TZ
9	OBJECT	OBJECT
10	ARRAY	ARRAY
11	BINARY	BINARY
12	TIME	TIME
13	BOOLEAN	BOOLEAN
----------------------------------------------

(last update: 2023-11-30)

"""

snowflake_registry_table_names_and_column_types = {
    "DATA_SOURCES": {
        "DATA_SOURCE_NAME": {"type_code": 2, "type": "VARCHAR"},
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
        "DATA_SOURCE_PROTO": {"type_code": 11, "type": "BINARY"},
    },
    "ENTITIES": {
        "ENTITY_NAME": {"type_code": 2, "type": "VARCHAR"},
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
        "ENTITY_PROTO": {"type_code": 11, "type": "BINARY"},
    },
    "FEAST_METADATA": {
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "METADATA_KEY": {"type_code": 2, "type": "VARCHAR"},
        "METADATA_VALUE": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
    },
    "FEATURE_SERVICES": {
        "FEATURE_SERVICE_NAME": {"type_code": 2, "type": "VARCHAR"},
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
        "FEATURE_SERVICE_PROTO": {"type_code": 11, "type": "BINARY"},
    },
    "FEATURE_VIEWS": {
        "FEATURE_VIEW_NAME": {"type_code": 2, "type": "VARCHAR"},
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
        "FEATURE_VIEW_PROTO": {"type_code": 11, "type": "BINARY"},
        "MATERIALIZED_INTERVALS": {"type_code": 11, "type": "BINARY"},
        "USER_METADATA": {"type_code": 11, "type": "BINARY"},
    },
    "MANAGED_INFRA": {
        "INFRA_NAME": {"type_code": 2, "type": "VARCHAR"},
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
        "INFRA_PROTO": {"type_code": 11, "type": "BINARY"},
    },
    "ON_DEMAND_FEATURE_VIEWS": {
        "ON_DEMAND_FEATURE_VIEW_NAME": {"type_code": 2, "type": "VARCHAR"},
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
        "ON_DEMAND_FEATURE_VIEW_PROTO": {"type_code": 11, "type": "BINARY"},
        "USER_METADATA": {"type_code": 11, "type": "BINARY"},
    },
    "REQUEST_FEATURE_VIEWS": {
        "REQUEST_FEATURE_VIEW_NAME": {"type_code": 2, "type": "VARCHAR"},
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
        "REQUEST_FEATURE_VIEW_PROTO": {"type_code": 11, "type": "BINARY"},
        "USER_METADATA": {"type_code": 11, "type": "BINARY"},
    },
    "SAVED_DATASETS": {
        "SAVED_DATASET_NAME": {"type_code": 2, "type": "VARCHAR"},
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
        "SAVED_DATASET_PROTO": {"type_code": 11, "type": "BINARY"},
    },
    "STREAM_FEATURE_VIEWS": {
        "STREAM_FEATURE_VIEW_NAME": {"type_code": 2, "type": "VARCHAR"},
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
        "STREAM_FEATURE_VIEW_PROTO": {"type_code": 11, "type": "BINARY"},
        "USER_METADATA": {"type_code": 11, "type": "BINARY"},
    },
    "VALIDATION_REFERENCES": {
        "VALIDATION_REFERENCE_NAME": {"type_code": 2, "type": "VARCHAR"},
        "PROJECT_ID": {"type_code": 2, "type": "VARCHAR"},
        "LAST_UPDATED_TIMESTAMP": {"type_code": 6, "type": "TIMESTAMP_LTZ"},
        "VALIDATION_REFERENCE_PROTO": {"type_code": 11, "type": "BINARY"},
    },
}
