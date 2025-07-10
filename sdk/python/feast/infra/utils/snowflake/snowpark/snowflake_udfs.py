import sys
from binascii import unhexlify

import numpy as np
import pandas
from _snowflake import vectorized

from feast.infra.key_encoding_utils import serialize_entity_key
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.type_map import (
    _convert_value_type_str_to_value_type,
    python_values_to_proto_values,
)
from feast.value_type import ValueType

"""
CREATE OR REPLACE FUNCTION feast_snowflake_binary_to_bytes_proto(df BINARY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_binary_to_bytes_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""
# ValueType.BYTES = 1
@vectorized(input=pandas.DataFrame)
def feast_snowflake_binary_to_bytes_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(df[0].to_numpy(), ValueType.BYTES),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_varchar_to_string_proto(df VARCHAR)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_varchar_to_string_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""
# ValueType.STRING = 2
@vectorized(input=pandas.DataFrame)
def feast_snowflake_varchar_to_string_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(df[0].to_numpy(), ValueType.STRING),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_array_bytes_to_list_bytes_proto(df ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_bytes_to_list_bytes_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""
# ValueType.STRING_LIST = 12
@vectorized(input=pandas.DataFrame)
def feast_snowflake_array_bytes_to_list_bytes_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    # Sometimes bytes come in as strings so we need to convert back to float
    numpy_arrays = np.asarray(df[0].to_list()).astype(bytes)

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(numpy_arrays, ValueType.BYTES_LIST),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_array_varchar_to_list_string_proto(df ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_varchar_to_list_string_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""


@vectorized(input=pandas.DataFrame)
def feast_snowflake_array_varchar_to_list_string_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(df[0].to_numpy(), ValueType.STRING_LIST),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_array_number_to_list_int32_proto(df ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_number_to_list_int32_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""


@vectorized(input=pandas.DataFrame)
def feast_snowflake_array_number_to_list_int32_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(df[0].to_numpy(), ValueType.INT32_LIST),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_array_number_to_list_int64_proto(df ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_number_to_list_int64_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""


@vectorized(input=pandas.DataFrame)
def feast_snowflake_array_number_to_list_int64_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(df[0].to_numpy(), ValueType.INT64_LIST),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_array_float_to_list_double_proto(df ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_float_to_list_double_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""


@vectorized(input=pandas.DataFrame)
def feast_snowflake_array_float_to_list_double_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    numpy_arrays = np.asarray(df[0].to_list()).astype(float)

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(numpy_arrays, ValueType.DOUBLE_LIST),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_array_boolean_to_list_bool_proto(df ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_boolean_to_list_bool_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""


@vectorized(input=pandas.DataFrame)
def feast_snowflake_array_boolean_to_list_bool_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(df[0].to_numpy(), ValueType.BOOL_LIST),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_array_timestamp_to_list_unix_timestamp_proto(df ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_array_timestamp_to_list_unix_timestamp_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""


@vectorized(input=pandas.DataFrame)
def feast_snowflake_array_timestamp_to_list_unix_timestamp_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    numpy_arrays = np.asarray(df[0].to_list()).astype(np.datetime64)

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(numpy_arrays, ValueType.UNIX_TIMESTAMP_LIST),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_number_to_int32_proto(df NUMBER)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_number_to_int32_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""
# ValueType.INT32 = 3
@vectorized(input=pandas.DataFrame)
def feast_snowflake_number_to_int32_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(df[0].to_numpy(), ValueType.INT32),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_number_to_int64_proto(df NUMBER)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_number_to_int64_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""
# ValueType.INT64 = 4
@vectorized(input=pandas.DataFrame)
def feast_snowflake_number_to_int64_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(df[0].to_numpy(), ValueType.INT64),
        )
    )
    return df


# All floating-point numbers stored as double
# https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#data-types-for-floating-point-numbers
"""
CREATE OR REPLACE FUNCTION feast_snowflake_float_to_double_proto(df DOUBLE)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_float_to_double_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""
# ValueType.FLOAT = 5 & ValueType.DOUBLE = 6
@vectorized(input=pandas.DataFrame)
def feast_snowflake_float_to_double_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(df[0].to_numpy(), ValueType.DOUBLE),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_boolean_to_bool_proto(df BOOLEAN)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_boolean_to_bool_boolean_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""
# ValueType.BOOL = 7
@vectorized(input=pandas.DataFrame)
def feast_snowflake_boolean_to_bool_boolean_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(df[0].to_numpy(), ValueType.BOOL),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_snowflake_timestamp_to_unix_timestamp_proto(df NUMBER)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_snowflake_timestamp_to_unix_timestamp_proto'
  IMPORTS = ('@feast_stage/feast.zip');
"""
# ValueType.UNIX_TIMESTAMP = 8
@vectorized(input=pandas.DataFrame)
def feast_snowflake_timestamp_to_unix_timestamp_proto(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    df = list(
        map(
            ValueProto.SerializeToString,
            python_values_to_proto_values(
                pandas.to_datetime(df[0], unit="ns").to_numpy(),
                ValueType.UNIX_TIMESTAMP,
            ),
        )
    )
    return df


"""
CREATE OR REPLACE FUNCTION feast_serialize_entity_keys(names ARRAY, data ARRAY, types ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_serialize_entity_keys'
  IMPORTS = ('@feast_stage/feast.zip')
"""
# converts 1 to n many entity keys to a single binary for lookups
@vectorized(input=pandas.DataFrame)
def feast_serialize_entity_keys(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    join_keys = create_entity_dict(df[0].values[0], df[2].values[0])

    df = pandas.DataFrame.from_dict(
        dict(zip(df[1].index, df[1].values)), orient="index", columns=df[0].values[0]
    )

    proto_values_by_column = {}
    for column, value_type in list(join_keys.items()):
        # BINARY is converted to a hex string, we need to convert back
        if value_type == ValueType.BYTES:
            proto_values = python_values_to_proto_values(
                list(map(unhexlify, df[column].tolist())), value_type
            )
        else:
            proto_values = python_values_to_proto_values(
                df[column].to_numpy(), value_type
            )

        proto_values_by_column.update({column: proto_values})

    serialized_entity_keys = [
        serialize_entity_key(
            EntityKeyProto(
                join_keys=join_keys,
                entity_values=[proto_values_by_column[k][idx] for k in join_keys],
            ),
            entity_key_serialization_version=3,
        )
        for idx in range(df.shape[0])
    ]
    return serialized_entity_keys


"""
CREATE OR REPLACE FUNCTION feast_entity_key_proto_to_string(names ARRAY, data ARRAY, types ARRAY)
  RETURNS BINARY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  PACKAGES = ('protobuf', 'pandas')
  HANDLER = 'feast.infra.utils.snowflake.snowpark.snowflake_udfs.feast_entity_key_proto_to_string'
  IMPORTS = ('@feast_stage/feast.zip')
"""
# converts 1 to n many entity keys to a single binary for lookups
@vectorized(input=pandas.DataFrame)
def feast_entity_key_proto_to_string(df):
    sys._xoptions["snowflake_partner_attribution"].append("feast")

    join_keys = create_entity_dict(df[0].values[0], df[2].values[0])

    df = pandas.DataFrame.from_dict(
        dict(zip(df[1].index, df[1].values)), orient="index", columns=df[0].values[0]
    )

    proto_values_by_column = {}
    for column, value_type in list(join_keys.items()):
        # BINARY is converted to a hex string, we need to convert back
        if value_type == ValueType.BYTES:
            proto_values = python_values_to_proto_values(
                list(map(unhexlify, df[column].tolist())), value_type
            )
        else:
            proto_values = python_values_to_proto_values(
                df[column].to_numpy(), value_type
            )

        proto_values_by_column.update({column: proto_values})

    serialized_entity_keys = [
        EntityKeyProto(
            join_keys=join_keys,
            entity_values=[proto_values_by_column[k][idx] for k in join_keys],
        ).SerializeToString()
        for idx in range(df.shape[0])
    ]
    return serialized_entity_keys


def create_entity_dict(names, types):
    return dict(
        zip(
            names,
            [_convert_value_type_str_to_value_type(type_str) for type_str in types],
        )
    )
