# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import enum
import re
from typing import Optional, Type, Union

from feast.protos.feast.types.Value_pb2 import (
    BoolList,
    BytesList,
    DoubleList,
    FloatList,
    Int32List,
    Int64List,
    StringList,
)


class ValueType(enum.Enum):
    """
    Feature value type. Used to define data types in Feature Tables.
    """

    UNKNOWN = 0
    BYTES = 1
    STRING = 2
    INT32 = 3
    INT64 = 4
    DOUBLE = 5
    FLOAT = 6
    BOOL = 7
    UNIX_TIMESTAMP = 8
    BYTES_LIST = 11
    STRING_LIST = 12
    INT32_LIST = 13
    INT64_LIST = 14
    DOUBLE_LIST = 15
    FLOAT_LIST = 16
    BOOL_LIST = 17
    UNIX_TIMESTAMP_LIST = 18
    NULL = 19


ListType = Union[
    Type[BoolList],
    Type[BytesList],
    Type[DoubleList],
    Type[FloatList],
    Type[Int32List],
    Type[Int64List],
    Type[StringList],
]


# This regex ensures that the input string is between 1 and 63 characters long. It allows an optional DNS subdomain (prefix)
# followed by a tag name. The optional prefix consists of alphanumeric characters and dashes, separated by periods,
# and must follow DNS subdomain rules. The tag name, which is required, must start and end with an alphanumeric character,
# can contain alphanumeric characters, dashes, underscores,and periods, and if a prefix is present, it must be separated from
# the name by a /.
# The 'feast.dev/' prefix is reserved.
tag_key_regex = r"^(?=.{1,63}$)(?!feast\.dev/)(?:(?:[a-z0-9]([-a-z0-9]*[a-z0-9])?\.)*[a-z0-9]([-a-z0-9]*[a-z0-9])?\/)?[a-z0-9A-Z]([a-z0-9A-Z._-]*[a-z0-9A-Z])?$"

# This regex ensures that the input is either empty or contains at most 63 characters, consisting of alphanumeric characters,
# periods (.), underscores (_), or hyphens (-). It must start and end with an alphanumeric character,
# with any internal periods, underscores, or hyphens allowed between alphanumeric characters.
tag_value_regex = r"^(?=.{0,63}$)(?:[a-zA-Z0-9](?:[a-zA-Z0-9._-]*[a-zA-Z0-9])?)?$"


def is_valid_tag_key(s: str):
    return bool(re.match(tag_key_regex, s))


def is_valid_tag_value(s: str):
    return bool(re.match(tag_value_regex, s))


def validate_tags(tags: Optional[dict[str, str]]):
    if tags:
        for key, value in tags.items():
            if not is_valid_tag_key(key):
                raise ValueError(
                    f"Invalid tag key: '{key}': name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '{tag_key_regex}'"
                )
            if not is_valid_tag_value(value):
                raise ValueError(
                    f"Invalid tag value: '{value}' in '{key}: {value}': name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '{tag_value_regex}'"
                )
