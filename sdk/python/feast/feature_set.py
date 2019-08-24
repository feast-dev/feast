# Copyright 2018 The Feast Authors
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

from feast.core import FeatureSet_pb2 as FeatureSet_pb
from typing import List, Dict

import enum


class ValueType(enum.Enum):
    """
    Feature value type
    """

    UNKNOWN = 0
    BYTES = 1
    STRING = 2
    INT32 = 3
    INT64 = 4
    DOUBLE = 5
    FLOAT = 6
    BOOL = 7
    BYTES_LIST = 11
    STRING_LIST = 12
    INT32_LIST = 13
    INT64_LIST = 14
    DOUBLE_LIST = 15
    FLOAT_LIST = 16
    BOOL_LIST = 17


class Feature:
    def __init__(self, name: str, dtype: ValueType, is_key: bool = False):
        self._name = name
        self._dtype = dtype
        self._is_key = is_key

    @property
    def name(self):
        return self._name

    @property
    def dtype(self):
        return self._dtype


class FeatureSet:
    """
    Represent a collection of features.
    """

    def __init__(self, name: str, features: List[Feature] = None):
        self._features = dict()  # type: Dict[str, Feature]
        if features is not None:
            self._add_features(features)
        self._name = name
        self._version = None

    @property
    def features(self) -> Dict[str, Feature]:
        """
        Return list of features of this feature set
        """
        return self._features

    def add(self, name: str, dtype: ValueType):
        if name in self._features.keys():
            raise ValueError(
                'could not add feature "'
                + name
                + '" since it already exists in feature set "'
                + self._name
                + '"'
            )
        self._features[name] = Feature(name=name, dtype=dtype)

    def drop(self, name: str):
        if name in self._features:
            del self._features[name]

    def _add_features(self, features: List[Feature]):
        for feature in features:
            self.add(name=feature.name, dtype=feature.dtype)
