# Copyright 2020 The Feast Authors
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

from typing import Dict, List, MutableMapping, Optional, Union

import yaml
from google.protobuf import json_format
from google.protobuf.duration_pb2 import Duration
from google.protobuf.json_format import MessageToDict, MessageToJson
from google.protobuf.timestamp_pb2 import Timestamp

from feast.core.FeatureTable_pb2 import FeatureTable as FeatureTableProto
from feast.core.FeatureTable_pb2 import FeatureTableMeta as FeatureTableMetaProto
from feast.core.FeatureTable_pb2 import FeatureTableSpec as FeatureTableSpecProto
from feast.data_source import (
    BigQuerySource,
    DataSource,
    FileSource,
    KafkaSource,
    KinesisSource,
)
from feast.feature import Feature
from feast.loaders import yaml as feast_yaml
from feast.value_type import ValueType


class FeatureTable:
    """
    Represents a collection of features and associated metadata.
    """

    def __init__(
        self,
        name: str,
        entities: List[str],
        features: List[Feature],
        batch_source: Union[BigQuerySource, FileSource] = None,
        stream_source: Optional[Union[KafkaSource, KinesisSource]] = None,
        max_age: Optional[Duration] = None,
        labels: Optional[MutableMapping[str, str]] = None,
    ):
        self._name = name
        self._entities = entities
        self._features = features
        self._batch_source = batch_source
        self._stream_source = stream_source
        if labels is None:
            self._labels = dict()  # type: MutableMapping[str, str]
        else:
            self._labels = labels

        self._max_age = max_age
        self._created_timestamp: Optional[Timestamp] = None
        self._last_updated_timestamp: Optional[Timestamp] = None

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __eq__(self, other):
        if not isinstance(other, FeatureTable):
            raise TypeError(
                "Comparisons should only involve FeatureTable class objects."
            )

        if (
            self.labels != other.labels
            or self.name != other.name
            or self.max_age != other.max_age
        ):
            return False

        if sorted(self.entities) != sorted(other.entities):
            return False
        if sorted(self.features) != sorted(other.features):
            return False
        if self.batch_source != other.batch_source:
            return False
        if self.stream_source != other.stream_source:
            return False

        return True

    @property
    def name(self):
        """
        Returns the name of this feature table
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """
        Sets the name of this feature table
        """
        self._name = name

    @property
    def entities(self):
        """
        Returns the entities of this feature table
        """
        return self._entities

    @entities.setter
    def entities(self, entities: List[str]):
        """
        Sets the entities of this feature table
        """
        self._entities = entities

    @property
    def features(self):
        """
        Returns the features of this feature table
        """
        return self._features

    @features.setter
    def features(self, features: List[Feature]):
        """
        Sets the features of this feature table
        """
        self._features = features

    @property
    def batch_source(self):
        """
        Returns the batch source of this feature table
        """
        return self._batch_source

    @batch_source.setter
    def batch_source(self, batch_source: Union[BigQuerySource, FileSource]):
        """
        Sets the batch source of this feature table
        """
        self._batch_source = batch_source

    @property
    def stream_source(self):
        """
        Returns the stream source of this feature table
        """
        return self._stream_source

    @stream_source.setter
    def stream_source(self, stream_source: Union[KafkaSource, KinesisSource]):
        """
        Sets the stream source of this feature table
        """
        self._stream_source = stream_source

    @property
    def max_age(self):
        """
        Returns the maximum age of this feature table. This is the total maximum
        amount of staleness that will be allowed during feature retrieval for
        each specific feature that is looked up.
        """
        return self._max_age

    @max_age.setter
    def max_age(self, max_age: Duration):
        """
        Set the maximum age for this feature table
        """
        self._max_age = max_age

    @property
    def labels(self):
        """
        Returns the labels of this feature table. This is the user defined metadata
        defined as a dictionary.
        """
        return self._labels

    @labels.setter
    def labels(self, labels: MutableMapping[str, str]):
        """
        Set the labels for this feature table
        """
        self._labels = labels

    @property
    def created_timestamp(self):
        """
        Returns the created_timestamp of this feature table
        """
        return self._created_timestamp

    @property
    def last_updated_timestamp(self):
        """
        Returns the last_updated_timestamp of this feature table
        """
        return self._last_updated_timestamp

    def add_feature(self, feature: Feature):
        """
        Adds a new feature to the feature table.
        """
        self.features.append(feature)

    def is_valid(self):
        """
        Validates the state of a feature table locally. Raises an exception
        if feature table is invalid.
        """

        if not self.name:
            raise ValueError("No name found in feature table.")

        if not self.entities:
            raise ValueError("No entities found in feature table {self.name}.")

    @classmethod
    def from_yaml(cls, yml: str):
        """
        Creates a feature table from a YAML string body or a file path

        Args:
            yml: Either a file path containing a yaml file or a YAML string

        Returns:
            Returns a FeatureTable object based on the YAML file
        """

        return cls.from_dict(feast_yaml.yaml_loader(yml, load_single=True))

    @classmethod
    def from_dict(cls, ft_dict):
        """
        Creates a feature table from a dict

        Args:
            ft_dict: A dict representation of a feature table

        Returns:
            Returns a FeatureTable object based on the feature table dict
        """

        feature_table_proto = json_format.ParseDict(
            ft_dict, FeatureTableProto(), ignore_unknown_fields=True
        )

        return cls.from_proto(feature_table_proto)

    @classmethod
    def from_proto(cls, feature_table_proto: FeatureTableProto):
        """
        Creates a feature table from a protobuf representation of a feature table

        Args:
            feature_table_proto: A protobuf representation of a feature table

        Returns:
            Returns a FeatureTableProto object based on the feature table protobuf
        """

        feature_table = cls(
            name=feature_table_proto.spec.name,
            entities=[entity for entity in feature_table_proto.spec.entities],
            features=[
                Feature(
                    name=feature.name,
                    dtype=ValueType(feature.value_type),
                    labels=feature.labels,
                )
                for feature in feature_table_proto.spec.features
            ],
            labels=feature_table_proto.spec.labels,
            max_age=(
                None
                if feature_table_proto.spec.max_age.seconds == 0
                and feature_table_proto.spec.max_age.nanos == 0
                else feature_table_proto.spec.max_age
            ),
            batch_source=DataSource.from_proto(feature_table_proto.spec.batch_source),
            stream_source=(
                None
                if not feature_table_proto.spec.stream_source.ByteSize()
                else DataSource.from_proto(feature_table_proto.spec.stream_source)
            ),
        )

        feature_table._created_timestamp = feature_table_proto.meta.created_timestamp

        return feature_table

    def to_proto(self) -> FeatureTableProto:
        """
        Converts an feature table object to its protobuf representation

        Returns:
            FeatureTableProto protobuf
        """

        meta = FeatureTableMetaProto(
            created_timestamp=self.created_timestamp,
            last_updated_timestamp=self.last_updated_timestamp,
        )

        spec = FeatureTableSpecProto(
            name=self.name,
            entities=self.entities,
            features=[
                feature.to_proto() if type(feature) == Feature else feature
                for feature in self.features
            ],
            labels=self.labels,
            max_age=self.max_age,
            batch_source=(
                self.batch_source.to_proto()
                if issubclass(type(self.batch_source), DataSource)
                else self.batch_source
            ),
            stream_source=(
                self.stream_source.to_proto()
                if issubclass(type(self.stream_source), DataSource)
                else self.stream_source
            ),
        )

        return FeatureTableProto(spec=spec, meta=meta)

    def to_spec_proto(self) -> FeatureTableSpecProto:
        """
        Converts an FeatureTableProto object to its protobuf representation.
        Used when passing FeatureTableSpecProto object to Feast request.

        Returns:
            FeatureTableSpecProto protobuf
        """

        spec = FeatureTableSpecProto(
            name=self.name,
            entities=self.entities,
            features=[
                feature.to_proto() if type(feature) == Feature else feature
                for feature in self.features
            ],
            labels=self.labels,
            max_age=self.max_age,
            batch_source=(
                self.batch_source.to_proto()
                if issubclass(type(self.batch_source), DataSource)
                else self.batch_source
            ),
            stream_source=(
                self.stream_source.to_proto()
                if issubclass(type(self.stream_source), DataSource)
                else self.stream_source
            ),
        )

        return spec

    def to_dict(self) -> Dict:
        """
        Converts feature table to dict

        :return: Dictionary object representation of feature table
        """
        feature_table_dict = MessageToDict(self.to_proto())

        # Remove meta when empty for more readable exports
        if feature_table_dict["meta"] == {}:
            del feature_table_dict["meta"]

        return feature_table_dict

    def to_yaml(self):
        """
        Converts a feature table to a YAML string.

        :return: Feature table string returned in YAML format
        """
        feature_table_dict = self.to_dict()
        return yaml.dump(feature_table_dict, allow_unicode=True, sort_keys=False)

    def _update_from_feature_table(self, feature_table):
        """
        Deep replaces one feature table with another

        Args:
            feature_table: Feature table to use as a source of configuration
        """

        self.name = feature_table.name
        self.entities = feature_table.entities
        self.features = feature_table.features
        self.labels = feature_table.labels
        self.max_age = feature_table.max_age
        self.batch_source = feature_table.batch_source
        self.stream_source = feature_table.stream_source
        self._created_timestamp = feature_table.created_timestamp
        self._last_updated_timestamp = feature_table.last_updated_timestamp

    def __repr__(self):
        return f"FeatureTable <{self.name}>"
