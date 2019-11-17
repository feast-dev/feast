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


import logging

import os
import pandas as pd
from math import ceil
from multiprocessing import Process, Queue, cpu_count
from typing import List, Optional
from collections import OrderedDict
from typing import Dict
from feast.source import Source
from feast.type_map import dtype_to_value_type
from pandas.api.types import is_datetime64_ns_dtype
from feast.entity import Entity
from feast.feature import Feature, Field
from feast.core.FeatureSet_pb2 import FeatureSetSpec as FeatureSetSpecProto
from feast.types import FeatureRow_pb2 as FeatureRow
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration
from kafka import KafkaProducer
from tqdm import tqdm
from feast.type_map import pandas_dtype_to_feast_value_type
from feast.types import FeatureRow_pb2 as FeatureRowProto, Field_pb2 as FieldProto
from feast.type_map import pd_value_to_proto_value
from google.protobuf.json_format import MessageToJson
import yaml
from google.protobuf import json_format
from feast.source import KafkaSource
from feast.type_map import DATETIME_COLUMN
from feast.loaders import yaml as feast_yaml


class FeatureSet:
    """
    Represents a collection of features.
    """

    def __init__(
        self,
        name: str,
        features: List[Feature] = None,
        entities: List[Entity] = None,
        source: Source = None,
        max_age: Optional[Duration] = None,
    ):
        self._name = name
        self._fields = OrderedDict()  # type: Dict[str, Field]
        if features is not None:
            self.features = features
        if entities is not None:
            self.entities = entities
        if source is None:
            self._source = None
        else:
            self._source = source
        self._max_age = max_age
        self._version = None
        self._client = None
        self._busy_ingesting = False
        self._is_dirty = True

    def __eq__(self, other):
        if not isinstance(other, FeatureSet):
            return NotImplemented

        for key in self.fields.keys():
            if key not in other.fields.keys() or self.fields[key] != other.fields[key]:
                return False

        if self.name != other.name or self.max_age != other.max_age:
            return False
        return True

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    @property
    def fields(self) -> Dict[str, Field]:
        """
        Returns a dict of fields from this feature set
        """
        return self._fields

    @property
    def features(self) -> List[Feature]:
        """
        Returns a list of features from this feature set
        """
        return [field for field in self._fields.values() if isinstance(field, Feature)]

    @features.setter
    def features(self, features: List[Feature]):
        for feature in features:
            if not isinstance(feature, Feature):
                raise Exception("object type is not a Feature: " + str(type(feature)))

        for key in list(self._fields.keys()):
            if isinstance(self._fields[key], Feature):
                del self._fields[key]

        if features is not None:
            self._add_fields(features)

    @property
    def entities(self) -> List[Entity]:
        """
        Returns list of entities from this feature set
        """
        return [field for field in self._fields.values() if isinstance(field, Entity)]

    @entities.setter
    def entities(self, entities: List[Entity]):
        for entity in entities:
            if not isinstance(entity, Entity):
                raise Exception("object type is not na Entity: " + str(type(entity)))

        for key in list(self._fields.keys()):
            if isinstance(self._fields[key], Entity):
                del self._fields[key]

        if entities is not None:
            self._add_fields(entities)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source: Source):
        self._source = source

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def max_age(self):
        return self._max_age

    @max_age.setter
    def max_age(self, max_age):
        self._max_age = max_age

    @property
    def is_dirty(self):
        return self._is_dirty

    def add(self, resource):
        """
        Adds a resource (Feature, Entity) to this Feature Set.
        Does not register the updated Feature Set with Feast Core
        :param resource: A resource can be either a Feature or an Entity object
        :return:
        """
        if resource.name in self._fields.keys():
            raise ValueError(
                'could not add field "'
                + resource.name
                + '" since it already exists in feature set "'
                + self._name
                + '"'
            )

        if issubclass(type(resource), Field):
            return self._set_field(resource)

        raise ValueError("Could not identify the resource being added")

    def _set_field(self, field: Field):
        self._fields[field.name] = field
        return

    def drop(self, name: str):
        """
        Removes a Feature or Entity from a Feature Set
        :param name: Name of Feature or Entity to be removed
        """
        if name not in self._fields:
            raise ValueError("Could not find field " + name + ", no action taken")
        if name in self._fields:
            del self._fields[name]
            return

    def _add_fields(self, fields: List[Field]):
        """
        Adds multiple Fields to a Feature Set
        :param fields: List of Feature or Entity Objects
        """
        for field in fields:
            self.add(field)

    def infer_fields_from_df(
        self,
        df: pd.DataFrame,
        entities: Optional[List[Entity]] = None,
        features: Optional[List[Feature]] = None,
        replace_existing_features: bool = False,
        replace_existing_entities: bool = False,
        discard_unused_fields: bool = False,
    ):
        """
        Adds fields (Features or Entities) to a feature set based on the schema
        of a Datatframe. Only Pandas dataframes are supported. All columns are
        detected as features, so setting at least one entity manually is
        advised.

        :param df: Pandas dataframe to read schema from
        :param entities: List of entities that will be set manually and not
        inferred. These will take precedence over any existing entities or
        entities found in the dataframe.
        :param features: List of features that will be set manually and not
        inferred. These will take precedence over any existing feature or
        features found in the dataframe.
        :param discard_unused_fields: Boolean flag. Setting this to True will
        discard any existing fields that are not found in the dataset or
        provided by the user
        :param replace_existing_features: Boolean flag. If true, will replace
        existing features in this feature set with features found in dataframe.
        If false, will skip conflicting features
        :param replace_existing_entities: Boolean flag. If true, will replace
        existing entities in this feature set with features found in dataframe.
        If false, will skip conflicting entities
        """

        if entities is None:
            entities = list()
        if features is None:
            features = list()

        # Validate whether the datetime column exists with the right name
        if DATETIME_COLUMN not in df:
            raise Exception("No column 'datetime'")

        # Validate the data type for the datetime column
        if not is_datetime64_ns_dtype(df.dtypes[DATETIME_COLUMN]):
            raise Exception(
                "Column 'datetime' does not have the correct type: datetime64[ns]"
            )

        # Create dictionary of fields that will not be inferred (manually set)
        provided_fields = OrderedDict()

        for field in entities + features:
            if not isinstance(field, Field):
                raise Exception(f"Invalid field object type provided {type(field)}")
            if field.name not in provided_fields:
                provided_fields[field.name] = field
            else:
                raise Exception(f"Duplicate field name detected {field.name}.")

        new_fields = self._fields.copy()
        output_log = ""

        # Add in provided fields
        for name, field in provided_fields.items():
            if name in new_fields.keys():
                upsert_message = "created"
            else:
                upsert_message = "updated (replacing an existing field)"

            output_log += (
                f"{type(field).__name__} {field.name}"
                f"({field.dtype}) manually {upsert_message}.\n"
            )
            new_fields[name] = field

        # Iterate over all of the columns and create features
        for column in df.columns:
            column = column.strip()

            # Skip datetime column
            if DATETIME_COLUMN in column:
                continue

            # Skip user provided fields
            if column in provided_fields.keys():
                continue

            # Only overwrite conflicting fields if replacement is allowed
            if column in new_fields:
                if (
                    isinstance(self._fields[column], Feature)
                    and not replace_existing_features
                ):
                    continue

                if (
                    isinstance(self._fields[column], Entity)
                    and not replace_existing_entities
                ):
                    continue

            # Store this field as a feature
            new_fields[column] = Feature(
                name=column, dtype=pandas_dtype_to_feast_value_type(df[column].dtype)
            )
            output_log += f"{type(new_fields[column]).__name__} {new_fields[column].name} ({new_fields[column].dtype}) added from dataframe.\n"

        # Discard unused fields from feature set
        if discard_unused_fields:
            keys_to_remove = []
            for key in new_fields.keys():
                if not (key in df.columns or key in provided_fields.keys()):
                    output_log += f"{type(new_fields[key]).__name__} {new_fields[key].name} ({new_fields[key].dtype}) removed because it is unused.\n"
                    keys_to_remove.append(key)
            for key in keys_to_remove:
                del new_fields[key]

        # Update feature set
        self._fields = new_fields
        print(output_log)

    def _update_from_feature_set(self, feature_set, is_dirty: bool = True):

        self.name = feature_set.name
        self.version = feature_set.version
        self.source = feature_set.source
        self.max_age = feature_set.max_age
        self.features = feature_set.features
        self.entities = feature_set.entities

        self._is_dirty = is_dirty

    def get_kafka_source_brokers(self) -> str:
        if self.source and self.source.source_type is "Kafka":
            return self.source.brokers
        raise Exception("Source type could not be identified")

    def get_kafka_source_topic(self) -> str:
        if self.source and self.source.source_type == "Kafka":
            return self.source.topic
        raise Exception("Source type could not be identified")

    def is_valid(self):
        """
        Validates the state of a feature set locally
        :return: (bool, str) True if valid, false if invalid. Contains a message
        string with a reason
        """
        if len(self.entities) == 0:
            return False, f"No entities found in feature set {self.name}"
        return True, ""

    @classmethod
    def from_yaml(cls, yml):
        return cls.from_dict(feast_yaml.yaml_loader(yml, load_single=True))

    @classmethod
    def from_dict(cls, fs_dict):
        if ("kind" not in fs_dict) and (fs_dict["kind"].strip() != "feature_set"):
            raise Exception(f"Resource kind is not a feature set {str(fs_dict)}")
        feature_set_proto = json_format.ParseDict(
            fs_dict, FeatureSetSpecProto(), ignore_unknown_fields=True
        )
        return cls.from_proto(feature_set_proto)

    @classmethod
    def from_proto(cls, feature_set_proto: FeatureSetSpecProto):
        feature_set = cls(
            name=feature_set_proto.name,
            features=[
                Feature.from_proto(feature) for feature in feature_set_proto.features
            ],
            entities=[
                Entity.from_proto(entity) for entity in feature_set_proto.entities
            ],
            max_age=feature_set_proto.max_age,
            source=(
                None
                if feature_set_proto.source.type == 0
                else Source.from_proto(feature_set_proto.source)
            ),
        )
        feature_set._version = feature_set_proto.version
        feature_set._is_dirty = False
        return feature_set

    def to_proto(self) -> FeatureSetSpecProto:
        return FeatureSetSpecProto(
            name=self.name,
            version=self.version,
            max_age=self.max_age,
            source=self.source.to_proto() if self.source is not None else None,
            features=[
                field.to_proto()
                for field in self._fields.values()
                if type(field) == Feature
            ],
            entities=[
                field.to_proto()
                for field in self._fields.values()
                if type(field) == Entity
            ],
        )
