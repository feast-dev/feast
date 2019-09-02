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


import pandas as pd
from typing import List
from collections import OrderedDict
from typing import Dict
from feast.type_map import dtype_to_value_type
from feast.value_type import ValueType
from pandas.api.types import is_datetime64_ns_dtype
from feast.entity import Entity
from feast.feature import Feature
from feast.core.FeatureSet_pb2 import (
    FeatureSetSpec as FeatureSetProto,
    FeatureSpec as FeatureProto,
)
from feast.core.Source_pb2 import Source

DATETIME_COLUMN = "datetime"  # type: str


class FeatureSet:
    """
    Represents a collection of features.
    """

    def __init__(
        self,
        name: str,
        features: List[Feature] = None,
        entities: List[Entity] = None,
        max_age: int = -1,
    ):
        self._name = name
        self._features = OrderedDict()  # type: Dict[str, Feature]
        self._entities = OrderedDict()  # type: Dict[str, Entity]
        if features is not None:
            self._add_features(features)
        if entities is not None:
            self._add_entities(entities)
        self._max_age = max_age
        self._version = None
        self._client = None
        self._source = None

    @property
    def features(self) -> List[Feature]:
        """
        Returns a list of features from this feature set
        """
        return list(self._features.values())

    @property
    def entities(self) -> List[Entity]:
        """
        Returns list of entities from this feature set
        """
        return list(self._entities.values())

    @property
    def name(self):
        return self._name

    @property
    def source(self):
        return self._source

    @property
    def version(self):
        return self._version

    @property
    def max_age(self):
        return self._max_age

    def add(self, resource):
        """
        Adds a resource (Feature, Entity) to this Feature Set.
        Does not register the updated Feature Set with Feast Core
        :param resource: A resource can be either a Feature or an Entity object
        :return:
        """
        if (
            resource.name in self._features.keys()
            or resource.name in self._entities.keys()
        ):
            raise ValueError(
                'could not add field "'
                + resource.name
                + '" since it already exists in feature set "'
                + self._name
                + '"'
            )

        if isinstance(resource, Feature):
            return self._add_feature(resource)

        if isinstance(resource, Entity):
            return self._add_entity(resource)

        raise ValueError("Could not identify the resource being added")

    def _add_entity(self, entity: Entity):
        self._entities[entity.name] = entity
        return

    def _add_feature(self, feature: Feature):
        self._features[feature.name] = feature
        return

    def drop(self, name: str):
        """
        Removes a Feature or Entity from a Feature Set
        :param name: Name of Feature or Entity to be removed
        """
        if name not in self._features and name not in self._entities:
            raise ValueError("Could not find field " + name + ", no action taken")
        if name in self._features and name in self._entities:
            raise ValueError("Duplicate field found for " + name + "!")
        if name in self._features:
            del self._features[name]
            return
        if name in self._entities:
            del self._entities[name]
            return

    def _add_features(self, features: List[Feature]):
        """
        Adds multiple Features to a Feature Set
        :param features: List of Feature Objects
        """
        for feature in features:
            self.add(feature)

    def _add_entities(self, entities: List[Entity]):
        """
        Adds multiple Entities to a Feature Set
        :param entities: List of Entity Objects
        """
        for entity in entities:
            self.add(entity)

    def update_from_dataset(self, df: pd.DataFrame):
        """
        Updates Feature Set values based on the data set. Only Pandas dataframes are supported.
        :param df: Pandas dataframe containing datetime column, entity columns, and feature columns.
        """
        features = OrderedDict()
        entities = OrderedDict()
        existing_entities = None
        if self._client:
            existing_entities = self._client.entities

        # Validate whether the datetime column exists with the right name
        if DATETIME_COLUMN not in df:
            raise Exception("No column 'datetime'")

        # Validate the data type for the datetime column
        if not is_datetime64_ns_dtype(df.dtypes[DATETIME_COLUMN]):
            raise Exception(
                "Column 'datetime' does not have the correct type: datetime64[ns]"
            )

        # Iterate over all of the columns and detect their class (feature, entity) and type
        for column in df.columns:
            column = column.strip()

            # Validate whether the datetime column exists with the right name
            if DATETIME_COLUMN in column:
                continue

            # Test whether this column is an existing entity. If it is named exactly the same
            # as an existing entity then it will be detected as such
            if existing_entities and column in existing_entities:
                entity = existing_entities[column]

                # test whether registered entity type matches user provided type
                if entity.dtype == dtype_to_value_type(df[column].dtype):
                    # Store this field as an entity
                    entities[column] = entity
                    continue

            for feature in self.features:
                # Ignore features that already exist
                if feature.name == column:
                    continue

            # Store this field as a feature
            features[column] = Feature(
                name=column, dtype=dtype_to_value_type(df[column].dtype)
            )
        self._entities = entities
        self._features = features

    @classmethod
    def from_proto(cls, feature_set_proto: FeatureSetProto):
        feature_set = cls(
            name=feature_set_proto.name,
            features=[
                Feature.from_proto(feature) for feature in feature_set_proto.features
            ],
            entities=[
                Entity.from_proto(entity) for entity in feature_set_proto.entities
            ],
        )
        feature_set._version = feature_set_proto.version
        feature_set._source = feature_set_proto.source
        return feature_set

    def to_proto(self) -> FeatureSetProto:
        return FeatureSetProto(
            name=self.name,
            version=self.version,
            maxAge=self.max_age,
            source=Source(),
            features=[
                feature.to_proto() for featureName, feature in self._features.items()
            ],
            entities=[
                entity.to_proto() for entityName, entity in self._entities.items()
            ],
        )
