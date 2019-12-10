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
from typing import List, Optional
from collections import OrderedDict
from typing import Dict
from feast.source import Source
from pandas.api.types import is_datetime64_ns_dtype
from feast.entity import Entity
from feast.feature import Feature, Field
from feast.core.FeatureSet_pb2 import FeatureSetSpec as FeatureSetSpecProto
from google.protobuf.duration_pb2 import Duration
from feast.type_map import python_type_to_feast_value_type
from google.protobuf.json_format import MessageToJson
from google.protobuf import json_format
from feast.type_map import DATETIME_COLUMN
from feast.loaders import yaml as feast_yaml


class FeatureSet:
    """
    Represents a collection of features and associated metadata.
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

    def __repr__(self):
        shortname = "" + self._name
        if self._version:
            shortname += ":" + str(self._version).strip()
        return shortname

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
        """
        Sets the active features within this feature set

        Args:
            features: List of feature objects
        """
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
        """
        Sets the active entities within this feature set

        Args:
            entities: List of entities objects
        """
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
        """
        Returns the name of this feature set
        """
        return self._name

    @name.setter
    def name(self, name):
        """
        Sets the name of this feature set
        """
        self._name = name

    @property
    def source(self):
        """
        Returns the source of this feature set
        """
        return self._source

    @source.setter
    def source(self, source: Source):
        """
        Sets the source of this feature set
        """
        self._source = source

    @property
    def version(self):
        """
        Returns the version of this feature set
        """
        return self._version

    @version.setter
    def version(self, version):
        """
        Sets the version of this feature set
        """
        self._version = version

    @property
    def max_age(self):
        """
        Returns the maximum age of this feature set. This is the total maximum
        amount of staleness that will be allowed during feature retrieval for
        each specific feature row that is looked up.
        """
        return self._max_age

    @max_age.setter
    def max_age(self, max_age):
        """
        Set the maximum age for this feature set
        """
        self._max_age = max_age

    def add(self, resource):
        """
        Adds a resource (Feature, Entity) to this Feature Set.
        Does not register the updated Feature Set with Feast Core

        Args:
            resource: A resource can be either a Feature or an Entity object
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
        Removes a Feature or Entity from a Feature Set. This does not apply
        any changes to Feast Core until the apply() method is called.

        Args:
            name: Name of Feature or Entity to be removed
        """
        if name not in self._fields:
            raise ValueError("Could not find field " + name + ", no action taken")
        if name in self._fields:
            del self._fields[name]
            return

    def _add_fields(self, fields: List[Field]):
        """
        Adds multiple Fields to a Feature Set

        Args:
            fields: List of Field (Feature or Entity) Objects
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
        rows_to_sample: int = 100,
    ):
        """

        Adds fields (Features or Entities) to a feature set based on the schema
        of a Datatframe. Only Pandas dataframes are supported. All columns are
        detected as features, so setting at least one entity manually is
        advised.

        Args:
            df: Pandas dataframe to read schema from
            entities: List of entities that will be set manually and not
                inferred. These will take precedence over any existing entities
                or entities found in the dataframe.
            features: List of features that will be set manually and not
                inferred. These will take precedence over any existing feature
                or features found in the dataframe.
            replace_existing_features: If true, will replace
                existing features in this feature set with features found in
                dataframe. If false, will skip conflicting features.
            replace_existing_entities: If true, will replace existing entities
                in this feature set with features found in dataframe. If false,
                will skip conflicting entities.
            discard_unused_fields: Boolean flag. Setting this to True will
                discard any existing fields that are not found in the dataset or
                provided by the user
            rows_to_sample: Number of rows to sample to infer types. All rows
                must have consistent types, even values within list types must
                be homogeneous
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
                name=column,
                dtype=_infer_pd_column_type(column, df[column], rows_to_sample),
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

    def update_from_feature_set(self, feature_set):
        """
        Deep replaces one feature set with another

        Args:
            feature_set: Feature set to use as a source of configuration
        """

        self.name = feature_set.name
        self.version = feature_set.version
        self.source = feature_set.source
        self.max_age = feature_set.max_age
        self.features = feature_set.features
        self.entities = feature_set.entities
        self.source = feature_set.source

    def get_kafka_source_brokers(self) -> str:
        """
        Get the broker list for the source in this feature set
        """
        if self.source and self.source.source_type is "Kafka":
            return self.source.brokers
        raise Exception("Source type could not be identified")

    def get_kafka_source_topic(self) -> str:
        """
        Get the topic that this feature set has been configured to use as source
        """
        if self.source and self.source.source_type == "Kafka":
            return self.source.topic
        raise Exception("Source type could not be identified")

    def is_valid(self):
        """
        Validates the state of a feature set locally. Raises an exception
        if feature set is invalid.
        """

        if len(self.entities) == 0:
            raise ValueError(f"No entities found in feature set {self.name}")

    @classmethod
    def from_yaml(cls, yml: str):
        """
        Creates a feature set from a YAML string body or a file path

        Args:
            yml: Either a file path containing a yaml file or a YAML string

        Returns:
            Returns a FeatureSet object based on the YAML file
        """

        return cls.from_dict(feast_yaml.yaml_loader(yml, load_single=True))

    @classmethod
    def from_dict(cls, fs_dict):
        """
        Creates a feature set from a dict

        Args:
            fs_dict: A dict representation of a feature set

        Returns:
            Returns a FeatureSet object based on the feature set dict
        """

        if ("kind" not in fs_dict) and (fs_dict["kind"].strip() != "feature_set"):
            raise Exception(f"Resource kind is not a feature set {str(fs_dict)}")
        feature_set_proto = json_format.ParseDict(
            fs_dict, FeatureSetSpecProto(), ignore_unknown_fields=True
        )
        return cls.from_proto(feature_set_proto)

    @classmethod
    def from_proto(cls, feature_set_proto: FeatureSetSpecProto):
        """
        Creates a feature set from a protobuf representation of a feature set

        Args:
            from_proto: A protobuf representation of a feature set

        Returns:
            Returns a FeatureSet object based on the feature set protobuf
        """

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
        return feature_set

    def to_proto(self) -> FeatureSetSpecProto:
        """
        Converts a feature set object to its protobuf representation

        Returns:
            FeatureSetSpec protobuf
        """

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


def _infer_pd_column_type(column, series, rows_to_sample):
    dtype = None
    sample_count = 0

    # Loop over all rows for this column to infer types
    for key, value in series.iteritems():
        sample_count += 1
        # Stop sampling at the row limit
        if sample_count > rows_to_sample:
            continue

        # Infer the specific type for this row
        current_dtype = python_type_to_feast_value_type(name=column, value=value)

        # Make sure the type is consistent for column
        if dtype:
            if dtype != current_dtype:
                raise ValueError(
                    f"Type mismatch detected in column {column}. Both "
                    f"the types {current_dtype} and {dtype} "
                    f"have been found."
                )
        else:
            # Store dtype in field to type map if it isnt already
            dtype = current_dtype

    return dtype
