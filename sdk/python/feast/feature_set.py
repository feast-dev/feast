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
from typing import List
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
from feast.type_map import convert_df_to_feature_rows
from feast.type_map import DATETIME_COLUMN

_logger = logging.getLogger(__name__)
CPU_COUNT = cpu_count()  # type: int
SENTINEL = 1  # type: int


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
        max_age: int = -1,
    ):
        self._name = name
        self._fields = OrderedDict()  # type: Dict[str, Field]
        if features is not None:
            self.features = features
        if entities is not None:
            self.entities = entities
        if source is None:
            self._source = KafkaSource()
        self._max_age = max_age
        self._version = None
        self._client = None
        self._message_producer = None
        self._busy_ingesting = False
        self._is_dirty = True

    def __eq__(self, other):
        if not isinstance(other, FeatureSet):
            return NotImplemented

        for self_feature in self.features:
            for other_feature in other.features:
                if self_feature != other_feature:
                    return False

        for self_entity in self.entities:
            for other_entity in other.entities:
                if self_entity != other_entity:
                    return False

        if (
            self.name != other.name
            or self.version != other.version
            or self.max_age != other.max_age
            or self.source != other.source
        ):
            return False
        return True

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

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
            return self._add_field(resource)

        raise ValueError("Could not identify the resource being added")

    def _add_field(self, field: Field):
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

    def update_from_dataset(self, df: pd.DataFrame, column_mapping=None):
        """
        Updates Feature Set values based on the data set. Only Pandas dataframes are supported.
        :param column_mapping: Dictionary of column names to resource (entity, feature) mapping. Forces the interpretation
        of a column as either an entity or feature. Example: {"driver_id": Entity(name="driver", dtype=ValueType.INT64)}
        :param df: Pandas dataframe containing datetime column, entity columns, and feature columns.
        """

        fields = OrderedDict()
        existing_entities = self._client.entities if self._client is not None else None

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

            # Skip datetime column
            if DATETIME_COLUMN in column:
                continue

            # Use entity or feature value if provided by the column mapping
            if column_mapping and column in column_mapping:
                if issubclass(type(column_mapping[column]), Field):
                    fields[column] = column_mapping[column]
                    continue
                raise ValueError(
                    "Invalid resource type specified at column name " + column
                )

            # Test whether this column is an existing entity (globally).
            if existing_entities and column in existing_entities:
                entity = existing_entities[column]

                # test whether registered entity type matches user provided type
                if entity.dtype == dtype_to_value_type(df[column].dtype):
                    # Store this field as an entity
                    fields[column] = entity
                    continue

            # Ignore fields that already exist
            if column in self._fields:
                continue

            # Store this field as a feature
            fields[column] = Feature(
                name=column, dtype=pandas_dtype_to_feast_value_type(df[column].dtype)
            )

        if len([field for field in fields.values() if type(field) == Entity]) == 0:
            raise Exception(
                "Could not detect entity column(s). Please provide entity column(s)."
            )
        if len([field for field in fields.values() if type(field) == Feature]) == 0:
            raise Exception(
                "Could not detect feature column(s). Please provide feature column(s)."
            )
        self._add_fields(list(fields.values()))

    def ingest(
        self,
        dataframe: pd.DataFrame,
        force_update: bool = False,
        timeout: int = 5,
        max_workers: int = CPU_COUNT,
    ):
        pbar = tqdm(unit="rows", total=dataframe.shape[0])
        q = Queue()
        proc = Process(target=self._listener, args=(pbar, q))
        try:
            proc.start()  #
            if dataframe.shape[0] > 10000:
                _logger.info("Performing batch upload.")
                # Split dataframe into smaller dataframes
                n = ceil(dataframe.shape[0] / max_workers)
                list_df = [
                    dataframe[i : min(i + n, dataframe.shape[0])]
                    for i in range(0, dataframe.shape[0], n)
                ]

                # Fork ingestion processes
                processes = []
                for i in range(max_workers):
                    _logger.info(f"Starting process number = {i + 1}")
                    p = Process(
                        target=self.ingest_one,
                        args=(list_df[i], q, force_update, timeout),
                    )
                    p.start()
                    processes.append(p)

                # Join ingestion processes
                for p in processes:
                    _logger.info("Joining processes")
                    p.join()
            else:
                _logger.info("Performing upload")
                self.ingest_one(dataframe, q, force_update, timeout)

            _logger.info("Upload complete.")
        except Exception as ex:
            _logger.error(f"Exception occurred: {ex}")
        finally:
            q.put(None)  # Signal to listener process to stop
            proc.join()  # Join listener process
            pbar.update(dataframe.shape[0] - pbar.n)  # Perform a final update
            pbar.close()

    def ingest_one(
        self,
        dataframe: pd.DataFrame,
        q: Queue,
        force_update: bool = False,
        timeout: int = 5,
    ):
        """
        Write the rows in the provided dataframe into a Kafka topic.
        :param dataframe: Dataframe containing the input data to be ingested.
        :param q: Queue used to send signals to update tqdm progress bar
        :param force_update: Flag to update feature set from data set and re-register if changed.
        :param timeout: Timeout in seconds to wait for completion.
        :return:
        """
        # Update feature set from data set and re-register if changed
        if force_update:
            self.update_from_dataset(dataframe)
            self._client.apply(self)

        # Validate feature set version with Feast Core
        self._validate_feature_set()

        # Create Kafka FeatureRow producer (Required if process is forked)
        if self._message_producer is None:
            self._message_producer = KafkaProducer(
                bootstrap_servers=self._get_kafka_source_brokers()
            )

        _logger.info(
            f"Publishing features to topic: '{self._get_kafka_source_topic()}' "
            f"on brokers: '{self._get_kafka_source_brokers()}'"
        )

        feature_rows = dataframe.apply(
            convert_df_to_feature_rows(dataframe, self), axis=1
        )

        for row in feature_rows:
            self._message_producer.send(
                self._get_kafka_source_topic(), row.SerializeToString()
            )
            q.put(SENTINEL)

        # Wait for all messages to be completely sent
        self._message_producer.flush(timeout=timeout)

    def ingest_file(
        self,
        file_path: str,
        force_update: bool = False,
        timeout: int = 5,
        max_workers=CPU_COUNT,
    ):
        """
        Load the contents of a file into a Kafka topic.
        Files that are currently supported:
            * csv
            * parquet
        :param file_path: Valid string path to the file
        :param force_update: Flag to update feature set from dataset and reregister if changed.
        :param timeout: Timeout in seconds to wait for completion
        :param max_workers: The maximum number of threads that can be used to execute the given calls.
        :return:
        """
        df = None
        filename, file_ext = os.path.splitext(file_path)
        if ".parquet" in file_ext:
            df = pd.read_parquet(file_path)
        elif ".csv" in file_ext:
            df = pd.read_csv(file_path, index_col=False)
        try:
            # Ensure that dataframe is initialised
            assert isinstance(df, pd.DataFrame)
        except AssertionError:
            _logger.error(f"Ingestion of file type {file_ext} is not supported")
            raise Exception("File type not supported")

        self.ingest(df, force_update, timeout, max_workers)

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
        )
        feature_set._version = feature_set_proto.version
        feature_set._source = Source.from_proto(feature_set_proto.source)
        feature_set._is_dirty = False
        return feature_set

    def to_proto(self) -> FeatureSetSpecProto:
        return FeatureSetSpecProto(
            name=self.name,
            version=self.version,
            max_age=Duration(seconds=self.max_age),
            source=self.source.to_proto(),
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

    def _update_from_feature_set(self, feature_set, is_dirty: bool = True):

        self.name = feature_set.name
        self.version = feature_set.version
        self.source = feature_set.source
        self.max_age = feature_set.max_age
        self.features = feature_set.features
        self.entities = feature_set.entities

        self._is_dirty = is_dirty

    def _validate_feature_set(self):
        # Validate whether the feature set has been modified and needs to be saved
        if self._is_dirty:
            raise Exception("Feature set has been modified and must be saved first")

    def _get_kafka_source_brokers(self) -> str:
        if self.source and self.source.source_type is "Kafka":
            return self.source.brokers
        raise Exception("Source type could not be identified")

    def _get_kafka_source_topic(self) -> str:
        if self.source and self.source.source_type == "Kafka":
            return self.source.topic
        raise Exception("Source type could not be identified")

    @staticmethod
    def _listener(pbar: tqdm, q: Queue):
        """
        Static method that listens to changes in a queue and updates the progress bar accordingly.
        :param pbar: Initialised tqdm instance
        :param q: Multiprocess queue object containing update instances as SENTINEL flags
        :return:
        """
        for _ in iter(q.get, None):
            pbar.update()

    @classmethod
    def from_yaml(cls, fs_yaml):
        featureset_dict = (
            yaml.safe_load(fs_yaml) if not isinstance(fs_yaml, dict) else fs_yaml
        )

        if ("kind" not in featureset_dict) and (
            featureset_dict["kind"].strip() != "feature_set"
        ):
            raise Exception(
                f"Could not determine the kind of resource from {str(fs_yaml)}"
            )

        feature_set_proto = json_format.ParseDict(
            featureset_dict, FeatureSetSpecProto(), ignore_unknown_fields=True
        )

        return cls.from_proto(feature_set_proto)
