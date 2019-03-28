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

from feast.core.DatasetService_pb2 import FeatureSet as FeatureSet_pb


class FeatureSet:
    """
    Represent a collection of features having same entity.
    """

    def __init__(self, entity, features):
        self._ensure_same_entity(entity, features)

        self._features = features
        self._entity = entity
        self._proto = FeatureSet_pb(entityName=entity, featureIds=features)

    @property
    def features(self):
        """
        Return list of feature ID of this feature set
        Returns: list of feature ID in this feature set

        """
        return self._features

    @property
    def entity(self):
        return self._entity

    @property
    def proto(self):
        return self._proto

    def _ensure_same_entity(self, entity, features):
        for feature in features:
            e = feature.split(".")[0]
            if e != entity:
                raise ValueError("feature set has different entity: " + e)


class FileType(object):
    """
    File type for downloading training dataset as file
    """
    CSV = "CSV"
    """CSV file format"""

    JSON = "NEWLINE_DELIMITED_JSON"
    """Newline delimited JSON file format"""

    AVRO = "AVRO"
    """Avro file format"""


class DatasetInfo:
    def __init__(self, name, table_id):
        """
            Create instance of DatasetInfo with a BigQuery table as its
            backing store.
        Args:
            name: (str) dataset name
            table_id: (str) fully qualified table id
        """
        self._name = name
        self._table_id = table_id

    @property
    def name(self):
        """
        Dataset name
        Returns: dataset name

        """
        return self._name

    @property
    def table_id(self):
        """

        Returns: fully qualified table id

        """
        return self._table_id
