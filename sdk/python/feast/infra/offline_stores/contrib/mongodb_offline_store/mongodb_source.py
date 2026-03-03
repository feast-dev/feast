# Copyright 2025 The Feast Authors
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

import json
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None  # type: ignore[assignment,misc]

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException, FeastExtrasDependencyImportError
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType


def _infer_python_type_str(value: Any) -> Optional[str]:
    """Infer a Feast-compatible type string from a Python value returned by pymongo."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, bytes):
        return "bytes"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, list):
        if not value:
            return "list[str]"
        elem_type = _infer_python_type_str(value[0])
        if elem_type:
            return f"list[{elem_type}]"
        return "list[str]"
    return None


def mongodb_to_feast_value_type(type_str: str) -> ValueType:
    """Map a Python type string (from pymongo) to a Feast ValueType."""
    _MAP: Dict[str, ValueType] = {
        "str": ValueType.STRING,
        "int": ValueType.INT64,
        "float": ValueType.DOUBLE,
        "bool": ValueType.BOOL,
        "bytes": ValueType.BYTES,
        "datetime": ValueType.UNIX_TIMESTAMP,
        "list[str]": ValueType.STRING_LIST,
        "list[int]": ValueType.INT64_LIST,
        "list[float]": ValueType.DOUBLE_LIST,
        "list[bool]": ValueType.BOOL_LIST,
        "list[bytes]": ValueType.BYTES_LIST,
        "list[datetime]": ValueType.UNIX_TIMESTAMP_LIST,
    }
    return _MAP.get(type_str, ValueType.UNKNOWN)


class MongoDBOptions:
    """Options for a MongoDB data source (database + collection)."""

    def __init__(self, database: str, collection: str):
        self._database = database
        self._collection = collection

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        return DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {"database": self._database, "collection": self._collection}
            ).encode()
        )

    @classmethod
    def from_proto(
        cls, options_proto: DataSourceProto.CustomSourceOptions
    ) -> "MongoDBOptions":
        config = json.loads(options_proto.configuration.decode("utf8"))
        return cls(database=config["database"], collection=config["collection"])


class MongoDBSource(DataSource):
    """A MongoDB collection as a Feast offline data source."""

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def __init__(
        self,
        name: Optional[str] = None,
        database: Optional[str] = None,
        collection: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        schema_sample_size: int = 100,
    ):
        if name is None and collection is None:
            raise DataSourceNoNameException()
        name = name or collection
        assert name

        self._mongodb_options = MongoDBOptions(
            database=database or "",
            collection=collection or name,
        )
        self._schema_sample_size = schema_sample_size

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, MongoDBSource):
            raise TypeError(
                "Comparisons should only involve MongoDBSource class objects."
            )
        return (
            super().__eq__(other)
            and self._mongodb_options._database == other._mongodb_options._database
            and self._mongodb_options._collection == other._mongodb_options._collection
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def database(self) -> str:
        return self._mongodb_options._database

    @property
    def collection(self) -> str:
        return self._mongodb_options._collection

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> "MongoDBSource":
        assert data_source.HasField("custom_options")
        options = json.loads(data_source.custom_options.configuration)
        return MongoDBSource(
            name=data_source.name,
            database=options["database"],
            collection=options["collection"],
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_source.MongoDBSource",
            field_mapping=self.field_mapping,
            custom_options=self._mongodb_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )
        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        return data_source_proto

    def validate(self, config: RepoConfig):
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return mongodb_to_feast_value_type

    def get_table_query_string(self) -> str:
        return f"{self._mongodb_options._database}.{self._mongodb_options._collection}"

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        if MongoClient is None:
            raise FeastExtrasDependencyImportError(
                "mongodb", "pymongo is not installed."
            )
        connection_string = config.offline_store.connection_string
        db_name = self.database or config.offline_store.database
        client: Any = MongoClient(connection_string, tz_aware=True)
        try:
            docs = list(
                client[db_name][self.collection].aggregate(
                    [{"$sample": {"size": self._schema_sample_size}}]
                )
            )
        finally:
            client.close()

        field_type_counts: Dict[str, Dict[str, int]] = {}
        for doc in docs:
            for field, value in doc.items():
                if field == "_id":
                    continue
                type_str = _infer_python_type_str(value)
                if type_str is None:
                    continue
                field_type_counts.setdefault(field, {})
                field_type_counts[field][type_str] = (
                    field_type_counts[field].get(type_str, 0) + 1
                )

        return [
            (field, max(counts, key=lambda t: counts[t]))
            for field, counts in field_type_counts.items()
        ]


class SavedDatasetMongoDBStorage(SavedDatasetStorage):
    """Persists a Feast SavedDataset into a MongoDB collection."""

    _proto_attr_name = "custom_storage"

    mongodb_options: MongoDBOptions

    def __init__(self, database: str, collection: str):
        self.mongodb_options = MongoDBOptions(
            database=database,
            collection=collection,
        )

    @staticmethod
    def from_proto(
        storage_proto: SavedDatasetStorageProto,
    ) -> "SavedDatasetMongoDBStorage":
        options = json.loads(storage_proto.custom_storage.configuration)
        return SavedDatasetMongoDBStorage(
            database=options["database"],
            collection=options["collection"],
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(custom_storage=self.mongodb_options.to_proto())

    def to_data_source(self) -> DataSource:
        return MongoDBSource(
            database=self.mongodb_options._database,
            collection=self.mongodb_options._collection,
        )
