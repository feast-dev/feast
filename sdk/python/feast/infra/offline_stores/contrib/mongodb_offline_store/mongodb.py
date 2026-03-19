# Copyright 2026 The Feast Authors
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
import warnings
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union, cast

import ibis
import pandas as pd
from ibis.expr.types import Table
from pydantic import StrictStr

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None  # type: ignore[assignment,misc]

from feast.data_source import DataSource
from feast.errors import (
    DataSourceNoNameException,
    FeastExtrasDependencyImportError,
    SavedDatasetLocationAlreadyExists,
)
from feast.feature_view import FeatureView
from feast.infra.offline_stores.contrib.mongodb_offline_store import DRIVER_METADATA
from feast.infra.offline_stores.ibis import (
    get_historical_features_ibis,
    pull_all_from_table_or_query_ibis,
    pull_latest_from_table_or_query_ibis,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import mongodb_to_feast_value_type
from feast.value_type import ValueType

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# MongoDBSource and related classes (collection-per-FeatureView schema)
# ---------------------------------------------------------------------------


class MongoDBOptions:
    """Options for a MongoDB data source (database + collection)."""

    def __init__(self, database: str, collection: str):
        self._database = database
        self._collection = collection

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        """Serialize database and collection names as JSON into a CustomSourceOptions proto."""
        return DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {"database": self._database, "collection": self._collection}
            ).encode()
        )

    @classmethod
    def from_proto(
        cls, options_proto: DataSourceProto.CustomSourceOptions
    ) -> "MongoDBOptions":
        """Deserialize a CustomSourceOptions proto back into a MongoDBOptions instance."""
        config = json.loads(options_proto.configuration.decode("utf8"))
        return cls(database=config["database"], collection=config["collection"])


class MongoDBSource(DataSource):
    """A MongoDB collection used as a Feast offline data source.

    ``name`` is the logical Feast name for this source. If omitted, it defaults
    to the value of ``collection``.  At least one of ``name`` or ``collection``
    must be supplied.

    ``database`` is the MongoDB database that contains the collection.  When
    omitted it falls back to ``MongoDBOfflineStoreConfig.database`` at query
    time, so a single store-level default can be shared across many sources.

    ``schema_sample_size`` controls how many documents are randomly sampled
    when Feast infers the collection schema (used by ``feast apply`` and
    ``get_table_column_names_and_types``).  Increase it for collections with
    highly variable document shapes; decrease it to speed up ``feast apply``
    at the cost of schema coverage.
    """

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
        # At least one of name / collection is non-None; cast to satisfy the type checker.
        name = cast(str, name or collection)

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
            data_source_class_type="feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb.MongoDBSource",
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
        # No upfront schema validation is required for MongoDB; the connection
        # is exercised lazily when features are actually retrieved.
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return mongodb_to_feast_value_type

    def get_table_query_string(self) -> str:
        return f"{self._mongodb_options._database}.{self._mongodb_options._collection}"

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """Sample documents from the collection to infer field names and their Feast type strings.

        Uses ``$sample`` to fetch up to ``schema_sample_size`` documents, then
        picks the most-frequent Python type observed per field.  The ``_id``
        field is always excluded.
        """
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


# ---------------------------------------------------------------------------
# Offline store configuration and implementation
# ---------------------------------------------------------------------------


class MongoDBOfflineStoreIbisConfig(FeastConfigBaseModel):
    """Configuration for the MongoDB Ibis-backed offline store."""

    type: StrictStr = "feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb.MongoDBOfflineStoreIbis"
    """Offline store type selector"""

    connection_string: StrictStr = "mongodb://localhost:27017"
    """MongoDB connection URI"""

    database: StrictStr = "feast"
    """Default MongoDB database name"""


class MongoDBOfflineStoreIbis(OfflineStore):
    """Offline store backed by MongoDB, using Ibis for point-in-time joins."""

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        if not isinstance(data_source, MongoDBSource):
            raise ValueError(
                f"MongoDBOfflineStore expected a MongoDBSource, "
                f"got {type(data_source).__name__!r}."
            )
        warnings.warn(
            "MongoDB offline store is in preview. API may change without notice.",
            RuntimeWarning,
        )
        return pull_latest_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_build_data_source_reader(config),
            data_source_writer=_build_data_source_writer(config),  # type: ignore[arg-type]
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        warnings.warn(
            "MongoDB offline store is in preview. API may change without notice.",
            RuntimeWarning,
        )
        return get_historical_features_ibis(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
            data_source_reader=_build_data_source_reader(config),
            data_source_writer=_build_data_source_writer(config),  # type: ignore[arg-type]
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        if not isinstance(data_source, MongoDBSource):
            raise ValueError(
                f"MongoDBOfflineStore expected a MongoDBSource, "
                f"got {type(data_source).__name__!r}."
            )
        warnings.warn(
            "MongoDB offline store is in preview. API may change without notice.",
            RuntimeWarning,
        )
        return pull_all_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_build_data_source_reader(config),
            data_source_writer=_build_data_source_writer(config),  # type: ignore[arg-type]
        )


def _build_data_source_reader(config: RepoConfig) -> Callable[[DataSource, str], Table]:
    """Return a closure that fetches a MongoDB collection as an ibis in-memory table."""

    def reader(data_source: DataSource, repo_path: str) -> Table:
        if MongoClient is None:
            raise FeastExtrasDependencyImportError(
                "mongodb", "pymongo is not installed."
            )
        if not isinstance(data_source, MongoDBSource):
            raise ValueError(
                f"MongoDBOfflineStore reader expected a MongoDBSource, "
                f"got {type(data_source).__name__!r}."
            )
        connection_string = config.offline_store.connection_string
        db_name = data_source.database or config.offline_store.database
        client: Any = MongoClient(connection_string, driver=DRIVER_METADATA)
        try:
            docs = list(client[db_name][data_source.collection].find({}, {"_id": 0}))
        finally:
            client.close()

        df = pd.DataFrame(docs)
        if df.empty:
            return ibis.memtable(df)

        # Localize naive datetime columns to UTC. MongoDB stores all dates as UTC,
        # and with tz_aware=False (default), pymongo returns naive datetime objects.
        # We convert them to timezone-aware UTC timestamps for pyarrow compatibility.
        for col in df.columns:
            if df[col].dtype == object and len(df[col].dropna()) > 0:
                sample = df[col].dropna().iloc[0]
                if isinstance(sample, datetime):
                    try:
                        df[col] = pd.to_datetime(df[col], utc=True)
                    except (ValueError, TypeError):
                        # Skip columns that can't be converted (e.g., mixed types)
                        pass

        return ibis.memtable(df)

    return reader


def _build_data_source_writer(
    config: RepoConfig,
) -> Callable[[Table, DataSource, str, str, bool], None]:
    """Return a closure that writes an ibis table to a MongoDB collection."""

    def writer(
        table: Table,
        data_source: DataSource,
        repo_path: str,
        mode: str = "append",
        allow_overwrite: bool = False,
    ) -> None:
        if MongoClient is None:
            raise FeastExtrasDependencyImportError(
                "mongodb", "pymongo is not installed."
            )
        if not isinstance(data_source, MongoDBSource):
            raise ValueError(
                f"MongoDBOfflineStore writer expected a MongoDBSource, "
                f"got {type(data_source).__name__!r}."
            )
        connection_string = config.offline_store.connection_string
        db_name = data_source.database or config.offline_store.database
        location = f"{db_name}.{data_source.collection}"
        client: Any = MongoClient(
            connection_string, driver=DRIVER_METADATA, tz_aware=True
        )
        try:
            coll = client[db_name][data_source.collection]
            if mode == "overwrite":
                if not allow_overwrite and coll.estimated_document_count() > 0:
                    raise SavedDatasetLocationAlreadyExists(location=location)
                coll.drop()
            records = table.to_pyarrow().to_pylist()
            if records:
                coll.insert_many(records)
        finally:
            client.close()

    return writer
