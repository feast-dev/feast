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

"""
Native MongoDB Offline Store — Atlas-first, pure-MQL implementation.

All feature views share a single ``feature_history`` collection, discriminated
by a ``feature_view`` field.  Historical retrieval (point-in-time join) is
executed entirely as a MongoDB aggregation pipeline using ``$documents`` +
``$lookup``, so all sorting, grouping, and TTL filtering run on the Atlas
cluster rather than on the client.

Minimum server version: MongoDB 5.1 (Atlas M10+ or self-managed 5.1+).
``$documents`` (the stage that injects the entity DataFrame into the pipeline
without a temp collection) was introduced in 5.1.

Collection Index (compound, covers both PIT join and pull-latest):

    db.feature_history.create_index([
        ("entity_id",        ASCENDING),
        ("feature_view",     ASCENDING),
        ("event_timestamp",  DESCENDING),
        ("created_at",       DESCENDING),
    ])

    The ``created_at`` key is required so that data corrections (two documents
    with the same entity_id + feature_view + event_timestamp) are resolved
    deterministically: the document with the later created_at wins.

Document Schema:

    {
        "_id":             ObjectId(),
        "entity_id":       Binary(...),          # serialized entity key
        "feature_view":    "driver_stats",
        "features": {
            "conv_rate":        0.72,
            "trips_last_7d":    132
        },
        "event_timestamp": ISODate("2026-01-20T12:00:00Z"),
        "created_at":      ISODate("2026-01-20T12:00:05Z")
    }

PIT Join Strategy (get_historical_features):

    For each chunk of the entity DataFrame:

    1. Python serializes one entity key per (row, feature_view), using only
       that feature view's declared join keys and value types.  This is the
       key correctness invariant: driver_stats is keyed by {driver_id} and
       customer_stats by {customer_id}; using the union would produce bytes
       that never match stored documents.

    2. A single ``$documents`` stage injects the chunk as a virtual collection:
       each document carries ``_row_idx``, ``_ts``, and a per-FV
       ``_entity_ids`` subdocument.

    3. One ``$lookup`` stage per feature view performs the correlated PIT join
       entirely on Atlas:
         - match: entity_id == $$eid, feature_view == <fv_name>,
                  event_timestamp <= $$ts, (optional) event_timestamp >= $$ts - ttl
         - sort:  event_timestamp DESC, created_at DESC
         - limit: 1

    4. A final ``$project`` drops the entity-id payload before results are
       sent over the network.

    5. Python assembles the flat result DataFrame from the matched docs.

Feature Freshness Semantics:

    This implementation uses document-level freshness: the most recent document
    for (entity_id, feature_view) whose event_timestamp <= entity row's
    timestamp is selected, and all requested features are read from that one
    document.  If a newer document is missing a feature that an older document
    had, the value is NULL.  This matches standard Feast offline store semantics.
"""

import json
import warnings
from collections import defaultdict
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import pandas as pd
import pyarrow

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None  # type: ignore[assignment,misc]

from pydantic import StrictStr

from feast.data_source import DataSource
from feast.errors import (
    DataSourceNoNameException,
    FeastExtrasDependencyImportError,
    SavedDatasetLocationAlreadyExists,
)
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.contrib.mongodb_offline_store import DRIVER_METADATA
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    get_expected_join_keys,
    infer_event_timestamp_from_entity_df,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import mongodb_to_feast_value_type
from feast.value_type import ValueType

# ---------------------------------------------------------------------------
# Index tracking — keyed by (connection_string, database, collection) so that
# multiple RepoConfigs pointing at different clusters never share state.
# ---------------------------------------------------------------------------
_indexes_ensured: set = set()

# Chunk size for entity_df processing.  Each chunk becomes one $documents
# stage; keep it small enough that the aggregation command stays well under
# MongoDB's 16 MB BSON document limit.
_CHUNK_SIZE = 50_000


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class MongoDBOfflineStoreNativeConfig(FeastConfigBaseModel):
    """Configuration for the native MongoDB offline store (Atlas-first)."""

    type: StrictStr = "feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_native.MongoDBOfflineStoreNative"

    connection_string: StrictStr = "mongodb://localhost:27017"
    """MongoDB connection URI"""

    database: StrictStr = "feast"
    """MongoDB database name"""

    collection: StrictStr = "feature_history"
    """Single collection name shared by all feature views"""


# ---------------------------------------------------------------------------
# DataSource
# ---------------------------------------------------------------------------


class MongoDBSourceNative(DataSource):
    """MongoDB data source for the native (Atlas-first) offline store.

    All feature views share a single collection.  The ``name`` of this source
    doubles as the ``feature_view`` discriminator value stored in each document.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        timestamp_field: str = "event_timestamp",
        created_timestamp_column: str = "created_at",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        if name is None:
            raise DataSourceNoNameException()
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
        if not isinstance(other, MongoDBSourceNative):
            raise TypeError(
                "Comparisons should only involve MongoDBSourceNative class objects."
            )
        return (
            super().__eq__(other)
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def feature_view_name(self) -> str:
        """The feature_view discriminator value stored in each document."""
        return self.name

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> "MongoDBSourceNative":
        assert data_source.HasField("custom_options")
        return MongoDBSourceNative(
            name=data_source.name,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        return DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type=(
                "feast.infra.offline_stores.contrib.mongodb_offline_store"
                ".mongodb_native.MongoDBSourceNative"
            ),
            field_mapping=self.field_mapping,
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=json.dumps({"feature_view": self.name}).encode()
            ),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
        )

    def validate(self, config: RepoConfig):
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return mongodb_to_feast_value_type

    def get_table_query_string(self) -> str:
        return f"feature_history[feature_view={self.name}]"

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """Sample documents to infer feature names and types."""
        if MongoClient is None:
            raise FeastExtrasDependencyImportError(
                "mongodb", "pymongo is not installed."
            )
        client: Any = MongoClient(
            config.offline_store.connection_string, driver=DRIVER_METADATA
        )
        try:
            pipeline = [
                {"$match": {"feature_view": self.name}},
                {"$sample": {"size": 100}},
            ]
            docs = list(
                client[config.offline_store.database][
                    config.offline_store.collection
                ].aggregate(pipeline)
            )
        finally:
            client.close()

        field_type_counts: Dict[str, Dict[str, int]] = {}
        for doc in docs:
            for field, value in doc.get("features", {}).items():
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


# ---------------------------------------------------------------------------
# SavedDataset storage
# ---------------------------------------------------------------------------


class SavedDatasetMongoDBStorageNative(SavedDatasetStorage):
    """Persists a SavedDataset as a flat MongoDB collection (native store)."""

    _proto_attr_name = "custom_storage"

    def __init__(self, database: str, collection: str):
        self._database = database
        self._collection = collection

    @staticmethod
    def from_proto(
        storage_proto: SavedDatasetStorageProto,
    ) -> "SavedDatasetMongoDBStorageNative":
        config = json.loads(storage_proto.custom_storage.configuration)
        return SavedDatasetMongoDBStorageNative(
            database=config["database"],
            collection=config["collection"],
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(
            custom_storage=DataSourceProto.CustomSourceOptions(
                configuration=json.dumps(
                    {"database": self._database, "collection": self._collection}
                ).encode()
            )
        )

    def to_data_source(self) -> DataSource:
        return MongoDBSourceNative(name=self._collection)


# ---------------------------------------------------------------------------
# RetrievalJob
# ---------------------------------------------------------------------------


class MongoDBNativeRetrievalJob(RetrievalJob):
    """Retrieval job for the native MongoDB offline store."""

    def __init__(
        self,
        query_fn: Callable[[], pyarrow.Table],
        full_feature_names: bool,
        config: RepoConfig,
        on_demand_feature_views: Optional[List[Any]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        self._query_fn = query_fn
        self._full_feature_names = full_feature_names
        self._config = config
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[Any]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        return self._to_arrow_internal(timeout).to_pandas()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        return self._query_fn()

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ) -> None:
        if not isinstance(storage, SavedDatasetMongoDBStorageNative):
            raise ValueError(
                f"MongoDBNativeRetrievalJob.persist expected "
                f"SavedDatasetMongoDBStorageNative, got "
                f"{type(storage).__name__!r}."
            )
        if MongoClient is None:
            raise FeastExtrasDependencyImportError(
                "mongodb", "pymongo is not installed."
            )
        db_name = storage._database or self._config.offline_store.database
        coll_name = storage._collection
        location = f"{db_name}.{coll_name}"

        client: Any = MongoClient(
            self._config.offline_store.connection_string,
            driver=DRIVER_METADATA,
            tz_aware=True,
        )
        try:
            coll = client[db_name][coll_name]
            if not allow_overwrite and coll.estimated_document_count() > 0:
                raise SavedDatasetLocationAlreadyExists(location=location)
            if allow_overwrite:
                coll.drop()
            records = self._to_arrow_internal().to_pylist()
            if records:
                coll.insert_many(records)
        finally:
            client.close()


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _infer_python_type_str(value: Any) -> Optional[str]:
    """Infer a Feast-compatible type string from a Python value."""
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
        return f"list[{elem_type}]" if elem_type else "list[str]"
    return None


def _serialize_entity_key_from_row(
    row: pd.Series,
    join_keys: List[str],
    entity_key_serialization_version: int,
    join_key_types: Optional[Dict[str, "ValueType"]] = None,
) -> bytes:
    """Serialize an entity key from a DataFrame row.

    Args:
        row: DataFrame row containing join key values.
        join_keys: Names of the join key columns for this feature view.
        entity_key_serialization_version: Version passed to serialize_entity_key.
        join_key_types: Declared ValueType per join key, derived from
            ``FeatureView.entity_columns``.  Required for correct INT32 vs
            INT64 serialization.  Without it, all Python ``int`` values map
            to ``int64_val``, silently mismatching stored INT32 entity keys.
    """
    entity_key = EntityKeyProto()
    for key in sorted(join_keys):
        entity_key.join_keys.append(key)
        value = row[key]
        val = ValueProto()
        if hasattr(value, "item"):
            # Unwrap numpy scalars (numpy 2.0 broke isinstance(np.int64, int))
            value = value.item()
        declared_type = join_key_types.get(key) if join_key_types else None
        if declared_type is not None:
            if declared_type == ValueType.INT32:
                val.int32_val = int(value)
            elif declared_type == ValueType.INT64:
                val.int64_val = int(value)
            elif declared_type == ValueType.STRING:
                val.string_val = str(value)
            elif declared_type == ValueType.BYTES:
                val.bytes_val = bytes(value)
            elif declared_type == ValueType.UNIX_TIMESTAMP:
                val.unix_timestamp_val = int(value)
        else:
            if isinstance(value, bool):
                val.bool_val = value
            elif isinstance(value, int):
                val.int64_val = value
            elif isinstance(value, str):
                val.string_val = value
            elif isinstance(value, float):
                val.double_val = value
            else:
                val.string_val = str(value)
        entity_key.entity_values.append(val)
    return serialize_entity_key(entity_key, entity_key_serialization_version)


# ---------------------------------------------------------------------------
# Offline store
# ---------------------------------------------------------------------------


class MongoDBOfflineStoreNative(OfflineStore):
    """Atlas-first MongoDB offline store using a single shared collection.

    All computation — sorting, grouping, TTL filtering, and the point-in-time
    join — runs as a MongoDB aggregation pipeline on the Atlas cluster.  The
    client serializes entity keys, constructs the pipeline, and assembles the
    final flat DataFrame from the matched feature documents returned by Atlas.

    Requires MongoDB 5.1+ for the ``$documents`` aggregation stage.
    """

    @staticmethod
    def _ensure_indexes(client: Any, db_name: str, collection_name: str) -> None:
        """Create the compound index that covers all query patterns.

        The four-key index satisfies:
          - pull_latest:  sort {entity_id, event_timestamp DESC, created_at DESC}
            after an equality match on feature_view.
          - get_historical_features: $lookup subpipeline match on entity_id
            (equality) + feature_view (equality) + event_timestamp (range),
            then sort by event_timestamp DESC, created_at DESC.
        """
        collection = client[db_name][collection_name]
        target_key = [
            ("entity_id", 1),
            ("feature_view", 1),
            ("event_timestamp", -1),
            ("created_at", -1),
        ]
        existing = collection.index_information()
        for idx_info in existing.values():
            if idx_info.get("key") == target_key:
                return
        collection.create_index(
            target_key,
            name="entity_fv_ts_idx",
            background=True,
        )

    @staticmethod
    def _get_client_and_ensure_indexes(config: RepoConfig) -> Any:
        """Return a MongoClient, creating the compound index once per process."""
        if MongoClient is None:
            raise FeastExtrasDependencyImportError(
                "mongodb", "pymongo is not installed."
            )
        client: Any = MongoClient(
            config.offline_store.connection_string, driver=DRIVER_METADATA
        )
        cache_key = (
            config.offline_store.connection_string,
            config.offline_store.database,
            config.offline_store.collection,
        )
        if cache_key not in _indexes_ensured:
            MongoDBOfflineStoreNative._ensure_indexes(
                client,
                config.offline_store.database,
                config.offline_store.collection,
            )
            _indexes_ensured.add(cache_key)
        return client

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """Append a batch of feature observations to the feature_history collection.

        Each row in ``table`` becomes one document.  Multiple documents for the
        same (entity_id, feature_view, event_timestamp) are kept; corrections
        win by having a later created_at.
        """
        if not isinstance(feature_view.batch_source, MongoDBSourceNative):
            raise ValueError(
                f"MongoDBOfflineStoreNative.offline_write_batch expected a "
                f"MongoDBSourceNative batch source, got "
                f"{type(feature_view.batch_source).__name__!r}."
            )

        entity_key_version = config.entity_key_serialization_version
        db_name = config.offline_store.database
        collection_name = config.offline_store.collection

        # Derive join key names and declared types from entity_columns so that
        # INT32 entities produce int32_val bytes, matching the read path.
        join_key_types: Dict[str, ValueType] = {
            feature_view.projection.join_key_map.get(
                ec.name, ec.name
            ): ec.dtype.to_value_type()
            for ec in feature_view.entity_columns
        }
        join_keys = list(join_key_types.keys())

        timestamp_field = feature_view.batch_source.timestamp_field
        created_ts_col: Optional[str] = (
            feature_view.batch_source.created_timestamp_column or None
        )
        reserved = set(join_keys) | {timestamp_field}
        if created_ts_col:
            reserved.add(created_ts_col)
        feature_cols = [c for c in table.column_names if c not in reserved]

        df = table.to_pandas()

        for ts_col in [timestamp_field] + ([created_ts_col] if created_ts_col else []):
            if ts_col in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df[ts_col]):
                    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
                elif df[ts_col].dt.tz is None:
                    df[ts_col] = df[ts_col].dt.tz_localize("UTC")

        df["_entity_id"] = df.apply(
            lambda row: _serialize_entity_key_from_row(
                row, join_keys, entity_key_version, join_key_types
            ),
            axis=1,
        )

        now = datetime.now(tz=timezone.utc)
        docs = []
        for _, row in df.iterrows():
            features: Dict[str, Any] = {}
            for col in feature_cols:
                val = row[col]
                if pd.isna(val):
                    continue
                if hasattr(val, "item"):
                    val = val.item()
                features[col] = val

            created_at = now
            if created_ts_col and created_ts_col in df.columns:
                ct = row[created_ts_col]
                if not pd.isna(ct):
                    created_at = (
                        ct.to_pydatetime() if hasattr(ct, "to_pydatetime") else ct
                    )

            docs.append(
                {
                    "entity_id": row["_entity_id"],
                    "feature_view": feature_view.name,
                    "features": features,
                    "event_timestamp": (
                        row[timestamp_field].to_pydatetime()
                        if hasattr(row[timestamp_field], "to_pydatetime")
                        else row[timestamp_field]
                    ),
                    "created_at": created_at,
                }
            )

        client = MongoDBOfflineStoreNative._get_client_and_ensure_indexes(config)
        try:
            coll = client[db_name][collection_name]
            BATCH_SIZE = 10_000
            for i in range(0, len(docs), BATCH_SIZE):
                batch = docs[i : i + BATCH_SIZE]
                coll.insert_many(batch, ordered=False)
                if progress:
                    progress(len(batch))
        finally:
            client.close()

    # ------------------------------------------------------------------
    # Read path — materialization helpers
    # ------------------------------------------------------------------

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
        """Return the most recent feature document per entity within [start, end].

        Used by the materialization engine to populate the online store.
        """
        if not isinstance(data_source, MongoDBSourceNative):
            raise ValueError(
                f"MongoDBOfflineStoreNative expected MongoDBSourceNative, "
                f"got {type(data_source).__name__!r}."
            )
        warnings.warn(
            "MongoDB offline store (native) is in preview. API may change without notice.",
            RuntimeWarning,
        )

        db_name = config.offline_store.database
        collection = config.offline_store.collection
        feature_view_name = data_source.feature_view_name
        start_utc = start_date.astimezone(tz=timezone.utc)
        end_utc = end_date.astimezone(tz=timezone.utc)

        project_stage: Dict[str, Any] = {
            "_id": 0,
            "entity_id": "$doc.entity_id",
            "event_timestamp": "$doc.event_timestamp",
        }
        if created_timestamp_column:
            project_stage["created_at"] = "$doc.created_at"
        for feat in feature_name_columns:
            project_stage[feat] = f"$doc.features.{feat}"

        # entity_id leads the sort so the compound index backs the entire stage.
        # created_at tiebreaks corrections sharing the same event_timestamp.
        pipeline: List[Dict[str, Any]] = [
            {
                "$match": {
                    "feature_view": feature_view_name,
                    "event_timestamp": {"$gte": start_utc, "$lte": end_utc},
                }
            },
            {"$sort": {"entity_id": 1, "event_timestamp": -1, "created_at": -1}},
            {"$group": {"_id": "$entity_id", "doc": {"$first": "$$ROOT"}}},
            {"$project": project_stage},
        ]

        def _run() -> pyarrow.Table:
            client = MongoDBOfflineStoreNative._get_client_and_ensure_indexes(config)
            try:
                docs = list(
                    client[db_name][collection].aggregate(pipeline, allowDiskUse=True)
                )
                if not docs:
                    return pyarrow.Table.from_pydict({})
                df = pd.DataFrame(docs)
                if not df.empty and "event_timestamp" in df.columns:
                    if df["event_timestamp"].dt.tz is None:
                        df["event_timestamp"] = pd.to_datetime(
                            df["event_timestamp"], utc=True
                        )
                return pyarrow.Table.from_pandas(df, preserve_index=False)
            finally:
                client.close()

        return MongoDBNativeRetrievalJob(
            query_fn=_run, full_feature_names=False, config=config
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
        """Return all feature documents in the given time range without deduplication.

        Used for bulk export and dataset generation.
        """
        if not isinstance(data_source, MongoDBSourceNative):
            raise ValueError(
                f"MongoDBOfflineStoreNative expected MongoDBSourceNative, "
                f"got {type(data_source).__name__!r}."
            )
        warnings.warn(
            "MongoDB offline store (native) is in preview. API may change without notice.",
            RuntimeWarning,
        )

        db_name = config.offline_store.database
        collection = config.offline_store.collection
        feature_view_name = data_source.feature_view_name

        match_filter: Dict[str, Any] = {"feature_view": feature_view_name}
        if start_date or end_date:
            ts_filter: Dict[str, Any] = {}
            if start_date:
                ts_filter["$gte"] = start_date.astimezone(tz=timezone.utc)
            if end_date:
                ts_filter["$lte"] = end_date.astimezone(tz=timezone.utc)
            match_filter["event_timestamp"] = ts_filter

        project_stage: Dict[str, Any] = {
            "_id": 0,
            "entity_id": 1,
            "event_timestamp": 1,
        }
        if created_timestamp_column:
            project_stage["created_at"] = 1
        for feat in feature_name_columns:
            project_stage[feat] = f"$features.{feat}"

        pipeline: List[Dict[str, Any]] = [
            {"$match": match_filter},
            {"$project": project_stage},
        ]

        def _run() -> pyarrow.Table:
            client = MongoDBOfflineStoreNative._get_client_and_ensure_indexes(config)
            try:
                docs = list(
                    client[db_name][collection].aggregate(pipeline, allowDiskUse=True)
                )
                if not docs:
                    return pyarrow.Table.from_pydict({})
                df = pd.DataFrame(docs)
                if not df.empty and "event_timestamp" in df.columns:
                    if df["event_timestamp"].dt.tz is None:
                        df["event_timestamp"] = pd.to_datetime(
                            df["event_timestamp"], utc=True
                        )
                return pyarrow.Table.from_pandas(df, preserve_index=False)
            finally:
                client.close()

        return MongoDBNativeRetrievalJob(
            query_fn=_run, full_feature_names=False, config=config
        )

    # ------------------------------------------------------------------
    # Read path — point-in-time join
    # ------------------------------------------------------------------

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
        """Perform a point-in-time join entirely on the Atlas cluster.

        The entity DataFrame is injected into the aggregation pipeline via
        ``$documents`` (MongoDB 5.1+).  One ``$lookup`` stage per feature view
        performs the correlated PIT join on the server; no temp collection is
        created.  Entity keys are serialized per-FV so that each feature view's
        documents are matched using only that view's own join keys.
        """
        if isinstance(entity_df, str):
            raise ValueError(
                "MongoDBOfflineStoreNative does not support SQL entity_df strings. "
                "Pass a pandas DataFrame instead."
            )
        warnings.warn(
            "MongoDB offline store (native) is in preview. API may change without notice.",
            RuntimeWarning,
        )

        db_name = config.offline_store.database
        feature_collection = config.offline_store.collection
        entity_key_version = config.entity_key_serialization_version

        entity_schema = dict(zip(entity_df.columns, entity_df.dtypes))
        event_timestamp_col = infer_event_timestamp_from_entity_df(entity_schema)

        # Map "feature_view:feature_name" refs → {fv_name: [feature, ...]}
        fv_to_features: Dict[str, List[str]] = defaultdict(list)
        for ref in feature_refs:
            fv_name, feat_name = ref.split(":", 1)
            fv_to_features[fv_name].append(feat_name)

        fv_by_name = {fv.name: fv for fv in feature_views}

        # Per-FV join keys: using only each FV's own join keys is the critical
        # invariant.  A driver_stats document is keyed by serialize({driver_id:
        # 1001}); including customer_id in the key would produce bytes that
        # never match any stored document.
        fv_join_keys_by_name: Dict[str, List[str]] = {
            fv.name: list(get_expected_join_keys(project, [fv], registry))
            for fv in feature_views
        }

        # Declared ValueType per join key — required for INT32 vs INT64
        # correctness (see _serialize_entity_key_from_row docstring).
        fv_join_key_types_by_name: Dict[str, Dict[str, ValueType]] = {
            fv.name: {
                fv.projection.join_key_map.get(
                    ec.name, ec.name
                ): ec.dtype.to_value_type()
                for ec in fv.entity_columns
            }
            for fv in feature_views
        }

        # Original entity columns (everything except the timestamp).
        entity_columns = [c for c in entity_df.columns if c != event_timestamp_col]

        def _run() -> pyarrow.Table:
            if MongoClient is None:
                raise FeastExtrasDependencyImportError(
                    "mongodb", "pymongo is not installed."
                )

            # Work on a copy with a guaranteed 0-based integer index so that
            # _row_idx stored in $documents == iloc position in result.
            work = entity_df.copy()
            work = work.reset_index(drop=True)

            if not pd.api.types.is_datetime64_any_dtype(work[event_timestamp_col]):
                work[event_timestamp_col] = pd.to_datetime(
                    work[event_timestamp_col], utc=True
                )
            elif work[event_timestamp_col].dt.tz is None:
                work[event_timestamp_col] = work[event_timestamp_col].dt.tz_localize(
                    "UTC"
                )

            # Serialize per-FV entity keys once for the whole DataFrame.
            # Column names use a prefix unlikely to clash with feature names.
            for fv_name in fv_to_features:
                join_keys = fv_join_keys_by_name[fv_name]
                join_key_types = fv_join_key_types_by_name[fv_name]
                work[f"__eid__{fv_name}"] = work.apply(
                    lambda row, jk=join_keys, jkt=join_key_types: (
                        _serialize_entity_key_from_row(row, jk, entity_key_version, jkt)
                    ),
                    axis=1,
                )

            client = MongoDBOfflineStoreNative._get_client_and_ensure_indexes(config)
            try:
                db = client[db_name]
                all_output_rows: List[Dict[str, Any]] = []

                for chunk_start in range(0, len(work), _CHUNK_SIZE):
                    chunk = work.iloc[chunk_start : chunk_start + _CHUNK_SIZE]

                    # Build the $documents array.  Each entry carries:
                    #   _row_idx   – position in work (used to recover entity cols)
                    #   _ts        – entity row's event_timestamp (PIT upper bound)
                    #   _entity_ids – per-FV serialized key bytes
                    documents: List[Dict[str, Any]] = []
                    for row_idx, row in chunk.iterrows():
                        ts = row[event_timestamp_col]
                        if hasattr(ts, "to_pydatetime"):
                            ts = ts.to_pydatetime()
                        documents.append(
                            {
                                "_row_idx": int(row_idx),
                                "_ts": ts,
                                "_entity_ids": {
                                    fv_name: row[f"__eid__{fv_name}"]
                                    for fv_name in fv_to_features
                                },
                            }
                        )

                    # Build the pipeline: $documents + one $lookup per FV.
                    #
                    # Each $lookup:
                    #   let   eid = $_entity_ids.<fv_name>   (Binary bytes)
                    #         ts  = $_ts                     (datetime)
                    #
                    #   subpipeline:
                    #     $match  entity_id == $$eid
                    #             feature_view == <fv_name>   (equality → index)
                    #             event_timestamp <= $$ts     (PIT upper bound)
                    #             event_timestamp >= $$ts - ttl_ms  (optional)
                    #     $sort   event_timestamp DESC, created_at DESC
                    #     $limit  1                           (most recent doc)
                    #     $project features only             (reduce transfer)
                    pipeline: List[Dict[str, Any]] = [{"$documents": documents}]

                    for fv_name, _features in fv_to_features.items():
                        fv = fv_by_name.get(fv_name)
                        match_exprs: List[Dict[str, Any]] = [
                            {"$eq": ["$entity_id", "$$eid"]},
                            {"$eq": ["$feature_view", fv_name]},
                            {"$lte": ["$event_timestamp", "$$ts"]},
                        ]
                        if fv and fv.ttl:
                            ttl_ms = int(fv.ttl.total_seconds() * 1000)
                            match_exprs.append(
                                {
                                    "$gte": [
                                        "$event_timestamp",
                                        {"$subtract": ["$$ts", ttl_ms]},
                                    ]
                                }
                            )

                        pipeline.append(
                            {
                                "$lookup": {
                                    "from": feature_collection,
                                    "let": {
                                        # Access the per-FV key from the
                                        # _entity_ids subdocument.
                                        "eid": f"$_entity_ids.{fv_name}",
                                        "ts": "$_ts",
                                    },
                                    "pipeline": [
                                        {"$match": {"$expr": {"$and": match_exprs}}},
                                        {
                                            "$sort": {
                                                "event_timestamp": -1,
                                                "created_at": -1,
                                            }
                                        },
                                        {"$limit": 1},
                                        # Return only features — drop entity_id,
                                        # feature_view, timestamps, and _id to
                                        # minimise data transferred from Atlas.
                                        {
                                            "$project": {
                                                "_id": 0,
                                                "features": 1,
                                            }
                                        },
                                    ],
                                    "as": f"_match_{fv_name}",
                                }
                            }
                        )

                    # Drop _entity_ids before results cross the network.
                    pipeline.append(
                        {
                            "$project": {
                                "_id": 0,
                                "_row_idx": 1,
                                **{
                                    f"_match_{fv_name}": 1 for fv_name in fv_to_features
                                },
                            }
                        }
                    )

                    # $documents pipelines run against the database, not a
                    # specific collection.
                    matched_docs = list(db.aggregate(pipeline, allowDiskUse=True))

                    # Assemble flat output rows from the matched documents.
                    for doc in matched_docs:
                        row_idx = doc["_row_idx"]
                        orig = work.iloc[row_idx]

                        out: Dict[str, Any] = {}
                        for col in entity_columns:
                            val = orig[col]
                            if hasattr(val, "item"):
                                val = val.item()
                            out[col] = val
                        out[event_timestamp_col] = orig[event_timestamp_col]
                        out["_row_idx"] = row_idx

                        for fv_name, feats in fv_to_features.items():
                            matches = doc.get(f"_match_{fv_name}", [])
                            feat_doc = matches[0] if matches else None
                            feat_values = (
                                feat_doc.get("features", {}) if feat_doc else {}
                            )
                            for feat in feats:
                                col_name = (
                                    f"{fv_name}__{feat}" if full_feature_names else feat
                                )
                                out[col_name] = feat_values.get(feat)

                        all_output_rows.append(out)

            finally:
                client.close()

            # Restore original entity_df ordering.
            all_output_rows.sort(key=lambda r: r["_row_idx"])
            result_df = pd.DataFrame(all_output_rows)
            result_df = result_df.drop(columns=["_row_idx"], errors="ignore")

            if not result_df.empty and event_timestamp_col in result_df.columns:
                if result_df[event_timestamp_col].dt.tz is None:
                    result_df[event_timestamp_col] = pd.to_datetime(
                        result_df[event_timestamp_col], utc=True
                    )

            return pyarrow.Table.from_pandas(result_df, preserve_index=False)

        return MongoDBNativeRetrievalJob(
            query_fn=_run,
            full_feature_names=full_feature_names,
            config=config,
        )
