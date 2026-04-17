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
Native MongoDB Offline Store Implementation.

This module implements a MongoDB offline store using native MQL aggregation
pipelines. It uses a single-collection schema where all feature views share
one collection. It is event-based: each document represents an observation
of a FeatureView at a specific point in time. Each document may contain a
subset (0 or more) of the features defined in that FeatureView, all sharing
a single event_timestamp.

Collection Index:
    db.feature_history.create_index([
        ("feature_view", ASCENDING),
        ("entity_id", ASCENDING),
        ("event_timestamp", DESCENDING),
    ])

Document Schema (example):
    {
        "_id": ObjectId(),
        "entity_id": "<serialized_entity_key>",
        "feature_view": "driver_stats",
        "features": {
            "rating": 4.91,
            "trips_last_7d": 132
        },
        "event_timestamp": ISODate("2026-01-20T12:00:00Z"),
        "created_at": ISODate("2026-01-20T12:00:05Z")
    }

Feature Freshness Semantics:
    This implementation operates at *document-level freshness*, not
    per-feature freshness. During retrieval (e.g. point-in-time joins),
    the system selects the most recent document for a given
    (entity_id, feature_view) that satisfies time constraints, and then
    extracts all requested features from that document.

    As a result, if a newer document contains only a subset of features,
    missing features will be returned as NULL—even if older documents
    contained values for those features. The system does not backfill
    individual feature values from earlier events.

    This behavior matches common Feast offline store semantics, but may
    differ from systems that compute "latest value per feature".

Schema Evolution ("Feature Creep"):
    Because features are stored in a flexible subdocument, different
    documents for the same FeatureView may contain different sets of
    feature fields over time. This supports:
        - adding new features without backfilling historical data
        - partial writes or sparse feature computation

    However, it also implies:
        - newly added features will be NULL for older events
        - partially populated documents may lead to NULL values even
          when older data contained those features

    Users should ensure that feature computation pipelines write
    complete feature sets when consistent availability is required.

Notes:
    - Entity keys are serialized to ensure consistency with Feast’s
      online store and to avoid type ambiguity.
    - Point-in-time correctness is enforced per FeatureView.
    - TTL (time-to-live) constraints are applied per FeatureView during
      historical retrieval.
"""

import json
import uuid
import warnings
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import pyarrow

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None  # type: ignore[assignment,misc]

from pydantic import StrictStr

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException, FeastExtrasDependencyImportError
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.contrib.mongodb_offline_store import DRIVER_METADATA
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    infer_event_timestamp_from_entity_df,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.type_map import mongodb_to_feast_value_type
from feast.value_type import ValueType


class MongoDBOfflineStoreNativeConfig(FeastConfigBaseModel):
    """Configuration for the Native MongoDB offline store."""

    type: StrictStr = "feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_native.MongoDBOfflineStoreNative"
    """Offline store type selector"""

    connection_string: StrictStr = "mongodb://localhost:27017"
    """MongoDB connection URI"""

    database: StrictStr = "feast"
    """MongoDB database name"""

    collection: StrictStr = "feature_history"
    """Single collection name for all feature views"""


class MongoDBSourceNative(DataSource):
    """A MongoDB data source for the native offline store.

    Unlike many data source implementations, this source does not map each
    FeatureView to its own table or collection. Instead, all FeatureViews
    share a single MongoDB collection (configured at the store level).

    Each document in that collection includes a ``feature_view`` field that
    identifies which FeatureView it belongs to. The ``name`` of this data
    source corresponds to that value and is used to filter documents during
    queries.
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
        """The feature_view discriminator value (same as source name)."""
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
            data_source_class_type="feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_native.MongoDBSourceNative",
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
        """Sample documents to infer feature names and types.

        Queries documents matching this source's feature_view name and
        inspects the ``features`` subdocument to determine schema.
        """
        if MongoClient is None:
            raise FeastExtrasDependencyImportError(
                "mongodb", "pymongo is not installed."
            )
        connection_string = config.offline_store.connection_string
        db_name = config.offline_store.database
        collection_name = config.offline_store.collection
        client: Any = MongoClient(connection_string, driver=DRIVER_METADATA)
        try:
            pipeline = [
                {"$match": {"feature_view": self.name}},
                {"$sample": {"size": 100}},
            ]
            docs = list(client[db_name][collection_name].aggregate(pipeline))
        finally:
            client.close()

        field_type_counts: Dict[str, Dict[str, int]] = {}
        for doc in docs:
            features = doc.get("features", {})
            for field, value in features.items():
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
        if elem_type:
            return f"list[{elem_type}]"
        return "list[str]"
    return None


def _fetch_documents(
    client: Any,
    database: str,
    collection: str,
    pipeline: List[Dict],
) -> List[Dict]:
    """Execute an aggregation pipeline and return documents."""
    return list(client[database][collection].aggregate(pipeline))


class MongoDBNativeRetrievalJob(RetrievalJob):
    """Retrieval job for native MongoDB offline store queries."""

    def __init__(
        self,
        query_fn: Callable[[], pyarrow.Table],
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[Any]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        self._query_fn = query_fn
        self._full_feature_names = full_feature_names
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
        storage: Any,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ) -> None:
        # TODO: Implement persist for native store
        raise NotImplementedError("persist() not yet implemented for native store")


def _serialize_entity_key_from_row(
    row: pd.Series, join_keys: List[str], entity_key_serialization_version: int
) -> bytes:
    """Serialize entity key from a DataFrame row."""
    entity_key = EntityKeyProto()
    for key in sorted(join_keys):
        entity_key.join_keys.append(key)
        value = row[key]
        val = ValueProto()
        if isinstance(value, int):
            val.int64_val = value
        elif isinstance(value, str):
            val.string_val = value
        elif isinstance(value, float):
            val.double_val = value
        else:
            val.string_val = str(value)
        entity_key.entity_values.append(val)
    return serialize_entity_key(entity_key, entity_key_serialization_version)


def _ttl_to_ms(fv: FeatureView) -> Optional[int]:
    """Convert FeatureView TTL to milliseconds."""
    if fv.ttl is None:
        return None
    return int(fv.ttl.total_seconds() * 1000)


def _build_ttl_gte_expr(feature_views: List[FeatureView]) -> Optional[Dict[str, Any]]:
    """Build a $gte expression with per-FV TTL using $switch.

    Returns a MongoDB expression that evaluates to:
        event_timestamp >= (entity_timestamp - ttl_for_this_feature_view)

    Each feature_view can have a different TTL, handled via $switch branches.
    If no feature views have TTL, returns None (no filtering needed).
    """
    branches = []

    for fv in feature_views:
        ttl_ms = _ttl_to_ms(fv)
        if ttl_ms is None:
            # No TTL for this FV - skip (effectively infinite history)
            continue

        branches.append(
            {
                "case": {"$eq": ["$feature_view", fv.name]},
                "then": {"$subtract": ["$$ts", ttl_ms]},
            }
        )

    # If no TTLs at all, no lower bound needed
    if not branches:
        return None

    return {
        "$gte": [
            "$event_timestamp",
            {
                "$switch": {
                    "branches": branches,
                    # Default: no lower bound (for FVs without TTL)
                    "default": {"$literal": 0},
                }
            },
        ]
    }


class MongoDBOfflineStoreNative(OfflineStore):
    """Native MongoDB offline store using single-collection schema.

    All feature views share one collection (``feature_history``), with documents
    containing:
    - ``entity_id``: serialized entity key (bytes)
    - ``feature_view``: field matching FeatureView name
    - ``features``: subdocument with feature name/value pairs
    - ``event_timestamp``: event time
    - ``created_at``: ingestion time
    """

    _index_initialized: bool = False

    @staticmethod
    def _ensure_indexes(client: Any, db_name: str, collection_name: str) -> None:
        """Create recommended indexes on the feature_history collection."""
        collection = client[db_name][collection_name]
        collection.create_index(
            [
                ("entity_id", 1),
                ("feature_view", 1),
                ("event_timestamp", -1),
            ],
            name="entity_fv_ts_idx",
        )

    @classmethod
    def _get_client_and_ensure_indexes(cls, config: RepoConfig) -> Any:
        """Get a MongoClient and ensure indexes exist (once per process)."""
        if MongoClient is None:
            raise FeastExtrasDependencyImportError(
                "mongodb", "pymongo is not installed."
            )
        client: Any = MongoClient(
            config.offline_store.connection_string, driver=DRIVER_METADATA
        )

        if not cls._index_initialized:
            cls._ensure_indexes(
                client,
                config.offline_store.database,
                config.offline_store.collection,
            )
            cls._index_initialized = True

        return client

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

        # Build projection to flatten features subdoc to top-level fields
        project_stage: Dict[str, Any] = {
            "_id": 0,
            "entity_id": "$doc.entity_id",
            "event_timestamp": "$doc.event_timestamp",
        }
        if created_timestamp_column:
            project_stage["created_at"] = "$doc.created_at"
        for feat in feature_name_columns:
            project_stage[feat] = f"$doc.features.{feat}"

        # Build aggregation pipeline
        pipeline: List[Dict[str, Any]] = [
            {
                "$match": {
                    "feature_view": feature_view_name,
                    "event_timestamp": {"$gte": start_utc, "$lte": end_utc},
                }
            },
            {"$sort": {"entity_id": 1, "event_timestamp": -1, "created_at": -1}},
            {
                "$group": {
                    "_id": "$entity_id",
                    "doc": {"$first": "$$ROOT"},
                }
            },
            {"$project": project_stage},
        ]

        def _run() -> pyarrow.Table:
            client = MongoDBOfflineStoreNative._get_client_and_ensure_indexes(config)
            try:
                docs = _fetch_documents(client, db_name, collection, pipeline)
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

        return MongoDBNativeRetrievalJob(query_fn=_run, full_feature_names=False)

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

        # Build match filter: feature_view + optional time range
        match_filter: Dict[str, Any] = {"feature_view": feature_view_name}
        if start_date or end_date:
            ts_filter: Dict[str, Any] = {}
            if start_date:
                ts_filter["$gte"] = start_date.astimezone(tz=timezone.utc)
            if end_date:
                ts_filter["$lte"] = end_date.astimezone(tz=timezone.utc)
            match_filter["event_timestamp"] = ts_filter

        # Build projection: flatten features subdoc to top-level fields
        # This uses $getField to extract each feature from the features subdoc
        project_stage: Dict[str, Any] = {
            "_id": 0,
            "entity_id": 1,
            "event_timestamp": 1,
        }
        if created_timestamp_column:
            project_stage["created_at"] = 1
        for feat in feature_name_columns:
            project_stage[feat] = f"$features.{feat}"

        # Simple range scan pipeline - no sorting for efficiency
        pipeline: List[Dict[str, Any]] = [
            {"$match": match_filter},
            {"$project": project_stage},
        ]

        def _run() -> pyarrow.Table:
            client = MongoDBOfflineStoreNative._get_client_and_ensure_indexes(config)
            try:
                docs = _fetch_documents(client, db_name, collection, pipeline)
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

        return MongoDBNativeRetrievalJob(query_fn=_run, full_feature_names=False)

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

        # Map "feature_view:feature" refs → {fv_name: [feature, ...]}
        fv_to_features: Dict[str, List[str]] = {}
        for ref in feature_refs:
            fv_name, feat_name = ref.split(":", 1)
            fv_to_features.setdefault(fv_name, []).append(feat_name)

        fv_names = list(fv_to_features.keys())

        # Build per-FV TTL expression using $switch
        relevant_fvs = [fv for fv in feature_views if fv.name in fv_to_features]
        ttl_expr = _build_ttl_gte_expr(relevant_fvs)

        def _run() -> pyarrow.Table:
            if MongoClient is None:
                raise FeastExtrasDependencyImportError(
                    "mongodb", "pymongo is not installed."
                )

            # Prepare entity_df: ensure timestamps are UTC and serialize entity keys
            result = entity_df.copy()
            if result[event_timestamp_col].dt.tz is None:
                result[event_timestamp_col] = pd.to_datetime(
                    result[event_timestamp_col], utc=True
                )

            # Get join keys (all columns except event_timestamp)
            entity_columns = [c for c in result.columns if c != event_timestamp_col]

            # Serialize entity keys to bytes (same format as online store)
            result["_entity_id"] = result.apply(
                lambda row: _serialize_entity_key_from_row(
                    row, entity_columns, entity_key_version
                ),
                axis=1,
            )

            # Build temp collection documents
            temp_docs = []
            for _, row in result.iterrows():
                temp_docs.append(
                    {
                        "entity_id": row["_entity_id"],
                        "event_timestamp": row[event_timestamp_col],
                        "_row_idx": _,  # Preserve original order
                    }
                )

            # Create temporary collection for query
            temp_collection_name = f"tmp_entity_df_{uuid.uuid4().hex[:12]}"

            client = MongoDBOfflineStoreNative._get_client_and_ensure_indexes(config)
            try:
                db = client[db_name]
                temp_collection = db[temp_collection_name]
                temp_collection.insert_many(temp_docs)

                # Build $lookup subpipeline with PIT join logic
                # Match: entity_id, feature_view in list, event_timestamp <= entity.ts
                match_conditions: List[Dict[str, Any]] = [
                    {"$eq": ["$entity_id", "$$entity_id"]},
                    {"$in": ["$feature_view", fv_names]},
                    {"$lte": ["$event_timestamp", "$$ts"]},
                ]
                # Add per-FV TTL filter using $switch
                if ttl_expr is not None:
                    match_conditions.append(ttl_expr)

                lookup_pipeline: List[Dict[str, Any]] = [
                    {"$match": {"$expr": {"$and": match_conditions}}},
                    {"$sort": {"feature_view": 1, "event_timestamp": -1}},
                    {
                        "$group": {
                            "_id": "$feature_view",
                            "doc": {"$first": "$$ROOT"},
                        }
                    },
                ]

                # Main aggregation pipeline
                pipeline: List[Dict[str, Any]] = [
                    {
                        "$lookup": {
                            "from": feature_collection,
                            "let": {
                                "entity_id": "$entity_id",
                                "ts": "$event_timestamp",
                            },
                            "pipeline": lookup_pipeline,
                            "as": "feature_rows",
                        }
                    },
                    {"$sort": {"_row_idx": 1}},  # Preserve original order
                ]

                docs = list(temp_collection.aggregate(pipeline))

            finally:
                # Cleanup temp collection
                client[db_name][temp_collection_name].drop()
                client.close()

            if not docs:
                return pyarrow.Table.from_pydict({})

            # Build result DataFrame
            result = result.reset_index(drop=True)
            rows = []
            for doc in docs:
                # Start with entity columns from original entity_df
                row_idx = doc["_row_idx"]
                row = result.iloc[row_idx][
                    entity_columns + [event_timestamp_col]
                ].to_dict()

                # Extract features from each feature_view's matched doc
                feature_rows_by_fv = {
                    fr["_id"]: fr["doc"] for fr in doc.get("feature_rows", [])
                }

                # Extract features from each feature_view's matched doc
                # TTL is already applied server-side via $switch expression
                for fv_name, features in fv_to_features.items():
                    fv_doc = feature_rows_by_fv.get(fv_name)

                    for feat in features:
                        col_name = f"{fv_name}__{feat}" if full_feature_names else feat
                        if fv_doc is None:
                            row[col_name] = None
                        else:
                            row[col_name] = fv_doc.get("features", {}).get(feat)

                rows.append(row)

            result_df = pd.DataFrame(rows)
            if not result_df.empty and event_timestamp_col in result_df.columns:
                if result_df[event_timestamp_col].dt.tz is None:
                    result_df[event_timestamp_col] = pd.to_datetime(
                        result_df[event_timestamp_col], utc=True
                    )
            return pyarrow.Table.from_pandas(result_df, preserve_index=False)

        return MongoDBNativeRetrievalJob(
            query_fn=_run,
            full_feature_names=full_feature_names,
        )
