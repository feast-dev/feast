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
import warnings
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
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


class MongoDBOfflineStoreOneConfig(FeastConfigBaseModel):
    """Configuration for the MongoDB offline store (single shared collection)."""

    type: StrictStr = "feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_one.MongoDBOfflineStoreOne"
    """Offline store type selector"""

    connection_string: StrictStr = "mongodb://localhost:27017"
    """MongoDB connection URI"""

    database: StrictStr = "feast"
    """MongoDB database name"""

    collection: StrictStr = "feature_history"
    """Single collection name for all feature views"""


class MongoDBSourceOne(DataSource):
    """A MongoDB data source for the single-collection offline store.

    Unlike MongoDBSourceMany, this source does not map each FeatureView to
    its own collection. Instead, all FeatureViews share a single MongoDB
    collection (configured at the store level).

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
        if not isinstance(other, MongoDBSourceOne):
            raise TypeError(
                "Comparisons should only involve MongoDBSourceOne class objects."
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
    def from_proto(data_source: DataSourceProto) -> "MongoDBSourceOne":
        assert data_source.HasField("custom_options")
        return MongoDBSourceOne(
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
            data_source_class_type="feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_one.MongoDBSourceOne",
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


class MongoDBOneRetrievalJob(RetrievalJob):
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


class MongoDBOfflineStoreOne(OfflineStore):
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
        """Create recommended indexes on the feature_history collection.

        Uses create_index with background=True. If index already exists
        (with same or different name), this is a no-op.
        """
        collection = client[db_name][collection_name]
        # Check if an equivalent index already exists
        existing_indexes = collection.index_information()
        target_key = [("entity_id", 1), ("feature_view", 1), ("event_timestamp", -1)]

        for idx_info in existing_indexes.values():
            if idx_info.get("key") == target_key:
                return  # Index already exists

        collection.create_index(
            target_key,
            name="entity_fv_ts_idx",
            background=True,
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
        if not isinstance(data_source, MongoDBSourceOne):
            raise ValueError(
                f"MongoDBOfflineStoreOne expected MongoDBSourceOne, "
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
            client = MongoDBOfflineStoreOne._get_client_and_ensure_indexes(config)
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

        return MongoDBOneRetrievalJob(query_fn=_run, full_feature_names=False)

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
        if not isinstance(data_source, MongoDBSourceOne):
            raise ValueError(
                f"MongoDBOfflineStoreOne expected MongoDBSourceOne, "
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
            client = MongoDBOfflineStoreOne._get_client_and_ensure_indexes(config)
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

        return MongoDBOneRetrievalJob(query_fn=_run, full_feature_names=False)

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
        """Fetch historical features using a "fetch + pandas join" strategy.

        Instead of using $lookup (which scales poorly), this:
        1. Extracts unique entity_ids and computes timestamp bounds
        2. Fetches all matching feature data in batched queries
        3. Uses pd.merge_asof for efficient point-in-time joins in Python

        For large entity_df, processing is chunked to bound memory usage.
        """
        if isinstance(entity_df, str):
            raise ValueError(
                "MongoDBOfflineStoreOne does not support SQL entity_df strings. "
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
        fv_by_name = {fv.name: fv for fv in feature_views}

        # Chunk size for entity_df processing (bounds memory usage)
        CHUNK_SIZE = 50_000
        # Batch size for MongoDB $in queries
        MONGO_BATCH_SIZE = 10_000

        def _chunk_dataframe(
            df: pd.DataFrame, size: int
        ) -> Generator[pd.DataFrame, None, None]:
            """Yield successive chunks of a DataFrame."""
            for i in range(0, len(df), size):
                yield df.iloc[i : i + size]

        def _run_single(entity_subset_df: pd.DataFrame, coll: Any) -> pd.DataFrame:
            """Process a single chunk of entity_df and return joined features.

            Args:
                entity_subset_df: Chunk of entity DataFrame to process
                coll: MongoDB collection object (reused across chunks)
            """
            # Prepare entity_df: ensure timestamps are UTC
            result = entity_subset_df.copy()
            # Convert timestamp column to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(result[event_timestamp_col]):
                result[event_timestamp_col] = pd.to_datetime(
                    result[event_timestamp_col], utc=True
                )
            elif result[event_timestamp_col].dt.tz is None:
                result[event_timestamp_col] = pd.to_datetime(
                    result[event_timestamp_col], utc=True
                )

            # Get join keys (all columns except event_timestamp and internal columns)
            entity_columns = [
                c
                for c in result.columns
                if c != event_timestamp_col and not c.startswith("_")
            ]

            # Serialize entity keys to bytes (same format as online store)
            result["_entity_id"] = result.apply(
                lambda row: _serialize_entity_key_from_row(
                    row, entity_columns, entity_key_version
                ),
                axis=1,
            )

            # Extract unique entity_ids and timestamp bounds for this chunk
            unique_entity_ids = result["_entity_id"].unique().tolist()
            max_ts = result[event_timestamp_col].max()

            # Fetch feature data in batches
            all_feature_docs: List[Dict] = []

            for i in range(0, len(unique_entity_ids), MONGO_BATCH_SIZE):
                batch_ids = unique_entity_ids[i : i + MONGO_BATCH_SIZE]

                query = {
                    "entity_id": {"$in": batch_ids},
                    "feature_view": {"$in": fv_names},
                    "event_timestamp": {"$lte": max_ts},
                }
                docs = list(coll.find(query, {"_id": 0}))
                all_feature_docs.extend(docs)

            # Handle empty result
            if not all_feature_docs:
                for fv_name, features in fv_to_features.items():
                    for feat in features:
                        col_name = f"{fv_name}__{feat}" if full_feature_names else feat
                        result[col_name] = None
                return result.drop(columns=["_entity_id"])

            # Convert to DataFrame and flatten features subdoc
            feature_df = pd.DataFrame(all_feature_docs)
            feature_df = feature_df.rename(columns={"entity_id": "_entity_id"})

            if "features" in feature_df.columns:
                features_expanded = pd.json_normalize(feature_df["features"])
                feature_df = pd.concat(
                    [feature_df.drop(columns=["features"]), features_expanded], axis=1
                )

            if feature_df["event_timestamp"].dt.tz is None:
                feature_df["event_timestamp"] = pd.to_datetime(
                    feature_df["event_timestamp"], utc=True
                )

            # Sort result for merge_asof
            result = result.sort_values(event_timestamp_col).reset_index(drop=True)

            # Perform PIT join for each feature view
            for fv_name, features in fv_to_features.items():
                fv = fv_by_name.get(fv_name)
                fv_df = feature_df[feature_df["feature_view"] == fv_name].copy()

                if fv_df.empty:
                    for feat in features:
                        col_name = f"{fv_name}__{feat}" if full_feature_names else feat
                        result[col_name] = None
                    continue

                fv_df = fv_df.sort_values("event_timestamp").reset_index(drop=True)

                merge_cols = ["_entity_id", "event_timestamp"] + [
                    f for f in features if f in fv_df.columns
                ]
                fv_df_subset = fv_df[
                    [c for c in merge_cols if c in fv_df.columns]
                ].copy()
                fv_df_subset = fv_df_subset.rename(
                    columns={"event_timestamp": "_fv_ts"}
                )

                result = pd.merge_asof(
                    result,
                    fv_df_subset,
                    left_on=event_timestamp_col,
                    right_on="_fv_ts",
                    by="_entity_id",
                    direction="backward",
                )

                # Apply TTL
                if fv and fv.ttl:
                    cutoff = result[event_timestamp_col] - fv.ttl
                    stale_mask = result["_fv_ts"] < cutoff
                    for feat in features:
                        if feat in result.columns:
                            result.loc[stale_mask, feat] = None

                # Rename features if full_feature_names
                for feat in features:
                    if feat in result.columns and full_feature_names:
                        result = result.rename(columns={feat: f"{fv_name}__{feat}"})
                    elif feat not in result.columns:
                        col_name = f"{fv_name}__{feat}" if full_feature_names else feat
                        result[col_name] = None

                result = result.drop(columns=["_fv_ts"], errors="ignore")

            return result.drop(columns=["_entity_id"], errors="ignore")

        def _run() -> pyarrow.Table:
            # Add row index to preserve original ordering
            working_df = entity_df.copy()
            working_df["_row_idx"] = range(len(working_df))

            # Create client once for all chunks
            client = MongoDBOfflineStoreOne._get_client_and_ensure_indexes(config)
            try:
                coll = client[db_name][feature_collection]

                if len(working_df) <= CHUNK_SIZE:
                    # Small workload: process in single pass
                    result_df = _run_single(working_df, coll)
                else:
                    # Large workload: process in chunks
                    chunk_results = []
                    for chunk in _chunk_dataframe(working_df, CHUNK_SIZE):
                        chunk_results.append(_run_single(chunk, coll))

                    result_df = pd.concat(chunk_results, ignore_index=True)
            finally:
                client.close()

            # Restore original ordering and remove index column
            result_df = result_df.sort_values("_row_idx").reset_index(drop=True)
            result_df = result_df.drop(columns=["_row_idx"], errors="ignore")

            # Ensure timestamp column is tz-aware
            if not result_df.empty and event_timestamp_col in result_df.columns:
                if result_df[event_timestamp_col].dt.tz is None:
                    result_df[event_timestamp_col] = pd.to_datetime(
                        result_df[event_timestamp_col], utc=True
                    )

            return pyarrow.Table.from_pandas(result_df, preserve_index=False)

        return MongoDBOneRetrievalJob(
            query_fn=_run,
            full_feature_names=full_feature_names,
        )
