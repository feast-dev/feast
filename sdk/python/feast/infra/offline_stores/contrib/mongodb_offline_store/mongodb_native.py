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
one collection, discriminated by a ``feature_view`` field.

Schema:
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

Recommended Index:
    db.feature_history.create_index([
        ("entity_id", ASCENDING),
        ("feature_view", ASCENDING),
        ("event_timestamp", DESCENDING),
    ])
"""

import json
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
    """Configuration for the Native MongoDB offline store.

    Uses a single shared collection for all feature views, with documents
    containing an ``entity_id``, ``feature_view`` discriminator, and nested
    ``features`` subdocument.
    """

    type: StrictStr = "feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_native.MongoDBOfflineStoreNative"
    """Offline store type selector"""

    connection_string: StrictStr = "mongodb://localhost:27017"
    """MongoDB connection URI"""

    database: StrictStr = "feast"
    """MongoDB database name"""

    collection: StrictStr = "feature_history"
    """Single collection name for all feature views"""


class MongoDBSourceNative(DataSource):
    """A MongoDB data source for the Native offline store.

    Unlike MongoDBSource (Ibis), this source does not specify a collection
    per FeatureView. Instead, all FeatureViews share a single collection
    (configured at the store level), and are discriminated by the
    ``feature_view`` field in each document.

    The ``name`` parameter becomes the ``feature_view`` discriminator value
    used to filter documents in queries.
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
        client: Any = MongoClient(connection_string)
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
    connection_string: str,
    database: str,
    collection: str,
    pipeline: List[Dict],
) -> List[Dict]:
    """Execute an aggregation pipeline and return documents."""
    if MongoClient is None:
        raise FeastExtrasDependencyImportError("mongodb", "pymongo is not installed.")
    client: Any = MongoClient(connection_string)
    try:
        return list(client[database][collection].aggregate(pipeline))
    finally:
        client.close()


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


class MongoDBOfflineStoreNative(OfflineStore):
    """Native MongoDB offline store using single-collection schema.

    All feature views share one collection (``feature_history``), with documents
    containing:
    - ``entity_id``: serialized entity key (bytes)
    - ``feature_view``: discriminator field matching FeatureView name
    - ``features``: subdocument with feature name/value pairs
    - ``event_timestamp``: event time
    - ``created_at``: ingestion time
    """

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

        connection_string = config.offline_store.connection_string
        db_name = config.offline_store.database
        collection = config.offline_store.collection
        feature_view_name = data_source.feature_view_name

        start_utc = start_date.astimezone(tz=timezone.utc)
        end_utc = end_date.astimezone(tz=timezone.utc)

        # Build aggregation pipeline
        pipeline: List[Dict[str, Any]] = [
            {
                "$match": {
                    "feature_view": feature_view_name,
                    "event_timestamp": {"$gte": start_utc, "$lte": end_utc},
                }
            },
            {"$sort": {"entity_id": 1, "event_timestamp": -1}},
            {
                "$group": {
                    "_id": "$entity_id",
                    "doc": {"$first": "$$ROOT"},
                }
            },
        ]

        def _run() -> pyarrow.Table:
            docs = _fetch_documents(connection_string, db_name, collection, pipeline)
            if not docs:
                return pyarrow.Table.from_pydict({})

            # Flatten documents
            rows = []
            for d in docs:
                doc = d["doc"]
                row = {
                    "entity_id": doc["entity_id"],
                    "event_timestamp": doc["event_timestamp"],
                }
                features = doc.get("features", {})
                for feat in feature_name_columns:
                    row[feat] = features.get(feat)
                rows.append(row)

            df = pd.DataFrame(rows)
            # Ensure timestamp is tz-aware
            if not df.empty and df["event_timestamp"].dt.tz is None:
                df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
            return pyarrow.Table.from_pandas(df, preserve_index=False)

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

        connection_string = config.offline_store.connection_string
        db_name = config.offline_store.database
        collection = config.offline_store.collection
        feature_view_name = data_source.feature_view_name

        # Build match filter
        match_filter: Dict[str, Any] = {"feature_view": feature_view_name}
        if start_date or end_date:
            ts_filter: Dict[str, Any] = {}
            if start_date:
                ts_filter["$gte"] = start_date.astimezone(tz=timezone.utc)
            if end_date:
                ts_filter["$lte"] = end_date.astimezone(tz=timezone.utc)
            match_filter["event_timestamp"] = ts_filter

        pipeline = [{"$match": match_filter}]

        def _run() -> pyarrow.Table:
            docs = _fetch_documents(connection_string, db_name, collection, pipeline)
            if not docs:
                return pyarrow.Table.from_pydict({})

            rows = []
            for doc in docs:
                row = {
                    "entity_id": doc["entity_id"],
                    "event_timestamp": doc["event_timestamp"],
                }
                features = doc.get("features", {})
                for feat in feature_name_columns:
                    row[feat] = features.get(feat)
                rows.append(row)

            df = pd.DataFrame(rows)
            if not df.empty and df["event_timestamp"].dt.tz is None:
                df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
            return pyarrow.Table.from_pandas(df, preserve_index=False)

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

        connection_string = config.offline_store.connection_string
        db_name = config.offline_store.database
        collection = config.offline_store.collection
        entity_key_version = config.entity_key_serialization_version

        entity_schema = dict(zip(entity_df.columns, entity_df.dtypes))
        event_timestamp_col = infer_event_timestamp_from_entity_df(entity_schema)

        # Map "feature_view:feature" refs → {fv_name: [feature, ...]}
        fv_to_features: Dict[str, List[str]] = {}
        for ref in feature_refs:
            fv_name, feat_name = ref.split(":", 1)
            fv_to_features.setdefault(fv_name, []).append(feat_name)

        fv_by_name = {fv.name: fv for fv in feature_views}

        def _run() -> pyarrow.Table:
            result = entity_df.copy()

            # Ensure entity timestamp is tz-aware UTC
            if result[event_timestamp_col].dt.tz is None:
                result[event_timestamp_col] = pd.to_datetime(
                    result[event_timestamp_col], utc=True
                )
            result = result.sort_values(event_timestamp_col)

            # Get join keys from entity_df columns (excluding event_timestamp)
            entity_columns = [c for c in result.columns if c != event_timestamp_col]

            # Serialize entity keys for lookup
            result["_entity_id"] = result.apply(
                lambda row: _serialize_entity_key_from_row(
                    row, entity_columns, entity_key_version
                ),
                axis=1,
            )

            for fv_name, features in fv_to_features.items():
                fv = fv_by_name[fv_name]
                source = fv.batch_source
                if not isinstance(source, MongoDBSourceNative):
                    raise ValueError(
                        f"MongoDBOfflineStoreNative: feature view {fv_name!r} has "
                        f"non-MongoDBSourceNative source ({type(source).__name__!r})."
                    )

                # Fetch all documents for this feature view
                pipeline = [{"$match": {"feature_view": fv_name}}]
                docs = _fetch_documents(
                    connection_string, db_name, collection, pipeline
                )

                if not docs:
                    for f in features:
                        col = f"{fv_name}__{f}" if full_feature_names else f
                        result[col] = None
                    continue

                # Build feature DataFrame
                feature_rows = []
                for doc in docs:
                    row = {
                        "_entity_id": doc["entity_id"],
                        "_fv_ts": doc["event_timestamp"],
                    }
                    feat_data = doc.get("features", {})
                    for f in features:
                        row[f] = feat_data.get(f)
                    feature_rows.append(row)

                feature_df = pd.DataFrame(feature_rows)
                if feature_df["_fv_ts"].dt.tz is None:
                    feature_df["_fv_ts"] = pd.to_datetime(
                        feature_df["_fv_ts"], utc=True
                    )
                feature_df = feature_df.sort_values("_fv_ts")

                # Rename features if full_feature_names
                col_rename = {
                    f: (f"{fv_name}__{f}" if full_feature_names else f)
                    for f in features
                }
                feature_df = feature_df.rename(columns=col_rename)
                out_features = list(col_rename.values())

                # Point-in-time join using merge_asof
                merged = pd.merge_asof(
                    result,
                    feature_df,
                    left_on=event_timestamp_col,
                    right_on="_fv_ts",
                    by="_entity_id",
                    direction="backward",
                )

                # Apply TTL: null out stale features
                if fv.ttl:
                    cutoff = merged[event_timestamp_col] - fv.ttl
                    too_old = merged["_fv_ts"] < cutoff
                    for col in out_features:
                        merged.loc[too_old, col] = None

                result = merged.drop(columns=["_fv_ts"], errors="ignore")

            # Remove internal entity_id column
            result = result.drop(columns=["_entity_id"], errors="ignore")
            return pyarrow.Table.from_pandas(result, preserve_index=False)

        return MongoDBNativeRetrievalJob(
            query_fn=_run,
            full_feature_names=full_feature_names,
        )
