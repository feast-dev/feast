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
MongoDB Offline Store.

Single-collection schema.  Key optimizations:

1. K-collapse: feature views that share the same join key set are batched
   into a single ``$match + $sort`` aggregation instead of K separate find
   queries.  Reduces round-trips from K to |unique join key signatures|.

2. Server-side deduplication (scoring path): when entity_df has unique
   entity IDs the aggregation adds a ``$group`` stage that returns at most
   one document per (entity_id, feature_view) pair — O(N×K) transfer
   instead of O(N×P×K).  The compound index backs the entire pipeline,
   making per-entity cost O(log P) rather than O(P).

   For training data (repeated entity IDs at different timestamps) the
   ``$group`` optimisation is skipped and ``merge_asof`` is used instead,


Index (created lazily on first use)::

    (entity_id ASC, feature_view ASC, event_timestamp DESC, created_at DESC)
"""

import warnings
from collections import defaultdict
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import pandas as pd
import pyarrow

try:
    from pymongo import ASCENDING, DESCENDING, MongoClient
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
from feast.infra.key_encoding_utils import deserialize_entity_key, serialize_entity_key
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
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import mongodb_to_feast_value_type
from feast.value_type import ValueType

# Cache: avoid re-creating the compound index on every call
_indexes_ensured: Set[str] = set()

# Chunk sizes — exposed at module level so tests can patch them.
_CHUNK_SIZE = 50_000
_MONGO_BATCH_SIZE = 10_000


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class MongoDBOfflineStoreConfig(FeastConfigBaseModel):
    """Configuration for the MongoDB offline store (single shared collection)."""

    type: StrictStr = "feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb.MongoDBOfflineStore"

    connection_string: StrictStr = "mongodb://localhost:27017"
    """MongoDB connection URI"""

    database: StrictStr = "feast"
    """MongoDB database name"""

    collection: StrictStr = "feature_history"
    """Single collection shared by all feature views"""


# ---------------------------------------------------------------------------
# Data source
# ---------------------------------------------------------------------------


class MongoDBSource(DataSource):
    """Data source for the MongoDB offline store.

    The ``name`` field is used as the ``feature_view`` discriminator inside
    the single shared collection.
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
            field_mapping=field_mapping or {},
            description=description,
            tags=tags or {},
            owner=owner,
        )

    @property
    def feature_view_name(self) -> str:
        return self.name

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.CUSTOM_SOURCE

    def validate(self, config: RepoConfig) -> None:
        pass

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> "MongoDBSource":
        assert data_source.HasField("custom_options")
        return MongoDBSource(
            name=data_source.name,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        import json

        return DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb.MongoDBSource",
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

    def get_table_query_string(self) -> str:
        return self.name

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> List[Tuple[str, str]]:
        """Infer column names and types by reading a sample document from MongoDB."""
        if MongoClient is None:
            raise FeastExtrasDependencyImportError("pymongo", "mongodb")
        client: Any = MongoClient(config.offline_store.connection_string)
        try:
            coll = client[config.offline_store.database][
                config.offline_store.collection
            ]
            doc = coll.find_one({"feature_view": self.feature_view_name})
            if doc is None:
                return []
            result: List[Tuple[str, str]] = []
            # Entity key is binary — join keys are inferred from the FeatureView,
            # not from the document.  Expose event_timestamp and created_at.
            if "event_timestamp" in doc:
                result.append(("event_timestamp", "datetime"))
            if "created_at" in doc:
                result.append(("created_at", "datetime"))
            features = doc.get("features", {})
            if isinstance(features, dict):
                for k, v in features.items():
                    if isinstance(v, bool):
                        result.append((k, "bool"))
                    elif isinstance(v, int):
                        result.append((k, "int64"))
                    elif isinstance(v, float):
                        result.append((k, "float64"))
                    elif isinstance(v, str):
                        result.append((k, "string"))
                    elif isinstance(v, list):
                        result.append((k, "list"))
                    elif isinstance(v, dict):
                        result.append((k, "dict"))
                    else:
                        result.append((k, "object"))
            return result
        finally:
            client.close()

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return mongodb_to_feast_value_type


# ---------------------------------------------------------------------------
# Retrieval job
# ---------------------------------------------------------------------------


class MongoDBRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query_fn: Callable[[], pyarrow.Table],
        full_feature_names: bool,
        config: RepoConfig,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        self._query_fn = query_fn
        self._full_feature_names = full_feature_names
        self._config = config
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[Any]:
        return []

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        return self._query_fn()

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ) -> None:
        import os

        from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

        if isinstance(storage, SavedDatasetFileStorage):
            path = storage.file_options.uri
        elif hasattr(storage, "path"):
            path = storage.path  # type: ignore[union-attr]
        else:
            raise ValueError(
                f"MongoDBRetrievalJob.persist does not support "
                f"{type(storage).__name__!r}. Use SavedDatasetFileStorage."
            )

        if not allow_overwrite and os.path.exists(path):
            raise SavedDatasetLocationAlreadyExists(location=path)
        self.to_df().to_parquet(path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fetch_documents(
    client: Any, db_name: str, collection_name: str, pipeline: List[Dict]
) -> List[Dict]:
    db = client[db_name]
    return list(db[collection_name].aggregate(pipeline))


def _serialize_entity_key_from_row(
    row: pd.Series,
    join_keys: List[str],
    entity_key_version: int,
    join_key_types: Dict[str, ValueType],
) -> bytes:
    entity_key = EntityKeyProto()
    for jk in sorted(join_keys):
        val = row[jk]
        entity_key.join_keys.append(jk)
        proto_val = ValueProto()
        vtype = join_key_types.get(jk, ValueType.UNKNOWN)
        if vtype == ValueType.INT32:
            proto_val.int32_val = int(val)
        elif vtype == ValueType.INT64 or isinstance(val, int):
            proto_val.int64_val = int(val)
        elif vtype == ValueType.STRING or isinstance(val, str):
            proto_val.string_val = str(val)
        elif isinstance(val, float):
            proto_val.double_val = float(val)
        else:
            proto_val.int64_val = int(val)
        entity_key.entity_values.append(proto_val)
    return serialize_entity_key(entity_key, entity_key_version)


def _expand_entity_id_column(
    df: pd.DataFrame,
    join_key_columns: List[str],
    entity_key_version: int,
) -> pd.DataFrame:
    """Deserialize ``entity_id`` bytes into individual join key columns.

    Each ``entity_id`` value is deserialized via ``deserialize_entity_key``
    and the resulting join key names/values are added as new columns.
    The ``entity_id`` column is then dropped.
    """
    if "entity_id" not in df.columns or df.empty:
        return df

    def _extract(eid_bytes: bytes) -> Dict[str, Any]:
        ek = deserialize_entity_key(eid_bytes, entity_key_version)
        row: Dict[str, Any] = {}
        for key, val in zip(ek.join_keys, ek.entity_values):
            which = val.WhichOneof("val")
            row[key] = getattr(val, which) if which else None
        return row

    expanded = pd.DataFrame([_extract(eid) for eid in df["entity_id"]])
    # Only keep the columns the caller asked for (in case the serialized
    # key contains more keys than expected).
    for col in join_key_columns:
        if col in expanded.columns:
            df[col] = expanded[col].values
    df = df.drop(columns=["entity_id"], errors="ignore")
    return df


# ---------------------------------------------------------------------------
# Offline store
# ---------------------------------------------------------------------------


class MongoDBOfflineStore(OfflineStore):
    """MongoDB offline store using a single collection and grouped aggregation.

    Key optimizations:
    - Collapsing K feature-view queries into one aggregation per join-key group
    - Using server-side ``$group`` (O(log P) with index) for the scoring path
    """

    @staticmethod
    def _ensure_indexes(client: Any, db_name: str, collection_name: str) -> None:
        """Create the compound index that enables O(log P) per-entity lookups."""
        collection = client[db_name][collection_name]
        target_key = [
            ("entity_id", ASCENDING),
            ("feature_view", ASCENDING),
            ("event_timestamp", DESCENDING),
            ("created_at", DESCENDING),
        ]
        existing = collection.index_information()
        for idx_info in existing.values():
            if idx_info.get("key") == target_key:
                return
        collection.create_index(target_key, name="entity_fv_ts_idx", background=True)

    @staticmethod
    def _get_client_and_ensure_indexes(config: RepoConfig) -> Any:
        if MongoClient is None:
            raise FeastExtrasDependencyImportError("pymongo", "mongodb")
        conn_str = config.offline_store.connection_string
        db_name = config.offline_store.database
        collection = config.offline_store.collection
        cache_key = f"{conn_str}/{db_name}/{collection}"
        client: Any = MongoClient(conn_str)
        if cache_key not in _indexes_ensured:
            MongoDBOfflineStore._ensure_indexes(client, db_name, collection)
            _indexes_ensured.add(cache_key)
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
        if not isinstance(data_source, MongoDBSource):
            raise ValueError(
                f"MongoDBOfflineStore expected MongoDBSource, "
                f"got {type(data_source).__name__!r}."
            )
        warnings.warn(
            "MongoDB offline store is in preview. API may change without notice.",
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

        entity_key_version = config.entity_key_serialization_version

        def _run() -> pyarrow.Table:
            client = MongoDBOfflineStore._get_client_and_ensure_indexes(config)
            try:
                docs = _fetch_documents(client, db_name, collection, pipeline)
                if not docs:
                    return pyarrow.Table.from_pydict({})
                df = pd.DataFrame(docs)
                df = _expand_entity_id_column(df, join_key_columns, entity_key_version)
                if not df.empty and "event_timestamp" in df.columns:
                    if df["event_timestamp"].dt.tz is None:
                        df["event_timestamp"] = pd.to_datetime(
                            df["event_timestamp"], utc=True
                        )
                return pyarrow.Table.from_pandas(df, preserve_index=False)
            finally:
                client.close()

        return MongoDBRetrievalJob(
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
        if not isinstance(data_source, MongoDBSource):
            raise ValueError(
                f"MongoDBOfflineStore expected MongoDBSource, "
                f"got {type(data_source).__name__!r}."
            )
        warnings.warn(
            "MongoDB offline store is in preview. API may change without notice.",
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
        project_stage: Dict[str, Any] = {"_id": 0, "entity_id": 1, "event_timestamp": 1}
        if created_timestamp_column:
            project_stage["created_at"] = 1
        for feat in feature_name_columns:
            project_stage[feat] = f"$features.{feat}"
        pipeline = [{"$match": match_filter}, {"$project": project_stage}]
        entity_key_version = config.entity_key_serialization_version

        def _run() -> pyarrow.Table:
            client = MongoDBOfflineStore._get_client_and_ensure_indexes(config)
            try:
                docs = _fetch_documents(client, db_name, collection, pipeline)
                if not docs:
                    return pyarrow.Table.from_pydict({})
                df = pd.DataFrame(docs)
                df = _expand_entity_id_column(df, join_key_columns, entity_key_version)
                if not df.empty and "event_timestamp" in df.columns:
                    if df["event_timestamp"].dt.tz is None:
                        df["event_timestamp"] = pd.to_datetime(
                            df["event_timestamp"], utc=True
                        )
                return pyarrow.Table.from_pandas(df, preserve_index=False)
            finally:
                client.close()

        return MongoDBRetrievalJob(
            query_fn=_run, full_feature_names=False, config=config
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
        strict_pit: bool = True,
    ) -> RetrievalJob:
        """Fetch historical features using grouped aggregation.

        Groups feature views by join key signature so that FVs sharing the
        same entity key are handled in a single MongoDB aggregation instead
        of K separate queries.

        Scoring path (unique entity IDs in entity_df):
            Uses ``$match + $sort + $group`` — server returns at most one
            document per (entity_id, feature_view).  The compound index
            makes per-entity cost O(log P).  Python post-filters the result.

        Training path (repeated entity IDs at different timestamps):
            Omits ``$group`` and uses ``merge_asof`` in Python, matching
            standard PIT behaviour but still with K-collapsed queries.

        Args:
            strict_pit: When True (default) features whose document timestamp
                is strictly after the entity request timestamp are returned as
                NULL — this is the safe training/evaluation default.  Set to
                False for real-time scoring where you want the most recent
                observation even if it post-dates the nominal request time.
        """
        if isinstance(entity_df, str):
            raise ValueError(
                "MongoDBOfflineStore does not support SQL entity_df strings."
            )
        warnings.warn(
            "MongoDB offline store is in preview. API may change without notice.",
            RuntimeWarning,
        )

        db_name = config.offline_store.database
        feature_collection = config.offline_store.collection
        entity_key_version = config.entity_key_serialization_version

        entity_schema = dict(zip(entity_df.columns, entity_df.dtypes))
        event_timestamp_col = infer_event_timestamp_from_entity_df(entity_schema)

        # Feature refs use projection names (e.g. "origin:temperature")
        fv_to_features: Dict[str, List[str]] = defaultdict(list)
        for ref in feature_refs:
            fv_name, feat_name = ref.split(":", 1)
            fv_to_features[fv_name].append(feat_name)

        # All dicts keyed by projection name (name_to_use), not fv.name,
        # because entity mapping creates multiple projections of the same FV.
        fv_by_proj: Dict[str, FeatureView] = {
            fv.projection.name_to_use(): fv for fv in feature_views
        }

        # projection_name → MongoDB feature_view discriminator value
        # (the data source name, NOT the FeatureView name)
        fv_mongo_name: Dict[str, str] = {}
        for fv in feature_views:
            proj = fv.projection.name_to_use()
            src = fv.batch_source
            fv_mongo_name[proj] = (
                src.feature_view_name
                if isinstance(src, MongoDBSource)
                else getattr(src, "name", fv.name)
            )

        # projection_name → mapped join keys (as in entity_df columns)
        fv_mapped_join_keys: Dict[str, List[str]] = {
            fv.projection.name_to_use(): list(
                get_expected_join_keys(project, [fv], registry)
            )
            for fv in feature_views
        }

        # projection_name → {mapped_key → original_key}
        fv_reverse_jk: Dict[str, Dict[str, str]] = {
            fv.projection.name_to_use(): {
                v: k for k, v in fv.projection.join_key_map.items()
            }
            for fv in feature_views
        }

        # projection_name → original join key types (keyed by original name)
        fv_jk_types_original: Dict[str, Dict[str, ValueType]] = {
            fv.projection.name_to_use(): {
                ec.name: ec.dtype.to_value_type() for ec in fv.entity_columns
            }
            for fv in feature_views
        }

        # projection_name → reverse field_mapping (feast_name → source_col_name)
        fv_reverse_fm: Dict[str, Dict[str, str]] = {}
        for fv in feature_views:
            proj = fv.projection.name_to_use()
            fm = fv.batch_source.field_mapping if fv.batch_source else {}
            fv_reverse_fm[proj] = {v: k for k, v in fm.items()} if fm else {}

        CHUNK_SIZE = _CHUNK_SIZE
        MONGO_BATCH_SIZE = _MONGO_BATCH_SIZE

        def _chunk_dataframe(
            df: pd.DataFrame, size: int
        ) -> Generator[pd.DataFrame, None, None]:
            for i in range(0, len(df), size):
                yield df.iloc[i : i + size]

        def _run_single(entity_subset_df: pd.DataFrame, coll: Any) -> pd.DataFrame:
            result = entity_subset_df.copy()
            if not pd.api.types.is_datetime64_any_dtype(result[event_timestamp_col]):
                result[event_timestamp_col] = pd.to_datetime(
                    result[event_timestamp_col], utc=True
                )
            elif result[event_timestamp_col].dt.tz is None:
                result[event_timestamp_col] = pd.to_datetime(
                    result[event_timestamp_col], utc=True
                )

            max_ts = result[event_timestamp_col].max()
            min_ts = result[event_timestamp_col].min()

            # Process each feature view projection independently.
            # (Different projections of the same FV have different
            #  entity key mappings and must be handled separately.)
            for proj_name, features in fv_to_features.items():
                fv = fv_by_proj.get(proj_name)
                if fv is None:
                    for feat in features:
                        col = f"{proj_name}__{feat}" if full_feature_names else feat
                        result[col] = None
                    continue

                mongo_fv_name = fv_mongo_name[proj_name]
                mapped_keys = fv_mapped_join_keys[proj_name]
                reverse_jk = fv_reverse_jk[proj_name]
                orig_key_types = fv_jk_types_original[proj_name]
                reverse_fm = fv_reverse_fm[proj_name]

                # Serialize entity keys: read values from MAPPED columns in
                # entity_df, but serialize with ORIGINAL join key names to
                # match the bytes stored in MongoDB.
                _mk = mapped_keys
                _rjk = reverse_jk
                _okt = orig_key_types

                def _ser(row, __mk=_mk, __rjk=_rjk, __okt=_okt):
                    ek = EntityKeyProto()
                    orig_keys = sorted([__rjk.get(m, m) for m in __mk])
                    o2m = {__rjk.get(m, m): m for m in __mk}
                    for ok in orig_keys:
                        mk = o2m[ok]
                        val = row[mk]
                        ek.join_keys.append(ok)
                        pv = ValueProto()
                        vt = __okt.get(ok, ValueType.UNKNOWN)
                        if vt == ValueType.INT32:
                            pv.int32_val = int(val)
                        elif vt == ValueType.INT64 or isinstance(val, int):
                            pv.int64_val = int(val)
                        elif vt == ValueType.STRING or isinstance(val, str):
                            pv.string_val = str(val)
                        elif isinstance(val, float):
                            pv.double_val = float(val)
                        else:
                            pv.int64_val = int(val)
                        ek.entity_values.append(pv)
                    return serialize_entity_key(ek, entity_key_version)

                eid_col = f"_eid_{proj_name}"
                result[eid_col] = result.apply(_ser, axis=1)
                unique_eids = result[eid_col].unique().tolist()

                # Detect scoring vs training path per-FV.
                #
                # The scoring path ($group $first) requires unique entity
                # IDs for THIS FV — otherwise $group discards rows that
                # share an entity_id.
                #
                # When strict_pit=True, there is an additional constraint:
                # all entity request timestamps must be identical.  The
                # $match uses $lte: max_ts which is correct only when
                # every entity shares the same request time.  When
                # timestamps differ, $group may pick a doc that is after
                # a specific entity's request time; the Python future_mask
                # would null it, but the valid older doc was already
                # discarded by $group.
                #
                # When strict_pit=False, there is no $lte filter and no
                # future_mask — we want the globally latest doc per entity
                # regardless of request time, so $group $first is always
                # correct.
                unique_entities = result[eid_col].nunique() == len(result)
                scoring_path = unique_entities and (
                    not strict_pit or result[event_timestamp_col].nunique() == 1
                )

                # TTL filter — use min_ts for the lower bound so that
                # documents needed for early entity rows are included.
                # Per-row TTL enforcement happens in the merge_asof path.
                fv_ttl = fv.ttl if fv else None
                ts_filter: Dict[str, Any] = {"$lte": max_ts} if strict_pit else {}
                if fv_ttl:
                    lower_ref = min_ts if strict_pit else datetime.now(tz=timezone.utc)
                    ts_filter["$gte"] = lower_ref - fv_ttl

                # Query MongoDB
                all_docs: List[Dict] = []
                for i in range(0, len(unique_eids), MONGO_BATCH_SIZE):
                    batch_ids = unique_eids[i : i + MONGO_BATCH_SIZE]
                    match_q: Dict[str, Any] = {
                        "entity_id": {"$in": batch_ids},
                        "feature_view": mongo_fv_name,
                    }
                    if ts_filter:
                        match_q["event_timestamp"] = ts_filter

                    if scoring_path:
                        pipeline: List[Dict] = [
                            {"$match": match_q},
                            {
                                "$sort": {
                                    "entity_id": 1,
                                    "event_timestamp": -1,
                                    "created_at": -1,
                                }
                            },
                            {
                                "$group": {
                                    "_id": "$entity_id",
                                    "event_timestamp": {"$first": "$event_timestamp"},
                                    "features": {"$first": "$features"},
                                    "created_at": {"$first": "$created_at"},
                                }
                            },
                            {
                                "$project": {
                                    "_id": 0,
                                    "entity_id": "$_id",
                                    "event_timestamp": 1,
                                    "features": 1,
                                    "created_at": 1,
                                }
                            },
                        ]
                    else:
                        pipeline = [{"$match": match_q}]

                    all_docs.extend(list(coll.aggregate(pipeline)))

                if not all_docs:
                    for feat in features:
                        col = f"{proj_name}__{feat}" if full_feature_names else feat
                        result[col] = None
                    result = result.drop(columns=[eid_col], errors="ignore")
                    continue

                fv_df = pd.DataFrame(all_docs)
                fv_df = fv_df.rename(columns={"entity_id": eid_col})

                # Extract features from nested dict, applying reverse field_mapping.
                # Using .apply() instead of json_normalize preserves complex types
                # (dicts for Map/Struct, lists for Array).
                if "features" in fv_df.columns:
                    for feat in features:
                        src_col = reverse_fm.get(feat, feat)
                        fv_df[feat] = fv_df["features"].apply(
                            lambda d, _s=src_col: (
                                d.get(_s) if isinstance(d, dict) else None
                            )
                        )
                    fv_df = fv_df.drop(columns=["features"])

                if fv_df["event_timestamp"].dt.tz is None:
                    fv_df["event_timestamp"] = pd.to_datetime(
                        fv_df["event_timestamp"], utc=True
                    )

                if scoring_path:
                    fv_join_cols = [eid_col, "event_timestamp"] + [
                        f for f in features if f in fv_df.columns
                    ]
                    fv_join = fv_df[fv_join_cols].rename(
                        columns={"event_timestamp": "_fv_ts"}
                    )
                    merged = result[[eid_col, event_timestamp_col]].merge(
                        fv_join, on=eid_col, how="left"
                    )
                    if strict_pit:
                        future_mask = merged["_fv_ts"] > merged[event_timestamp_col]
                    else:
                        future_mask = pd.Series(
                            [False] * len(merged), index=merged.index
                        )
                    if fv_ttl:
                        ttl_mask = merged["_fv_ts"] < (
                            merged[event_timestamp_col] - fv_ttl
                        )
                        bad_mask = future_mask | ttl_mask
                    else:
                        bad_mask = future_mask
                    for feat in features:
                        col = f"{proj_name}__{feat}" if full_feature_names else feat
                        vals = (
                            merged[feat].copy()
                            if feat in merged.columns
                            else pd.Series([None] * len(merged), dtype=object)
                        )
                        vals[bad_mask | merged["_fv_ts"].isna()] = None
                        result[col] = vals.values
                else:
                    # merge_asof path (training data)
                    result = result.sort_values(event_timestamp_col).reset_index(
                        drop=True
                    )
                    fv_df = fv_df.sort_values(
                        ["event_timestamp", "created_at"]
                    ).reset_index(drop=True)
                    merge_cols = [eid_col, "event_timestamp"] + [
                        f for f in features if f in fv_df.columns
                    ]
                    fv_df_subset = fv_df[
                        [c for c in merge_cols if c in fv_df.columns]
                    ].copy()
                    fv_df_subset = fv_df_subset.rename(
                        columns={"event_timestamp": "_fv_ts"}
                    )
                    fv_prefix = f"__fv_{proj_name}__"
                    fv_df_subset = fv_df_subset.rename(
                        columns={
                            f: f"{fv_prefix}{f}"
                            for f in features
                            if f in fv_df_subset.columns
                        }
                    )
                    result = pd.merge_asof(
                        result,
                        fv_df_subset,
                        left_on=event_timestamp_col,
                        right_on="_fv_ts",
                        by=eid_col,
                        direction="backward",
                    )
                    if fv_ttl:
                        cutoff = result[event_timestamp_col] - fv_ttl
                        stale = result["_fv_ts"] < cutoff
                        for feat in features:
                            tc = f"{fv_prefix}{feat}"
                            if tc in result.columns:
                                result.loc[stale, tc] = None
                    for feat in features:
                        tc = f"{fv_prefix}{feat}"
                        col = f"{proj_name}__{feat}" if full_feature_names else feat
                        if tc in result.columns:
                            if col in result.columns:
                                result = result.drop(columns=[col])
                            result = result.rename(columns={tc: col})
                        elif col not in result.columns:
                            result[col] = None
                    result = result.drop(columns=["_fv_ts"], errors="ignore")

                result = result.drop(columns=[eid_col], errors="ignore")

            return result

        def _run() -> pyarrow.Table:
            working_df = entity_df.copy()
            working_df["_row_idx"] = range(len(working_df))

            client = MongoDBOfflineStore._get_client_and_ensure_indexes(config)
            try:
                coll = client[db_name][feature_collection]
                if len(working_df) <= CHUNK_SIZE:
                    result_df = _run_single(working_df, coll)
                else:
                    chunks = [
                        _run_single(chunk, coll)
                        for chunk in _chunk_dataframe(working_df, CHUNK_SIZE)
                    ]
                    result_df = pd.concat(chunks, ignore_index=True)
            finally:
                client.close()

            result_df = result_df.sort_values("_row_idx").reset_index(drop=True)
            result_df = result_df.drop(columns=["_row_idx"], errors="ignore")

            if not result_df.empty and event_timestamp_col in result_df.columns:
                if result_df[event_timestamp_col].dt.tz is None:
                    result_df[event_timestamp_col] = pd.to_datetime(
                        result_df[event_timestamp_col], utc=True
                    )

            return pyarrow.Table.from_pandas(result_df, preserve_index=False)

        return MongoDBRetrievalJob(
            query_fn=_run,
            full_feature_names=full_feature_names,
            config=config,
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """Write a batch of feature observations into the feature_history collection.

        Each row in *table* is stored as one document::

            {
                "entity_id":       <serialized entity key bytes>,
                "feature_view":    <feature view name>,
                "features":        {<feature_name>: <value>, ...},
                "event_timestamp": <datetime>,
                "created_at":      <datetime>,
            }

        Writes are append-only (no upsert).  Conflict resolution at read time:
        pull_latest picks the highest ``created_at``; the scoring path
        ``$sort created_at DESC`` → ``$group $first`` also picks the highest.

        Args:
            config: Feast repo configuration.
            feature_view: The feature view being written; must have a
                MongoDBSource batch source.
            table: Arrow table with join key columns, feature columns,
                ``event_timestamp``, and optionally ``created_at``.
            progress: Optional callback invoked with the row count after each
                batch insert.
        """
        if not isinstance(feature_view.batch_source, MongoDBSource):
            raise ValueError(
                f"MongoDBOfflineStore.offline_write_batch expected a MongoDBSource "
                f"batch source, got {type(feature_view.batch_source).__name__!r}."
            )

        entity_key_version = config.entity_key_serialization_version
        db_name = config.offline_store.database
        collection_name = config.offline_store.collection

        # Use original (unmapped) join key names so that the serialized
        # entity_id bytes match those produced by get_historical_features,
        # which also serializes with original names (see _ser at line ~662).
        join_key_types: Dict[str, ValueType] = {
            ec.name: ec.dtype.to_value_type() for ec in feature_view.entity_columns
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

        # Use the batch source name as the MongoDB discriminator so that
        # data written via push/write_to_offline_store lands in the same
        # collection partition as the initial ingest from create_data_source.
        mongo_fv_name = feature_view.batch_source.feature_view_name

        docs = []
        for _, row in df.iterrows():
            features: Dict[str, Any] = {}
            for col in feature_cols:
                val = row[col]
                try:
                    is_na = pd.isna(val)
                    if isinstance(is_na, bool) and is_na:
                        continue
                except (ValueError, TypeError):
                    pass  # non-scalar (list/array) — not NA
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
                    "feature_view": mongo_fv_name,
                    "features": features,
                    "event_timestamp": (
                        row[timestamp_field].to_pydatetime()
                        if hasattr(row[timestamp_field], "to_pydatetime")
                        else row[timestamp_field]
                    ),
                    "created_at": created_at,
                }
            )

        client = MongoDBOfflineStore._get_client_and_ensure_indexes(config)
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
