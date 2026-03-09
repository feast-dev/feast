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

import warnings
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Union

import ibis
import pandas as pd
import pyarrow
from ibis.expr.types import Table
from pydantic import StrictStr

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None  # type: ignore[assignment,misc]

from feast.data_source import DataSource
from feast.errors import (
    FeastExtrasDependencyImportError,
    SavedDatasetLocationAlreadyExists,
)
from feast.feature_view import FeatureView
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_source import (
    MongoDBSource,
)
from feast.infra.offline_stores.ibis import (
    get_historical_features_ibis,
    pull_all_from_table_or_query_ibis,
    pull_latest_from_table_or_query_ibis,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    infer_event_timestamp_from_entity_df,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage


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
            "MongoDB offline store is in alpha. API may change without notice.",
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
            "MongoDB offline store is in alpha. API may change without notice.",
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
            "MongoDB offline store is in alpha. API may change without notice.",
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
        client: Any = MongoClient(connection_string, tz_aware=True)
        try:
            docs = list(client[db_name][data_source.collection].find({}, {"_id": 0}))
        finally:
            client.close()

        df = pd.DataFrame(docs)
        if df.empty:
            return ibis.memtable(df)

        # Ensure datetime-like columns are timezone-aware UTC pandas timestamps.
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if df[col].dt.tz is None:
                    df[col] = pd.to_datetime(df[col], utc=True)
            elif df[col].dtype == object and len(df[col].dropna()) > 0:
                sample = df[col].dropna().iloc[0]
                if isinstance(sample, datetime):
                    try:
                        df[col] = pd.to_datetime(df[col], utc=True)
                    except Exception:
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
        client: Any = MongoClient(connection_string, tz_aware=True)
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


# ---------------------------------------------------------------------------
# Native MQL implementation
# ---------------------------------------------------------------------------


class MongoDBOfflineStoreNativeConfig(FeastConfigBaseModel):
    """Configuration for the MongoDB native-MQL offline store."""

    type: StrictStr = "feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb.MongoDBOfflineStoreNative"
    """Offline store type selector"""

    connection_string: StrictStr = "mongodb://localhost:27017"
    """MongoDB connection URI"""

    database: StrictStr = "feast"
    """Default MongoDB database name"""


def _fetch_collection_as_arrow(
    connection_string: str,
    db_name: str,
    collection: str,
    pipeline: Optional[List[Dict]] = None,
) -> pyarrow.Table:
    """Run an aggregation pipeline (or full scan) via PyMongo and return a pyarrow Table.

    If *pipeline* is None the entire collection is scanned (``_id`` excluded).
    The ``_id`` field is stripped from every result document before conversion.
    """
    if MongoClient is None:
        raise FeastExtrasDependencyImportError("mongodb", "pymongo is not installed.")
    client: Any = MongoClient(connection_string, tz_aware=True)
    try:
        if pipeline is not None:
            docs = list(client[db_name][collection].aggregate(pipeline))
        else:
            docs = list(client[db_name][collection].find({}, {"_id": 0}))
    finally:
        client.close()

    if not docs:
        return pyarrow.table({})

    for doc in docs:
        doc.pop("_id", None)

    return pyarrow.Table.from_pylist(docs)


class MongoDBNativeRetrievalJob(RetrievalJob):
    """A RetrievalJob whose results come from a lazy PyMongo query callable.

    The callable is only executed when the caller materialises the job (e.g.
    ``to_df()``, ``to_arrow()``, ``persist()``).
    """

    def __init__(
        self,
        query_fn: Callable[[], pyarrow.Table],
        full_feature_names: bool,
        on_demand_feature_views: List,
        metadata: Optional[RetrievalMetadata],
        config: RepoConfig,
    ) -> None:
        super().__init__()
        self._query_fn = query_fn
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        self._config = config

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        return self._query_fn()

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        return self._to_arrow_internal().to_pandas()

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List:
        return self._on_demand_feature_views

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ) -> None:
        if MongoClient is None:
            raise FeastExtrasDependencyImportError(
                "mongodb", "pymongo is not installed."
            )
        data_source = storage.to_data_source()
        if not isinstance(data_source, MongoDBSource):
            raise ValueError(
                f"MongoDBNativeRetrievalJob.persist expected a MongoDBSource storage, "
                f"got {type(data_source).__name__!r}."
            )
        table = self._to_arrow_internal()
        connection_string = self._config.offline_store.connection_string
        db_name = data_source.database or self._config.offline_store.database
        location = f"{db_name}.{data_source.collection}"
        client: Any = MongoClient(connection_string, tz_aware=True)
        try:
            coll = client[db_name][data_source.collection]
            if not allow_overwrite and coll.estimated_document_count() > 0:
                raise SavedDatasetLocationAlreadyExists(location=location)
            coll.drop()
            records = table.to_pylist()
            if records:
                coll.insert_many(records)
        finally:
            client.close()


class MongoDBOfflineStoreNative(OfflineStore):
    """Offline store backed by MongoDB using native MQL aggregation pipelines.

    Compared with :class:`MongoDBOfflineStoreIbis`, this implementation avoids
    the Ibis dependency entirely.  The three main workflows map to:

    * ``offline_write_batch``           – Arrow → ``insert_many``
    * ``pull_latest_from_table_or_query`` – ``$match`` → ``$sort`` → ``$group``
    * ``pull_all_from_table_or_query``  – ``$match`` → ``$project``
    * ``get_historical_features``       – per-collection fetch + ``merge_asof``
    """

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        if MongoClient is None:
            raise FeastExtrasDependencyImportError(
                "mongodb", "pymongo is not installed."
            )
        data_source = feature_view.batch_source
        if not isinstance(data_source, MongoDBSource):
            raise ValueError(
                f"MongoDBOfflineStoreNative.offline_write_batch expected a MongoDBSource, "
                f"got {type(data_source).__name__!r}."
            )
        connection_string = config.offline_store.connection_string
        db_name = data_source.database or config.offline_store.database
        records = table.to_pylist()
        client: Any = MongoClient(connection_string, tz_aware=True)
        try:
            coll = client[db_name][data_source.collection]
            if records:
                coll.insert_many(records)
                if progress:
                    progress(len(records))
        finally:
            client.close()

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
                f"MongoDBOfflineStoreNative expected a MongoDBSource, "
                f"got {type(data_source).__name__!r}."
            )
        warnings.warn(
            "MongoDB offline store (native) is in alpha. API may change without notice.",
            RuntimeWarning,
        )
        start_utc = start_date.astimezone(tz=timezone.utc)
        end_utc = end_date.astimezone(tz=timezone.utc)
        connection_string = config.offline_store.connection_string
        db_name = data_source.database or config.offline_store.database
        collection = data_source.collection

        sort_spec: Dict = {timestamp_field: -1}
        if created_timestamp_column:
            sort_spec[created_timestamp_column] = -1

        group_id = {k: f"${k}" for k in join_key_columns}
        group_stage: Dict = {
            "_id": group_id,  # todo this isn't correct. or i don't follow
            **{f: {"$first": f"${f}"} for f in feature_name_columns},
            timestamp_field: {"$first": f"${timestamp_field}"},
        }
        if created_timestamp_column:
            group_stage[created_timestamp_column] = {
                "$first": f"${created_timestamp_column}"
            }

        project_stage: Dict = {
            "_id": 0,
            **{k: f"$_id.{k}" for k in join_key_columns},  # todo here too
            **{f: 1 for f in feature_name_columns},
            timestamp_field: 1,
        }
        if created_timestamp_column:
            project_stage[created_timestamp_column] = 1

        pipeline = [
            {"$match": {timestamp_field: {"$gte": start_utc, "$lte": end_utc}}},
            {"$sort": sort_spec},
            {"$group": group_stage},
            {"$project": project_stage},
        ]

        def _run() -> pyarrow.Table:
            return _fetch_collection_as_arrow(
                connection_string, db_name, collection, pipeline
            )

        return MongoDBNativeRetrievalJob(
            query_fn=_run,
            full_feature_names=False,
            on_demand_feature_views=[],
            metadata=None,
            config=config,
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
                f"MongoDBOfflineStoreNative expected a MongoDBSource, "
                f"got {type(data_source).__name__!r}."
            )
        warnings.warn(
            "MongoDB offline store (native) is in alpha. API may change without notice.",
            RuntimeWarning,
        )
        connection_string = config.offline_store.connection_string
        db_name = data_source.database or config.offline_store.database
        collection = data_source.collection

        fields = join_key_columns + feature_name_columns + [timestamp_field]
        if created_timestamp_column:
            fields.append(created_timestamp_column)

        match_filter: Dict = {}
        if start_date or end_date:
            ts_filter: Dict = {}
            if start_date:
                ts_filter["$gte"] = start_date.astimezone(tz=timezone.utc)
            if end_date:
                ts_filter["$lte"] = end_date.astimezone(tz=timezone.utc)
            match_filter[timestamp_field] = ts_filter

        pipeline = [
            {"$match": match_filter},
            {"$project": {"_id": 0, **{f: 1 for f in fields}}},
        ]

        def _run() -> pyarrow.Table:
            return _fetch_collection_as_arrow(
                connection_string, db_name, collection, pipeline
            )

        return MongoDBNativeRetrievalJob(
            query_fn=_run,
            full_feature_names=False,
            on_demand_feature_views=[],
            metadata=None,
            config=config,
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
        if isinstance(entity_df, str):
            raise ValueError(
                "MongoDBOfflineStoreNative does not support SQL entity_df strings. "
                "Pass a pandas DataFrame instead."
            )
        warnings.warn(
            "MongoDB offline store (native) is in alpha. API may change without notice.",  # todo change wording: alpha -> preview
            RuntimeWarning,
        )
        connection_string = config.offline_store.connection_string
        default_db = config.offline_store.database

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
            # Ensure the entity timestamp is tz-aware UTC for merge_asof
            if result[event_timestamp_col].dt.tz is None:
                result[event_timestamp_col] = pd.to_datetime(
                    result[event_timestamp_col], utc=True
                )
            result = result.sort_values(event_timestamp_col)

            for fv_name, features in fv_to_features.items():
                fv = fv_by_name[fv_name]
                source = fv.batch_source
                if not isinstance(source, MongoDBSource):
                    raise ValueError(
                        f"MongoDBOfflineStoreNative: feature view {fv_name!r} has "
                        f"a non-MongoDBSource batch source ({type(source).__name__!r})."
                    )
                db_name = source.database or default_db
                ts_field = source.timestamp_field
                join_keys = [e.name for e in fv.entity_columns]

                arrow_table = _fetch_collection_as_arrow(
                    connection_string, db_name, source.collection
                )
                if arrow_table.num_rows == 0:
                    for f in features:
                        col = f"{fv_name}__{f}" if full_feature_names else f
                        result[col] = None
                    continue

                feature_df = arrow_table.to_pandas()
                # Ensure tz-aware UTC
                if feature_df[ts_field].dt.tz is None:
                    feature_df[ts_field] = pd.to_datetime(
                        feature_df[ts_field], utc=True
                    )
                feature_df = feature_df.sort_values(ts_field)

                col_rename = {
                    f: (f"{fv_name}__{f}" if full_feature_names else f)
                    for f in features
                }
                cols_to_select = join_keys + features + [ts_field]
                feature_df = feature_df[cols_to_select].rename(columns=col_rename)
                out_features = list(col_rename.values())

                merged = pd.merge_asof(
                    result,
                    feature_df,
                    left_on=event_timestamp_col,
                    right_on=ts_field,
                    by=join_keys,
                    direction="backward",
                )
                # Apply TTL: null out features whose timestamp is too far in the past
                if fv.ttl:
                    cutoff = merged[event_timestamp_col] - fv.ttl
                    too_old = merged[ts_field] < cutoff
                    for col in out_features:
                        merged.loc[too_old, col] = None

                result = merged.drop(columns=[ts_field], errors="ignore")

            return pyarrow.Table.from_pandas(result, preserve_index=False)

        return MongoDBNativeRetrievalJob(
            query_fn=_run,
            full_feature_names=full_feature_names,
            on_demand_feature_views=[],
            metadata=None,
            config=config,
        )
