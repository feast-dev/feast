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

import warnings
from datetime import datetime
from typing import Any, Callable, List, Optional, Union

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
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig

# Print RuntimeWarning only once per process.
warnings.simplefilter("once", RuntimeWarning)


class MongoDBOfflineStoreConfig(FeastConfigBaseModel):
    """Configuration for the MongoDB offline store."""

    type: StrictStr = "feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb.MongoDBOfflineStore"
    """Offline store type selector"""

    connection_string: StrictStr = "mongodb://localhost:27017"
    """MongoDB connection URI"""

    database: StrictStr = "feast"
    """Default MongoDB database name"""


class MongoDBOfflineStore(OfflineStore):
    """Offline store backed by MongoDB, using ibis for point-in-time joins."""

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
        assert isinstance(data_source, MongoDBSource)
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
        assert isinstance(data_source, MongoDBSource)
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
        assert isinstance(data_source, MongoDBSource)
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
        assert isinstance(data_source, MongoDBSource)
        connection_string = config.offline_store.connection_string
        db_name = data_source.database or config.offline_store.database
        location = f"{db_name}.{data_source.collection}"
        client: Any = MongoClient(connection_string)
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
