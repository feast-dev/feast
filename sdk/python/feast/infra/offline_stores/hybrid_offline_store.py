from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import pyarrow

from feast import FeatureView, RepoConfig
from feast.data_source import _DATA_SOURCE_FOR_OFFLINE_STORE, DataSource
from feast.errors import FeastOfflineStoreInvalidName
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config
from feast.infra.registry.base_registry import BaseRegistry
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import (
    FeastConfigBaseModel,
    get_offline_config_from_type,
    get_offline_store_type,
)


class HybridOfflineStoreConfig(FeastConfigBaseModel):
    type: str = "hybrid_offline_store.HybridOfflineStore"

    class OfflineStoresWithConfig(FeastConfigBaseModel):
        type: str
        conf: Dict[str, Any]

    offline_stores: Optional[List[OfflineStoresWithConfig]]


class HybridOfflineStore(OfflineStore):
    _instance: Optional["HybridOfflineStore"] = None
    _initialized: bool
    offline_stores: Dict[str, OfflineStore]

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(HybridOfflineStore, cls).__new__(cls)
            cls._instance._initialized = False
            cls._instance.offline_stores = {}
        return cls._instance

    def _initialize_offline_stores(self, config: RepoConfig):
        if self._initialized:
            return
        for store_cfg in getattr(config.offline_store, "offline_stores", []):
            try:
                offline_store_type = get_offline_store_type(store_cfg.type)
                config_cls = get_offline_config_from_type(store_cfg.type)
                config_instance = config_cls(**store_cfg.conf)
                store = get_offline_store_from_config(config_instance)
                self.offline_stores[offline_store_type] = store
            except FeastOfflineStoreInvalidName as e:
                raise FeastOfflineStoreInvalidName(
                    f"Failed to initialize Hybrid offline store {store_cfg.type}: {e}"
                )
        self._initialized = True

    def get_source_key_from_type(
        self, source_type: DataSourceProto.SourceType.ValueType
    ) -> Optional[str]:
        if source_type not in list(_DATA_SOURCE_FOR_OFFLINE_STORE.keys()):
            raise ValueError(
                f"Unsupported DataSource type for HybridOfflineStore: {source_type}."
                f"Supported types are: {list(_DATA_SOURCE_FOR_OFFLINE_STORE.keys())}"
            )
        return _DATA_SOURCE_FOR_OFFLINE_STORE.get(source_type, None)

    def _get_offline_store_for_feature_view(
        self, feature_view: FeatureView, config: RepoConfig
    ) -> OfflineStore:
        self._initialize_offline_stores(config)
        source_type = feature_view.batch_source.source_type()
        store_key = self.get_source_key_from_type(source_type)
        if store_key is None:
            raise ValueError(
                f"Unsupported FeatureView batch_source type: {source_type}"
            )
        return self.offline_stores[store_key]

    def _get_offline_store_for_source(
        self, data_source: DataSource, config: RepoConfig
    ) -> OfflineStore:
        self._initialize_offline_stores(config)
        source_type = data_source.source_type()
        store_key = self.get_source_key_from_type(source_type)
        if store_key is None:
            raise ValueError(f"Unsupported DataSource type: {source_type}")
        return self.offline_stores[store_key]

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
        # TODO: Multiple data sources can be supported when feature store use compute engine
        # for getting historical features
        data_source = None
        for feature_view in feature_views:
            if not feature_view.batch_source:
                raise ValueError(
                    "HybridOfflineStore only supports feature views with DataSource as source. "
                )
            if not data_source:
                data_source = feature_view.batch_source
            elif data_source != feature_view.batch_source:
                raise ValueError(
                    "All feature views must have the same batch source for HybridOfflineStore."
                )

        store = HybridOfflineStore()._get_offline_store_for_feature_view(
            feature_views[0], config
        )
        return store.get_historical_features(
            config,
            feature_views,
            feature_refs,
            entity_df,
            registry,
            project,
            full_feature_names,
        )

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
        store = HybridOfflineStore()._get_offline_store_for_source(data_source, config)
        return store.pull_latest_from_table_or_query(
            config,
            data_source,
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
            start_date,
            end_date,
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
        store = HybridOfflineStore()._get_offline_store_for_source(data_source, config)
        return store.pull_all_from_table_or_query(
            config,
            data_source,
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
            start_date,
            end_date,
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        raise NotImplementedError(
            "HybridOfflineStore does not support write_logged_features. "
            "Please use the specific offline store for logging."
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        store = HybridOfflineStore()._get_offline_store_for_feature_view(
            feature_view, config
        )
        return store.offline_write_batch(config, feature_view, table, progress)

    def validate_data_source(
        self,
        config: RepoConfig,
        data_source: DataSource,
    ):
        store = self._get_offline_store_for_source(data_source, config)
        return store.validate_data_source(config, data_source)

    def get_table_column_names_and_types_from_data_source(
        self,
        config: RepoConfig,
        data_source: DataSource,
    ) -> Iterable[Tuple[str, str]]:
        store = self._get_offline_store_for_source(data_source, config)
        return store.get_table_column_names_and_types_from_data_source(
            config, data_source
        )
