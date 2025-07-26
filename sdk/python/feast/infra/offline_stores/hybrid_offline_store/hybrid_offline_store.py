"""
HybridOfflineStore implementation that routes operations to different offline stores
based on data source type.
"""

from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import pandas as pd
import pyarrow
from pydantic import Field, StrictStr

from feast import FeatureView
from feast.data_source import DataSource
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import (
    FeastConfigBaseModel,
    RepoConfig,
    get_offline_config_from_type,
)


class OfflineStoreConfig(FeastConfigBaseModel):
    """Configuration for a single offline store backend."""

    type: StrictStr = Field(
        description="Type of offline store (e.g., 'bigquery', 'spark')"
    )
    conf: Dict[str, Any] = Field(
        default_factory=dict, description="Store-specific configuration"
    )


class HybridOfflineStoreConfig(FeastConfigBaseModel):
    """Configuration for the HybridOfflineStore."""

    type: Literal["HybridOfflineStore", "hybrid_offline_store.HybridOfflineStore"] = (
        "hybrid_offline_store.HybridOfflineStore"
    )
    offline_stores: List[OfflineStoreConfig] = Field(
        default_factory=list, description="List of offline store configurations"
    )


class HybridOfflineStore(OfflineStore):
    """
    Routes operations to different offline stores based on data source type.

    This store acts as a router that delegates operations to the appropriate
    underlying offline store based on the type of data source being accessed.
    """

    @staticmethod
    def _prepare_store_config(
        base_config: RepoConfig, store_config: OfflineStoreConfig
    ) -> Dict[str, Any]:
        """
        Prepare store configuration dictionary from base config and store-specific config.

        Args:
            base_config: Base repository configuration containing common settings
            store_config: Store-specific configuration for the target offline store

        Returns:
            Dictionary containing the combined configuration ready for offline store
        """
        config_dict = base_config.__dict__.copy()
        config_dict["registry"] = config_dict["registry_config"]
        config_dict["offline_store"] = store_config.conf.copy()
        config_dict["offline_store"]["type"] = store_config.type
        config_dict["online_store"] = config_dict["online_config"]
        return config_dict

    @staticmethod
    def _get_store_and_config_by_source(
        repo_config: RepoConfig, data_source: DataSource
    ) -> Tuple[OfflineStore, RepoConfig]:
        """
        Get the appropriate offline store and configuration for a data source.

        Args:
            repo_config: Repository configuration
            data_source: Data source to match with an offline store

        Returns:
            Tuple of (offline store instance, store configuration)

        Raises:
            ValueError: If no matching offline store configuration is found or if
                       the data source type is not valid
        """
        # Get source type information
        source_type = type(data_source).__name__
        source_store_type = source_type.lower().replace("source", "")

        # Find matching store configuration
        store_config = next(
            (
                config
                for config in repo_config.offline_store.offline_stores
                if config.type.lower() == source_store_type
            ),
            None,
        )

        if not store_config:
            available_stores = [
                store.type for store in repo_config.offline_store.offline_stores
            ]
            raise ValueError(
                f"No offline store configuration found for source type '{source_type}'. "
                f"Available store types: {available_stores}"
            )

        # Create offline store instance and configuration
        offline_config = get_offline_config_from_type(source_store_type)(
            **store_config.conf
        )
        offline_store = get_offline_store_from_config(offline_config)

        # Create a new RepoConfig with the appropriate store configuration
        store_repo_config = RepoConfig(
            **HybridOfflineStore._prepare_store_config(repo_config, store_config)
        )

        return offline_store, store_repo_config

    @staticmethod
    def _validate_feature_views_source_type(feature_views: List[FeatureView]) -> None:
        """
        Validate that all feature views have the same source type.

        This check ensures that features can be retrieved from a single offline store.

        Args:
            feature_views: List of feature views to validate

        Raises:
            ValueError: If feature views have different source types
        """
        if len(feature_views) > 1:
            source_types = {type(fv.batch_source).__name__ for fv in feature_views}
            if len(source_types) > 1:
                raise ValueError(
                    f"All feature views must have the same source type for hybrid offline store. "
                    f"Found multiple source types: {', '.join(sorted(source_types))}"
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
        """Route pull operation to appropriate offline store."""
        store, store_config = HybridOfflineStore._get_store_and_config_by_source(
            config, data_source
        )
        return store.pull_latest_from_table_or_query(
            config=store_config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
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
        """Get historical features from appropriate offline store."""
        # Ensure all feature views use the same source type
        HybridOfflineStore._validate_feature_views_source_type(feature_views)

        # Get store for the first feature view's source
        store, store_config = HybridOfflineStore._get_store_and_config_by_source(
            config, feature_views[0].batch_source
        )

        # Delegate to the appropriate store
        return store.get_historical_features(
            config=store_config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
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
        """Pull all data from appropriate offline store."""
        store, store_config = HybridOfflineStore._get_store_and_config_by_source(
            config, data_source
        )
        return store.pull_all_from_table_or_query(
            config=store_config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """Write batch data to appropriate offline store."""
        store, store_config = HybridOfflineStore._get_store_and_config_by_source(
            config, feature_view.batch_source
        )
        store.offline_write_batch(
            config=store_config,
            feature_view=feature_view,
            table=table,
            progress=progress,
        )
