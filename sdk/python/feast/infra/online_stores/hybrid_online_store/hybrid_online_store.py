"""
hybrid_online_store.py
----------------------

This module provides the HybridOnlineStore, a Feast OnlineStore implementation that enables routing online feature operations
to different online stores based on a configurable tag (e.g., tribe, team, or project) on the FeatureView. This allows a single Feast deployment
to support multiple online store backends, each configured independently and selected dynamically at runtime.

Features:
    - Supports multiple online store backends in a single Feast deployment.
    - Routes online reads and writes to the correct backend based on a configurable tag on the FeatureView.
    - Enables multi-tenancy and flexible data management strategies.
    - Designed for extensibility and compatibility with Feast's OnlineStore interface.

Usage:
    1. Add a tag (e.g., 'tribe', 'team', or any custom name) to your FeatureView.
    2. Configure multiple online stores in your Feast repo config under 'online_stores'.
    3. Set the 'routing_tag' field in your online_store config to specify which tag to use for routing.
    4. The HybridOnlineStore will route reads and writes to the correct backend based on the tag value.

Example configuration (feature_store.yaml):

    online_store:
      type: hybrid_online_store.HybridOnlineStore
      routing_tag: team  # or any tag name you want to use for routing
      online_stores:
        - type: feast.infra.online_stores.bigtable.BigtableOnlineStore
          conf:
            ... # bigtable config
        - type: feast.infra.online_stores.contrib.cassandra_online_store.cassandra_online_store.CassandraOnlineStore
          conf:
            ... # cassandra config

Example FeatureView:

    tags:
      team: bigtable

The HybridOnlineStore will route requests to the correct online store based on the value of the tag specified by 'routing_tag'.
"""

from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from pydantic import StrictStr

from feast import Entity, FeatureView, RepoConfig
from feast.infra.online_stores.helpers import get_online_store_from_config
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, get_online_config_from_type


class HybridOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for HybridOnlineStore.

    This config allows multiple online stores to be used in a single Feast deployment. Each online store is specified by its type (Python import path)
    and a configuration dictionary. The HybridOnlineStore uses this configuration to instantiate and manage the set of online stores.

    Attributes:
        type: The type identifier for the HybridOnlineStore.
        online_stores: A list of OnlineStoresWithConfig, each specifying the type and config for an online store backend.
    """

    type: Literal["HybridOnlineStore", "hybrid_online_store.HybridOnlineStore"] = (
        "hybrid_online_store.HybridOnlineStore"
    )

    class OnlineStoresWithConfig(FeastConfigBaseModel):
        """
        Configuration for a single online store backend.

        Attributes:
            type: Python import path to the online store class.
            conf: Dictionary of configuration parameters for the online store.
        """

        type: StrictStr  # Python import path to the online store class
        conf: Dict

    online_stores: Optional[List[OnlineStoresWithConfig]]
    routing_tag: StrictStr = (
        "tribe"  # Configurable tag name for routing, default is 'tribe'
    )


class HybridOnlineStore(OnlineStore):
    """
    HybridOnlineStore routes online feature operations to different online store backends
    based on a tag (e.g., 'tribe') on the FeatureView. This enables multi-tenancy and flexible
    backend selection in a single Feast deployment.

    The backend is selected dynamically at runtime according to the tag value.
    """

    def __init__(self):
        """
        Initialize the HybridOnlineStore. Online stores are instantiated lazily on first use.
        """
        self.online_stores = {}
        self._initialized = False

    def _initialize_online_stores(self, config: RepoConfig):
        """
        Lazily instantiate all configured online store backends from the repo config.

        Args:
            config: Feast RepoConfig containing the online_stores configuration.
        """
        if self._initialized:
            return
        self.online_stores = {}
        online_stores_cfg = getattr(config.online_store, "online_stores", [])
        for store_cfg in online_stores_cfg:
            config_cls = get_online_config_from_type(
                store_cfg.type.split(".")[-1].lower()
            )
            config_instance = config_cls(**store_cfg.conf)
            online_store_instance = get_online_store_from_config(config_instance)
            self.online_stores[store_cfg.type.split(".")[-1].lower()] = (
                online_store_instance
            )
        self._initialized = True

    def _get_online_store(self, tribe_tag, config: RepoConfig):
        """
        Retrieve the online store backend corresponding to the given tag value.

        Args:
            tribe_tag: The tag value (e.g., 'tribe') used to select the backend.
            config: Feast RepoConfig.
        Returns:
            The OnlineStore instance for the given tag, or None if not found.
        """
        self._initialize_online_stores(config)
        return self.online_stores.get(tribe_tag.lower())

    def _prepare_repo_conf(self, config: RepoConfig, online_store_type: str):
        """
        Prepare a RepoConfig for the selected online store backend.

        Args:
            config: The original Feast RepoConfig.
            online_store_type: The type of the online store backend to use.
        Returns:
            A dictionary representing the updated RepoConfig for the selected backend.
        """
        rconfig = config
        for online_store in config.online_store.online_stores:
            if online_store.type.split(".")[-1].lower() == online_store_type.lower():
                rconfig.online_config = online_store.conf
                rconfig.online_config["type"] = online_store.type
        data = rconfig.__dict__
        data["registry"] = data["registry_config"]
        data["offline_store"] = data["offline_config"]
        data["online_store"] = data["online_config"]
        return data

    def _get_routing_tag_value(self, table: FeatureView, config: RepoConfig):
        tag_name = getattr(config.online_store, "routing_tag", "tribe")
        return table.tags.get(tag_name)

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        odata: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Write a batch of feature rows to the appropriate online store based on the FeatureView's tag.

        Args:
            config: Feast RepoConfig.
            table: FeatureView to write to. Must have a tag (e.g., 'tribe') to select the backend.
            odata: List of tuples containing entity key, feature values, event timestamp, and created timestamp.
            progress: Optional callback for progress reporting.
        Raises:
            ValueError: If the FeatureView does not have the required tag.
            NotImplementedError: If no online store is found for the tag value.
        """
        tribe = self._get_routing_tag_value(table, config)
        if not tribe:
            tag_name = getattr(config.online_store, "routing_tag", "tribe")
            raise ValueError(
                f"FeatureView must have a '{tag_name}' tag to use HybridOnlineStore."
            )
        online_store = self._get_online_store(tribe, config)
        if online_store:
            config = RepoConfig(**self._prepare_repo_conf(config, tribe))
            online_store.online_write_batch(config, table, odata, progress)
        else:
            raise NotImplementedError(
                f"No online store found for {getattr(config.online_store, 'routing_tag', 'tribe')} tag '{tribe}'. Please check your configuration."
            )

    @staticmethod
    def write_to_table(
        created_ts, cur, entity_key_bin, feature_name, project, table, timestamp, val
    ):
        """
        (Not implemented) Write a single feature value to the online store table.
        """
        pass

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Read feature rows from the appropriate online store based on the FeatureView's tag.

        Args:
            config: Feast RepoConfig.
            table: FeatureView to read from. Must have a tag (e.g., 'tribe') to select the backend.
            entity_keys: List of entity keys to read.
            requested_features: Optional list of feature names to read.
        Returns:
            List of tuples containing event timestamp and feature values.
        Raises:
            ValueError: If the FeatureView does not have the required tag.
            NotImplementedError: If no online store is found for the tag value.
        """
        tribe = self._get_routing_tag_value(table, config)
        if not tribe:
            tag_name = getattr(config.online_store, "routing_tag", "tribe")
            raise ValueError(
                f"FeatureView must have a '{tag_name}' tag to use HybridOnlineStore."
            )
        online_store = self._get_online_store(tribe, config)
        if online_store:
            config = RepoConfig(**self._prepare_repo_conf(config, tribe))
            return online_store.online_read(
                config, table, entity_keys, requested_features
            )
        else:
            raise NotImplementedError(
                f"No online store found for {getattr(config.online_store, 'routing_tag', 'tribe')} tag '{tribe}'. Please check your configuration."
            )

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Update the state of the online stores for the given FeatureViews and Entities.

        Args:
            config: Feast RepoConfig.
            tables_to_delete: Sequence of FeatureViews to delete.
            tables_to_keep: Sequence of FeatureViews to keep.
            entities_to_delete: Sequence of Entities to delete.
            entities_to_keep: Sequence of Entities to keep.
            partial: Whether to perform a partial update.
        Raises:
            ValueError: If a FeatureView does not have the required tag.
            NotImplementedError: If no online store is found for a tag value.
        """
        for table in tables_to_keep:
            tribe = self._get_routing_tag_value(table, config)
            if not tribe:
                tag_name = getattr(config.online_store, "routing_tag", "tribe")
                raise ValueError(
                    f"FeatureView must have a '{tag_name}' tag to use HybridOnlineStore."
                )
            online_store = self._get_online_store(tribe, config)
            if online_store:
                config = RepoConfig(**self._prepare_repo_conf(config, tribe))
                online_store.update(
                    config,
                    tables_to_delete,
                    tables_to_keep,
                    entities_to_delete,
                    entities_to_keep,
                    partial,
                )
            else:
                raise NotImplementedError(
                    f"No online store found for {getattr(config.online_store, 'routing_tag', 'tribe')} tag '{tribe}'. Please check your configuration."
                )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Teardown all managed online stores for the given FeatureViews and Entities.

        Args:
            config: Feast RepoConfig.
            tables: Sequence of FeatureViews to teardown.
            entities: Sequence of Entities to teardown.
        """
        # Use a set of (tribe, store_type, conf_id) to avoid duplicate teardowns for the same instance
        tribes_seen = set()
        online_stores_cfg = getattr(config.online_store, "online_stores", [])
        tag_name = getattr(config.online_store, "routing_tag", "tribe")
        for table in tables:
            tribe = table.tags.get(tag_name)
            if not tribe:
                continue
            # Find all store configs matching this tribe (supporting multiple instances of the same type)
            for store_cfg in online_stores_cfg:
                store_type = store_cfg.type
                # Use id(store_cfg.conf) to distinguish different configs of the same type
                key = (tribe, store_type, id(store_cfg.conf))
                if key in tribes_seen:
                    continue
                tribes_seen.add(key)
                # Only select the online store if tribe matches the type (or you can add a mapping in config for more flexibility)
                if tribe.lower() == store_type.split(".")[-1].lower():
                    online_store = self._get_online_store(tribe, config)
                    if online_store:
                        config = RepoConfig(**self._prepare_repo_conf(config, tribe))
                        online_store.teardown(config, tables, entities)
