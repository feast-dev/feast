# Copyright 2019 The Feast Authors
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
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.provider import Provider, get_provider
from feast.offline_store import RetrievalJob, get_offline_store_for_retrieval
from feast.registry import Registry
from feast.repo_config import (
    LocalOnlineStoreConfig,
    OnlineStoreConfig,
    RepoConfig,
    load_repo_config,
)


class FeatureStore:
    """
    A FeatureStore object is used to define, create, and retrieve features.
    """

    config: RepoConfig

    def __init__(
        self, repo_path: Optional[str] = None, config: Optional[RepoConfig] = None,
    ):
        if repo_path is not None and config is not None:
            raise ValueError("You cannot specify both repo_path and config")
        if config is not None:
            self.config = config
        elif repo_path is not None:
            self.config = load_repo_config(Path(repo_path))
        else:
            self.config = RepoConfig(
                metadata_store="./metadata.db",
                project="default",
                provider="local",
                online_store=OnlineStoreConfig(
                    local=LocalOnlineStoreConfig("online_store.db")
                ),
            )

    def _get_provider(self) -> Provider:
        return get_provider(self.config)

    def _get_registry(self) -> Registry:
        return Registry(self.config.metadata_store)

    def apply(self, objects: List[Union[FeatureView, Entity]]):
        """Register objects to metadata store and update related infrastructure.

        The apply method registers one or more definitions (e.g., Entity, FeatureView) and registers or updates these
        objects in the Feast registry. Once the registry has been updated, the apply method will update related
        infrastructure (e.g., create tables in an online store) in order to reflect these new definitions. All
        operations are idempotent, meaning they can safely be rerun.

        Args: objects (List[Union[FeatureView, Entity]]): A list of FeatureView or Entity objects that should be
            registered

        Examples:
            Register a single Entity and FeatureView.
            >>> from feast.feature_store import FeatureStore
            >>> from feast import Entity, FeatureView, Feature, ValueType, FileSource
            >>> from datetime import timedelta
            >>>
            >>> fs = FeatureStore()
            >>> customer_entity = Entity(name="customer", value_type=ValueType.INT64, description="customer entity")
            >>> customer_feature_view = FeatureView(
            >>>     name="customer_fv",
            >>>     entities=["customer"],
            >>>     features=[Feature(name="age", dtype=ValueType.INT64)],
            >>>     input=FileSource(path="file.parquet", event_timestamp_column="timestamp"),
            >>>     ttl=timedelta(days=1)
            >>> )
            >>> fs.apply([customer_entity, customer_feature_view])
        """

        # TODO: Add locking
        # TODO: Optimize by only making a single call (read/write)
        # TODO: Add infra update operation (currently we are just writing to registry)
        registry = self._get_registry()
        for ob in objects:
            if isinstance(ob, FeatureView):
                registry.apply_feature_view(ob, project=self.config.project)
            elif isinstance(ob, Entity):
                registry.apply_entity(ob, project=self.config.project)
            else:
                raise ValueError(
                    f"Unknown object type ({type(ob)}) provided as part of apply() call"
                )

    def get_historical_features(
        self, entity_df: Union[pd.DataFrame, str], feature_refs: List[str],
    ) -> RetrievalJob:
        """Enrich an entity dataframe with historical feature values for either training or batch scoring.

        This method joins historical feature data from one or more feature views to an entity dataframe by using a time
        travel join.

        Each feature view is joined to the entity dataframe using all entities configured for the respective feature
        view. All configured entities must be available in the entity dataframe. Therefore, the entity dataframe must
        contain all entities found in all feature views, but the individual feature views can have different entities.

        Time travel is based on the configured TTL for each feature view. A shorter TTL will limit the
        amount of scanning that will be done in order to find feature data for a specific entity key. Setting a short
        TTL may result in null values being returned.

        Args:
            entity_df (Union[pd.DataFrame, str]): An entity dataframe is a collection of rows containing all entity
                columns (e.g., customer_id, driver_id) on which features need to be joined, as well as a event_timestamp
                column used to ensure point-in-time correctness. Either a Pandas DataFrame can be provided or a string
                SQL query. The query must be of a format supported by the configured offline store (e.g., BigQuery)
            feature_refs: A list of features that should be retrieved from the offline store. Feature references are of
                the format "feature_view:feature", e.g., "customer_fv:daily_transactions".

        Returns:
            RetrievalJob which can be used to materialize the results.

        Examples:
            Retrieve historical features using a BigQuery SQL entity dataframe
            >>> from feast.feature_store import FeatureStore
            >>>
            >>> fs = FeatureStore(config=RepoConfig(provider="gcp"))
            >>> retrieval_job = fs.get_historical_features(
            >>>     entity_df="SELECT event_timestamp, order_id, customer_id from gcp_project.my_ds.customer_orders",
            >>>     feature_refs=["customer:age", "customer:avg_orders_1d", "customer:avg_orders_7d"]
            >>> )
            >>> feature_data = job.to_df()
            >>> model.fit(feature_data) # insert your modeling framework here.
        """

        registry = self._get_registry()
        all_feature_views = registry.list_feature_views(project=self.config.project)
        feature_views = _get_requested_feature_views(feature_refs, all_feature_views)
        offline_store = get_offline_store_for_retrieval(feature_views)
        job = offline_store.get_historical_features(
            feature_views, feature_refs, entity_df
        )
        return job


def _get_requested_feature_views(
    feature_refs: List[str], all_feature_views: List[FeatureView]
) -> List[FeatureView]:
    """Get list of feature views based on feature references"""

    feature_views_dict = {}
    for ref in feature_refs:
        ref_parts = ref.split(":")
        found = False
        for feature_view in all_feature_views:
            if feature_view.name == ref_parts[0]:
                found = True
                feature_views_dict[feature_view.name] = feature_view
                continue

        if not found:
            raise ValueError(f"Could not find feature view from reference {ref}")
    feature_views_list = []
    for view in feature_views_dict.values():
        feature_views_list.append(view)

    return feature_views_list
