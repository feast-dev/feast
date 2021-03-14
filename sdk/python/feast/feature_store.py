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
from feast.registry import Registry, RegistryState
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

    def apply(self, objects: Union[List[Union[FeatureView, Entity]]]):
        # TODO: Add locking and optimize by only writing once (not multiple times during each apply step)
        # TODO: Add infra update operation (currently we are just writing to registry)
        # TODO: Add docstring
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
        # TODO: Add docstring
        registry = self._get_registry()
        registry_state = registry.get_registry_state(project=self.config.project)
        feature_views = _get_requested_feature_views(feature_refs, registry_state)
        offline_store = get_offline_store_for_retrieval(feature_views)
        job = offline_store.get_historical_features(
            feature_views, feature_refs, entity_df
        )
        return job


def _get_requested_feature_views(
    feature_refs: List[str], registry_state: RegistryState
) -> List[FeatureView]:
    """Get list of feature views based on feature references"""

    feature_views_dict = {}
    for ref in feature_refs:
        ref_parts = ref.split(":")
        found = False
        for feature_view in registry_state.feature_views:
            if feature_view.name == ref_parts[0]:
                found = True
                feature_views_dict[feature_view.name] = feature_view

        if not found:
            raise ValueError(f"Could not find feature view from reference {ref}")
    feature_views_list = []
    for view in feature_views_dict.values():
        feature_views_list.append(view)

    return feature_views_list
