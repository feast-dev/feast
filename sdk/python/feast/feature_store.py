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
from datetime import datetime
from pathlib import Path
from typing import Optional

import pyarrow

from feast.feature_view import FeatureView
from feast.infra.provider import Provider, get_provider
from feast.offline_store import BigQueryOfflineStore
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

    def _pull_table(
        self, feature_view: FeatureView, start_date: datetime, end_date: datetime
    ) -> Optional[pyarrow.Table]:
        if feature_view.inputs.table_ref is None:
            raise NotImplementedError(
                "Ingestion is not yet implemented for query-based sources."
            )

        # if we have mapped fields, use the original field names in the call to the offline store
        event_timestamp_column = feature_view.inputs.event_timestamp_column
        fields = (
            [entity.name for entity in feature_view.entities]
            + [feature.name for feature in feature_view.features]
            + [feature_view.inputs.event_timestamp_column]
        )
        if feature_view.inputs.created_timestamp_column is not None:
            fields.append(feature_view.inputs.created_timestamp_column)
        if feature_view.inputs.field_mapping is not None:
            reverse_field_mapping = {
                v: k for k, v in feature_view.inputs.field_mapping.items()
            }
            event_timestamp_column = (
                reverse_field_mapping[event_timestamp_column]
                if event_timestamp_column in reverse_field_mapping.keys()
                else event_timestamp_column
            )
            fields = [
                reverse_field_mapping[col]
                if col in reverse_field_mapping.keys()
                else col
                for col in fields
            ]

        table = BigQueryOfflineStore.pull_table(
            feature_view.inputs.table_ref,
            fields,
            event_timestamp_column,
            start_date,
            end_date,
        )

        # run feature mapping in the forward direction
        if table is not None and feature_view.inputs.field_mapping is not None:
            cols = table.column_names
            mapped_cols = [
                feature_view.inputs.field_mapping[col]
                if col in feature_view.inputs.field_mapping.keys()
                else col
                for col in cols
            ]
            table = table.rename_columns(mapped_cols)
        return table
