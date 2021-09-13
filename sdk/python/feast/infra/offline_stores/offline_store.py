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
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
import pyarrow

from feast.data_source import DataSource
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.registry import Registry
from feast.repo_config import RepoConfig


class RetrievalJob(ABC):
    """RetrievalJob is used to manage the execution of a historical feature retrieval"""

    @property
    @abstractmethod
    def full_feature_names(self) -> bool:
        pass

    @property
    @abstractmethod
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        pass

    def to_df(self) -> pd.DataFrame:
        """Return dataset as Pandas DataFrame synchronously including on demand transforms"""
        features_df = self._to_df_internal()
        if self.on_demand_feature_views is None:
            return features_df

        for odfv in self.on_demand_feature_views:
            features_df = features_df.join(
                odfv.get_transformed_features_df(self.full_feature_names, features_df)
            )
        return features_df

    @abstractmethod
    def _to_df_internal(self) -> pd.DataFrame:
        """Return dataset as Pandas DataFrame synchronously"""
        pass

    @abstractmethod
    def _to_arrow_internal(self) -> pyarrow.Table:
        """Return dataset as pyarrow Table synchronously"""
        pass

    def to_arrow(self) -> pyarrow.Table:
        """Return dataset as pyarrow Table synchronously"""
        if self.on_demand_feature_views is None:
            return self._to_arrow_internal()

        features_df = self._to_df_internal()
        for odfv in self.on_demand_feature_views:
            features_df = features_df.join(
                odfv.get_transformed_features_df(self.full_feature_names, features_df)
            )
        return pyarrow.Table.from_pandas(features_df)


class OfflineStore(ABC):
    """
    OfflineStore is an object used for all interaction between Feast and the service used for offline storage of
    features.
    """

    @staticmethod
    @abstractmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        """
        Note that join_key_columns, feature_name_columns, event_timestamp_column, and created_timestamp_column
        have all already been mapped to column names of the source table and those column names are the values passed
        into this function.
        """
        pass

    @staticmethod
    @abstractmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        pass
