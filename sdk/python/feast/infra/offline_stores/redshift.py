from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
from pydantic import StrictStr
from pydantic.typing import Literal

from feast.data_source import DataSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig


class RedshiftOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for AWS Redshift """

    type: Literal["redshift"] = "redshift"
    """ Offline store type selector"""

    cluster_id: StrictStr
    """ Redshift cluster identifier """

    region: StrictStr
    """ Redshift cluster's AWS region """

    user: StrictStr
    """ Redshift user name """

    database: StrictStr
    """ Redshift database name """

    s3_path: StrictStr
    """ S3 path for importing & exporting data to Redshift """


class RedshiftOfflineStore(OfflineStore):
    @staticmethod
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
        pass

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = True,
    ) -> RetrievalJob:
        pass
