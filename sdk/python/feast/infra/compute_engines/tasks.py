from dataclasses import dataclass
from datetime import datetime
from typing import Union

import pandas as pd

from feast import BatchFeatureView, RepoConfig, StreamFeatureView
from feast.infra.registry.registry import Registry


@dataclass
class HistoricalRetrievalTask:
    entity_df: Union[pd.DataFrame, str]
    feature_view: Union[BatchFeatureView, StreamFeatureView]
    full_feature_name: bool
    registry: Registry
    config: RepoConfig
    start_time: datetime
    end_time: datetime
