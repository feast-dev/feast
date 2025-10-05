from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

import pandas as pd

from feast import BatchFeatureView, StreamFeatureView
from feast.infra.registry.registry import Registry


@dataclass
class HistoricalRetrievalTask:
    project: str
    entity_df: Union[pd.DataFrame, str]
    feature_view: Union[BatchFeatureView, StreamFeatureView]
    full_feature_name: bool
    registry: Registry
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
