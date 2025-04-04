from enum import Enum
from dataclasses import dataclass, field
import pandas as pd
from typing import Union, List, Dict

from feast.entity import Entity
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.repo_config import RepoConfig
from feast.infra.compute_engines.dag.value import DAGValue


class DAGFormat(str, Enum):
    SPARK = "spark"
    PANDAS = "pandas"
    ARROW = "arrow"


@dataclass
class ExecutionContext:
    project: str
    repo_config: RepoConfig
    offline_store: OfflineStore
    online_store: OnlineStore
    entity_defs: List[Entity]
    entity_df: Union[pd.DataFrame, None] = None
    node_outputs: Dict[str, DAGValue] = field(default_factory=dict)
