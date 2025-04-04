from dataclasses import dataclass, field
from typing import Dict, List, Union

import pandas as pd

from feast.entity import Entity
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.repo_config import RepoConfig


@dataclass
class ExecutionContext:
    project: str
    repo_config: RepoConfig
    offline_store: OfflineStore
    online_store: OnlineStore
    entity_defs: List[Entity]
    entity_df: Union[pd.DataFrame, None] = None
    node_outputs: Dict[str, DAGValue] = field(default_factory=dict)
