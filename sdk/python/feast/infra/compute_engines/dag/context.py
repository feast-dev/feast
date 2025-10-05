from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

import pandas as pd

from feast.entity import Entity
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.repo_config import RepoConfig


@dataclass
class ColumnInfo:
    join_keys: List[str]
    feature_cols: List[str]
    ts_col: str
    created_ts_col: Optional[str]
    field_mapping: Optional[Dict[str, str]] = None

    def __iter__(self):
        yield self.join_keys
        yield self.feature_cols
        yield self.ts_col
        yield self.created_ts_col
        yield self.field_mapping

    @property
    def timestamp_column(self) -> str:
        """
        Get the event timestamp column from the context.
        """
        mapped_column = self._get_mapped_column(self.ts_col)
        if mapped_column is None:
            raise ValueError("Timestamp column cannot be None")
        return mapped_column

    @property
    def created_timestamp_column(self) -> Optional[str]:
        """
        Get the created timestamp column from the context.
        """
        return self._get_mapped_column(self.created_ts_col)

    def _get_mapped_column(self, column: Optional[str]) -> Optional[str]:
        """
        Helper method to get the mapped column name if it exists in field_mapping.
        """
        if column and self.field_mapping:
            return self.field_mapping.get(column, column)
        return column


@dataclass
class ExecutionContext:
    """
    ExecutionContext holds all runtime information required to execute a DAG plan
    within a ComputeEngine. It is passed into each DAGNode during execution and
    contains shared context such as configuration, registry-backed entities, runtime
    data (e.g. entity_df), and DAG evaluation state.

    Attributes:
        project: Feast project name (namespace for features, entities, views).

        repo_config: Resolved RepoConfig containing provider and store configuration.

        offline_store: Reference to the configured OfflineStore implementation.
            Used for loading raw feature data during materialization or retrieval.

        online_store: Reference to the OnlineStore implementation.
            Used during materialization to write online features.

        entity_defs: List of Entity definitions fetched from the registry.
            Used for resolving join keys, inferring timestamp columns, and
            validating FeatureViews against schema.

        entity_df: A runtime DataFrame of entity rows used during historical
            retrieval (e.g. for point-in-time join). Includes entity keys and
            event timestamps. This is not part of the registry and is user-supplied
            for training dataset generation.

        node_outputs: Internal cache of DAGValue outputs keyed by DAGNode name.
            Automatically populated during ExecutionPlan execution to avoid redundant
            computation. Used by downstream nodes to access their input data.
    """

    project: str
    repo_config: RepoConfig
    offline_store: OfflineStore
    online_store: OnlineStore
    entity_defs: List[Entity]
    entity_df: Union[pd.DataFrame, None] = None
    node_outputs: Dict[str, DAGValue] = field(default_factory=dict)
