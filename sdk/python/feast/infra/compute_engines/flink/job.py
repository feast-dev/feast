from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
import pyarrow as pa

from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
)
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.flink.utils import (
    cleanup_flink_temporary_views,
    flink_table_to_arrow,
)
from feast.infra.offline_stores.offline_store import RetrievalJob, RetrievalMetadata
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.saved_dataset import SavedDatasetStorage


class FlinkDAGRetrievalJob(RetrievalJob):
    def __init__(
        self,
        plan: Optional[ExecutionPlan],
        context: ExecutionContext,
        table_env: object,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
        error: Optional[BaseException] = None,
    ) -> None:
        self._plan = plan
        self._context = context
        self._table_env = table_env
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        self._error = error
        self._arrow_table: Optional[pa.Table] = None

    def error(self) -> Optional[BaseException]:
        return self._error

    def _ensure_executed(self) -> None:
        if self._arrow_table is None:
            if self._error is not None:
                raise self._error
            if self._plan is None:
                raise RuntimeError("Execution plan is not set")
            try:
                result = self._plan.execute(self._context)
                self._arrow_table = flink_table_to_arrow(result.data)
            finally:
                cleanup_flink_temporary_views(self._table_env)

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        self._ensure_executed()
        assert self._arrow_table is not None
        return self._arrow_table.to_pandas()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        self._ensure_executed()
        assert self._arrow_table is not None
        return self._arrow_table

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ) -> None:
        raise NotImplementedError("Persisting Flink retrieval jobs is not supported.")

    def to_remote_storage(self) -> List[str]:
        raise NotImplementedError(
            "Remote storage is not supported in FlinkDAGRetrievalJob."
        )

    def to_sql(self) -> str:
        raise NotImplementedError("SQL generation is not supported for Flink DAGs.")


@dataclass
class FlinkMaterializationJob(MaterializationJob):
    def __init__(
        self,
        job_id: str,
        status: MaterializationJobStatus,
        error: Optional[BaseException] = None,
    ) -> None:
        super().__init__()
        self._job_id = job_id
        self._status = status
        self._error = error

    def status(self) -> MaterializationJobStatus:
        return self._status

    def error(self) -> Optional[BaseException]:
        return self._error

    def should_be_retried(self) -> bool:
        return False

    def job_id(self) -> str:
        return self._job_id

    def url(self) -> Optional[str]:
        return None
