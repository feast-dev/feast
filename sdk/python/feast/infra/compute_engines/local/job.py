from dataclasses import dataclass
from typing import List, Optional, cast

import pandas as pd
import pyarrow

from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
)
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.local.arrow_table_value import ArrowTableValue
from feast.infra.offline_stores.offline_store import RetrievalJob, RetrievalMetadata
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.saved_dataset import SavedDatasetStorage


class LocalRetrievalJob(RetrievalJob):
    def __init__(
        self,
        plan: Optional[ExecutionPlan],
        context: ExecutionContext,
        full_feature_names: bool = True,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
        error: Optional[BaseException] = None,
    ):
        self._plan = plan
        self._context = context
        self._arrow_table = None
        self._error = error
        self._metadata = metadata
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []

    def error(self) -> Optional[BaseException]:
        return self._error

    def _ensure_executed(self):
        if self._arrow_table is None:
            result = cast(ArrowTableValue, self._plan.execute(self._context))
            self._arrow_table = result.data

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        self._ensure_executed()
        assert self._arrow_table is not None
        return self._arrow_table.to_pandas()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        self._ensure_executed()
        return self._arrow_table

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ):
        pass

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def to_remote_storage(self) -> List[str]:
        raise NotImplementedError(
            "Remote storage is not supported in LocalRetrievalJob"
        )

    def to_sql(self) -> str:
        raise NotImplementedError(
            "SQL generation is not supported in LocalRetrievalJob"
        )


@dataclass
class LocalMaterializationJob(MaterializationJob):
    def __init__(
        self,
        job_id: str,
        status: MaterializationJobStatus,
        error: Optional[BaseException] = None,
    ) -> None:
        super().__init__()
        self._job_id: str = job_id
        self._status: MaterializationJobStatus = status
        self._error: Optional[BaseException] = error

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
