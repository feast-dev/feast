from dataclasses import dataclass
from typing import List, Optional

import pyspark
from pyspark.sql import SparkSession

from feast import OnDemandFeatureView, RepoConfig
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
)
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkRetrievalJob,
)
from feast.infra.offline_stores.offline_store import RetrievalMetadata


class SparkDAGRetrievalJob(SparkRetrievalJob):
    def __init__(
        self,
        spark_session: SparkSession,
        plan: Optional[ExecutionPlan],
        context: ExecutionContext,
        full_feature_names: bool,
        config: RepoConfig,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
        error: Optional[BaseException] = None,
    ):
        super().__init__(
            spark_session=spark_session,
            query="",
            full_feature_names=full_feature_names,
            config=config,
            on_demand_feature_views=on_demand_feature_views,
            metadata=metadata,
        )
        self._plan = plan
        self._context = context
        self._metadata = metadata
        self._spark_df = None
        self._error = error

    def error(self) -> Optional[BaseException]:
        return self._error

    def _ensure_executed(self):
        if self._spark_df is None:
            result = self._plan.execute(self._context)
            self._spark_df = result.data

    def to_spark_df(self) -> pyspark.sql.DataFrame:
        self._ensure_executed()
        assert self._spark_df is not None, "Execution plan did not produce a DataFrame"
        return self._spark_df

    def to_sql(self) -> str:
        assert self._plan is not None, "Execution plan is not set"
        return self._plan.to_sql(self._context)


@dataclass
class SparkMaterializationJob(MaterializationJob):
    def url(self) -> Optional[str]:
        pass

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
