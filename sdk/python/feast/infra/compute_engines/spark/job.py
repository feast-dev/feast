from typing import List, Optional

import pyspark
from pyspark.sql import SparkSession

from feast import OnDemandFeatureView, RepoConfig
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
        plan: ExecutionPlan,
        context: ExecutionContext,
        full_feature_names: bool,
        config: RepoConfig,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
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
        self._spark_df = None  # Will be populated on first access

    def _ensure_executed(self):
        if self._spark_df is None:
            result = self._plan.execute(self._context)
            self._spark_df = result.data

    def to_spark_df(self) -> pyspark.sql.DataFrame:
        self._ensure_executed()
        assert self._spark_df is not None, "Execution plan did not produce a DataFrame"
        return self._spark_df

    def to_sql(self) -> str:
        self._ensure_executed()
        return self._plan.to_sql(self._context)
