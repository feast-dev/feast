from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
import pyarrow as pa
from trino.exceptions import TrinoConnectionError, TrinoQueryError

from feast import OnDemandFeatureView, RepoConfig
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
)
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.trino.sql_builder import TrinoQueryPlan
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import Trino
from feast.infra.offline_stores.offline_store import RetrievalJob, RetrievalMetadata
from feast.saved_dataset import SavedDatasetStorage

logger = logging.getLogger(__name__)


class TrinoDAGRetrievalJob(RetrievalJob):
    """Lazy retrieval job returned by Trino DAG execution.

    Maintains the compiled SQL query without executing it eagerly. Data is streamed
    from Trino only when .to_arrow() or .to_df() is explicitly called.
    """

    def __init__(
        self,
        client: Optional[Trino],
        plan: Optional[ExecutionPlan],
        context: Optional[ExecutionContext],
        full_feature_names: bool,
        config: RepoConfig,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
        error: Optional[BaseException] = None,
        query: Optional[str] = None,
    ):
        self._client = client
        self._plan = plan
        self._context = context
        self._full_feature_names = full_feature_names
        self._config = config
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        self._error = error
        self._query = query or ""
        self._arrow_table: Optional[pa.Table] = None

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def error(self) -> Optional[BaseException]:
        return self._error

    def to_sql(self) -> str:
        """Return the compiled Trino ANSI SQL query for the feature retrieval DAG."""
        if self._query:
            return self._query

        if self._plan and self._context:
            result = self._plan.execute(self._context)
            if isinstance(result.data, TrinoQueryPlan):
                self._query = result.data.to_sql()
                return self._query
            elif isinstance(result.data, str):
                self._query = result.data
                return self._query

        return self._query

    def _ensure_executed(self) -> pa.Table:
        """Execute the compiled SQL query against Trino cluster and cache as Arrow table."""
        if self._error:
            raise self._error

        if self._arrow_table is not None:
            return self._arrow_table

        query = self.to_sql()
        if not self._client or not query:
            raise RuntimeError(
                "No Trino client or SQL query available to retrieve data."
            )

        try:
            results = self._client.execute_query(query)
            df = results.to_dataframe()
            self._arrow_table = pa.Table.from_pandas(df)
            return self._arrow_table
        except TrinoConnectionError as ce:
            logger.error(
                "Connection failed while executing Trino retrieval query: %s", ce
            )
            raise
        except TrinoQueryError as e:
            logger.debug("Trino query error during retrieval execution: %s", e)
            raise

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        return self._ensure_executed()

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        return self._ensure_executed().to_pandas()

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ) -> None:
        """Persist training dataset directly to a Trino table via CREATE TABLE AS SELECT."""
        from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import (
            SavedDatasetTrinoStorage,
        )

        if not isinstance(storage, SavedDatasetTrinoStorage):
            raise ValueError(
                f"Expected `SavedDatasetTrinoStorage` but received {type(storage)}"
            )

        destination_table = storage.trino_options.table
        query = self.to_sql()
        if not self._client or not query:
            raise RuntimeError(
                "Cannot persist dataset without client and compiled query."
            )

        if allow_overwrite:
            try:
                self._client.execute_query(f"DROP TABLE IF EXISTS {destination_table}")
            except TrinoConnectionError:
                raise
            except TrinoQueryError as e:
                logger.debug(
                    "Failed to drop existing table %s: %s", destination_table, e
                )

        create_query = f"CREATE TABLE {destination_table} AS ({query})"
        self._client.execute_query(create_query)


@dataclass
class TrinoMaterializationJob(MaterializationJob):
    """Tracks materialization jobs executed via TrinoComputeEngine."""

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
        # TODO(@mrosti): we could probably retry some trino failures but no one uses this
        return False

    def job_id(self) -> str:
        return self._job_id

    def url(self) -> Optional[str]:
        return None
