import logging
import uuid
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
import pyarrow as pa
from ray.data import Dataset

from feast import OnDemandFeatureView
from feast.dqm.errors import ValidationFailed
from feast.errors import SavedDatasetLocationAlreadyExists
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
)
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.infra.offline_stores.offline_store import RetrievalJob, RetrievalMetadata
from feast.infra.ray_initializer import get_ray_wrapper
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage

logger = logging.getLogger(__name__)


class RayDAGRetrievalJob(RetrievalJob):
    """
    Ray-based retrieval job that executes a DAG plan to retrieve historical features.
    """

    def __init__(
        self,
        plan: Optional[ExecutionPlan],
        context: Optional[ExecutionContext],
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        feature_refs: Optional[List[str]] = None,
        metadata: Optional[RetrievalMetadata] = None,
        error: Optional[BaseException] = None,
    ):
        super().__init__()
        self._plan = plan
        self._context = context
        self._config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._feature_refs = feature_refs or []
        self._metadata = metadata
        self._error = error
        self._result_dataset: Optional[Dataset] = None
        self._result_df: Optional[pd.DataFrame] = None
        self._result_arrow: Optional[pa.Table] = None

    def error(self) -> Optional[BaseException]:
        """Return any error that occurred during job execution."""
        return self._error

    def _ensure_executed(self) -> DAGValue:
        """Ensure the execution plan has been executed."""
        if self._result_dataset is None and self._plan and self._context:
            try:
                result = self._plan.execute(self._context)
                if hasattr(result, "data") and isinstance(result.data, Dataset):
                    self._result_dataset = result.data
                else:
                    # If result is not a Ray Dataset, convert it
                    ray_wrapper = get_ray_wrapper()
                    if isinstance(result.data, pd.DataFrame):
                        self._result_dataset = ray_wrapper.from_pandas(result.data)
                    elif isinstance(result.data, pa.Table):
                        self._result_dataset = ray_wrapper.from_arrow(result.data)
                    else:
                        raise ValueError(
                            f"Unsupported result type: {type(result.data)}"
                        )
                return result
            except Exception as e:
                self._error = e
                logger.error(f"Ray DAG execution failed: {e}")
                raise
        elif self._result_dataset is None:
            raise ValueError("No execution plan available or execution failed")

        # Return a mock DAGValue for compatibility
        return DAGValue(data=self._result_dataset, format=DAGFormat.RAY)

    def to_ray_dataset(self) -> Dataset:
        """Get the result as a Ray Dataset."""
        self._ensure_executed()
        assert self._result_dataset is not None, (
            "Dataset should not be None after execution"
        )
        return self._result_dataset

    def to_df(
        self,
        validation_reference=None,
        timeout: Optional[int] = None,
    ) -> pd.DataFrame:
        """Convert the result to a pandas DataFrame."""
        if self._result_df is None:
            if self.on_demand_feature_views:
                # Use parent implementation for ODFV processing
                logger.info(
                    f"Processing {len(self.on_demand_feature_views)} on-demand feature views"
                )
                self._result_df = super().to_df(
                    validation_reference=validation_reference, timeout=timeout
                )
            else:
                # Direct conversion from Ray Dataset
                self._ensure_executed()
                assert self._result_dataset is not None, (
                    "Dataset should not be None after execution"
                )
                self._result_df = self._result_dataset.to_pandas()

        # Handle validation if provided
        if validation_reference:
            try:
                validation_result = validation_reference.profile.validate(
                    self._result_df
                )
                if not validation_result.is_success:
                    raise ValidationFailed(validation_result)
            except ImportError:
                logger.warning("DQM profiler not available, skipping validation")
            except Exception as e:
                logger.error(f"Validation failed: {e}")
                raise ValueError(f"Data validation failed: {e}")

        return self._result_df

    def to_arrow(
        self,
        validation_reference=None,
        timeout: Optional[int] = None,
    ) -> pa.Table:
        """Convert the result to an Arrow Table."""
        if self._result_arrow is None:
            if self.on_demand_feature_views:
                # Use parent implementation for ODFV processing
                self._result_arrow = super().to_arrow(
                    validation_reference=validation_reference, timeout=timeout
                )
            else:
                # Direct conversion from Ray Dataset
                self._ensure_executed()
                assert self._result_dataset is not None, (
                    "Dataset should not be None after execution"
                )
                self._result_arrow = self._result_dataset.to_pandas().to_arrow()

        # Handle validation if provided
        if validation_reference:
            try:
                df = self._result_arrow.to_pandas()
                validation_result = validation_reference.profile.validate(df)
                if not validation_result.is_success:
                    raise ValidationFailed(validation_result)
            except ImportError:
                logger.warning("DQM profiler not available, skipping validation")
            except Exception as e:
                logger.error(f"Validation failed: {e}")
                raise ValueError(f"Data validation failed: {e}")

        return self._result_arrow

    def to_remote_storage(self) -> list[str]:
        """Write the result to remote storage."""
        if not self._config.batch_engine.staging_location:
            raise ValueError("Staging location must be set for remote storage")

        try:
            self._ensure_executed()
            assert self._result_dataset is not None, (
                "Dataset should not be None after execution"
            )
            output_uri = (
                f"{self._config.batch_engine.staging_location}/{str(uuid.uuid4())}"
            )
            self._result_dataset.write_parquet(output_uri)
            logger.debug(f"Wrote result to {output_uri}")
            return [output_uri]
        except Exception as e:
            raise RuntimeError(f"Failed to write to remote storage: {e}")

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ) -> str:
        """Persist the result to the specified storage."""
        if not isinstance(storage, SavedDatasetFileStorage):
            raise ValueError(
                f"Ray compute engine only supports SavedDatasetFileStorage, got {type(storage)}"
            )

        destination_path = storage.file_options.uri

        # Check if destination already exists
        if not destination_path.startswith(("s3://", "gs://", "hdfs://")):
            import os

            if not allow_overwrite and os.path.exists(destination_path):
                raise SavedDatasetLocationAlreadyExists(location=destination_path)
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        try:
            self._ensure_executed()
            assert self._result_dataset is not None, (
                "Dataset should not be None after execution"
            )
            self._result_dataset.write_parquet(destination_path)
            return destination_path
        except Exception as e:
            raise RuntimeError(f"Failed to persist dataset to {destination_path}: {e}")

    def to_sql(self) -> str:
        """Generate SQL representation of the execution plan."""
        if self._plan and self._context:
            return self._plan.to_sql(self._context)
        raise NotImplementedError("SQL generation not available without execution plan")

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """Internal method to get DataFrame (used by parent class)."""
        self._ensure_executed()
        assert self._result_dataset is not None, (
            "Dataset should not be None after execution"
        )
        return self._result_dataset.to_pandas()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        """Internal method to get Arrow Table (used by parent class)."""
        self._ensure_executed()
        assert self._result_dataset is not None, (
            "Dataset should not be None after execution"
        )
        return self._result_dataset.to_pandas().to_arrow()


@dataclass
class RayMaterializationJob(MaterializationJob):
    """
    Ray-based materialization job that tracks the status of feature materialization.
    """

    def __init__(
        self,
        job_id: str,
        status: MaterializationJobStatus,
        result: Optional[DAGValue] = None,
        error: Optional[BaseException] = None,
    ):
        super().__init__()
        self._job_id = job_id
        self._status = status
        self._result = result
        self._error = error

    def job_id(self) -> str:
        return self._job_id

    def status(self) -> MaterializationJobStatus:
        return self._status

    def error(self) -> Optional[BaseException]:
        return self._error

    def should_be_retried(self) -> bool:
        """Ray jobs are generally not retried by default."""
        return False

    def url(self) -> Optional[str]:
        """Ray jobs don't have a specific URL."""
        return None

    def result(self) -> Optional[DAGValue]:
        """Get the result of the materialization job."""
        return self._result
