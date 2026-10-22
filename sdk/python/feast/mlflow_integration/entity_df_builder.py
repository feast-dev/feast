from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd

if TYPE_CHECKING:
    from feast import FeatureStore

_logger = logging.getLogger(__name__)


class FeastMlflowEntityDfError(Exception):
    """Raised when an entity DataFrame cannot be built from an MLflow run."""

    pass


class FeastMlflowEntityDfBuilder:
    """Reconstructs entity DataFrames from MLflow run artifacts.

    Instantiated once inside :class:`FeastMlflowClient` and reuses its
    ``MlflowClient`` — no separate ``import mlflow`` needed.
    """

    def __init__(self, store: "FeatureStore", mlflow_mod: Any, client: Any):
        self._store = store
        self._mlflow = mlflow_mod
        self._client = client

    def get_entity_df(
        self,
        run_id: str,
        timestamp_column: str = "event_timestamp",
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Build an entity DataFrame from an MLflow run's artifacts.

        Convention: the run should have an artifact named ``entity_df.parquet``
        (or ``entity_df.csv``), saved automatically when
        ``auto_log_entity_df: true`` is set in ``feature_store.yaml``.

        Args:
            run_id: The MLflow run ID.
            timestamp_column: Expected name of the timestamp column in the
                entity DataFrame.
            max_rows: Optional limit on number of rows to load.

        Returns:
            A ``pd.DataFrame`` suitable for passing to
            ``store.get_historical_features(entity_df=...)``.

        Raises:
            FeastMlflowEntityDfError: If run not found or no entity data
                is available on the run.
        """
        from mlflow.exceptions import MlflowException

        try:
            self._client.get_run(run_id)
        except MlflowException as e:
            raise FeastMlflowEntityDfError(f"Run '{run_id}' not found: {e}")

        df = self._try_artifact(run_id, "entity_df.parquet", "parquet")
        if df is not None:
            if max_rows is not None:
                df = df.head(max_rows)
            self._validate_timestamp_col(df, timestamp_column)
            return df

        df = self._try_artifact(run_id, "entity_df.csv", "csv")
        if df is not None:
            if max_rows is not None:
                df = df.head(max_rows)
            self._validate_timestamp_col(df, timestamp_column)
            return df

        raise FeastMlflowEntityDfError(
            f"No entity data found for run '{run_id}'. "
            f"Expected artifact 'entity_df.parquet' or 'entity_df.csv'. "
            f"Ensure auto_log_entity_df is enabled in feature_store.yaml."
        )

    def _try_artifact(
        self, run_id: str, artifact_name: str, fmt: str
    ) -> Optional[pd.DataFrame]:
        try:
            local_path = self._client.download_artifacts(run_id, artifact_name)
            if fmt == "parquet":
                return pd.read_parquet(local_path)
            if fmt == "csv":
                return pd.read_csv(local_path)
            _logger.warning(
                "Unsupported entity DataFrame format '%s' for artifact '%s'. "
                "Only 'parquet' and 'csv' are supported.",
                fmt,
                artifact_name,
            )
            return None
        except Exception as e:
            _logger.debug(
                "Artifact '%s' not found for run '%s': %s",
                artifact_name,
                run_id,
                e,
            )
            return None

    @staticmethod
    def _validate_timestamp_col(df: pd.DataFrame, col: str) -> None:
        if col not in df.columns:
            raise FeastMlflowEntityDfError(
                f"Entity DataFrame missing required timestamp column '{col}'. "
                f"Available columns: {list(df.columns)}"
            )
