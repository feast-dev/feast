from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

_logger = logging.getLogger(__name__)


class FeastMlflowEntityDfError(Exception):
    """Raised when an entity DataFrame cannot be built from an MLflow run."""

    pass


def get_entity_df_from_mlflow_run(
    run_id: str,
    tracking_uri: Optional[str] = None,
    timestamp_column: str = "event_timestamp",
) -> pd.DataFrame:
    """Build an entity DataFrame from an MLflow run's artifacts or params.

    Convention: the run should have an artifact named ``entity_df.parquet``
    (or ``entity_df.csv``).  Alternatively, a run param
    ``feast.entity_df_path`` pointing to a local/remote file path.

    Args:
        run_id: The MLflow run ID.
        tracking_uri: Optional MLflow tracking URI.
        timestamp_column: Expected name of the timestamp column in the
            entity DataFrame.

    Returns:
        A ``pd.DataFrame`` suitable for passing to
        ``store.get_historical_features(entity_df=...)``.

    Raises:
        FeastMlflowEntityDfError: If mlflow is not installed, run not found,
            or no entity data is available on the run.
    """
    try:
        import mlflow
        from mlflow.exceptions import MlflowException
    except ImportError:
        raise FeastMlflowEntityDfError(
            "mlflow is not installed. Install with: pip install feast[mlflow]"
        )

    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    client = mlflow.MlflowClient()

    try:
        run = client.get_run(run_id)
    except MlflowException as e:
        raise FeastMlflowEntityDfError(f"Run '{run_id}' not found: {e}")

    # Strategy 1: artifact entity_df.parquet
    df = _try_artifact(client, run_id, "entity_df.parquet", "parquet")
    if df is not None:
        _validate_timestamp_col(df, timestamp_column)
        return df

    # Strategy 2: artifact entity_df.csv
    df = _try_artifact(client, run_id, "entity_df.csv", "csv")
    if df is not None:
        _validate_timestamp_col(df, timestamp_column)
        return df

    # Strategy 3: run param feast.entity_df_path
    params = run.data.params
    path = params.get("feast.entity_df_path")
    if path:
        try:
            if path.endswith(".parquet"):
                df = pd.read_parquet(path)
            else:
                df = pd.read_csv(path)
            _validate_timestamp_col(df, timestamp_column)
            return df
        except FeastMlflowEntityDfError:
            raise
        except Exception as e:
            raise FeastMlflowEntityDfError(
                f"Could not load entity df from param path '{path}': {e}"
            )

    raise FeastMlflowEntityDfError(
        f"No entity data found for run '{run_id}'. "
        f"Expected artifact 'entity_df.parquet' or 'entity_df.csv', "
        f"or param 'feast.entity_df_path'."
    )


def _try_artifact(client, run_id: str, artifact_name: str, fmt: str):
    """Try to download and load an artifact as a DataFrame."""
    try:
        local_path = client.download_artifacts(run_id, artifact_name)
        if fmt == "parquet":
            return pd.read_parquet(local_path)
        return pd.read_csv(local_path)
    except Exception:
        return None


def _validate_timestamp_col(df: pd.DataFrame, col: str):
    """Ensure the expected timestamp column exists."""
    if col not in df.columns:
        raise FeastMlflowEntityDfError(
            f"Entity DataFrame missing required timestamp column '{col}'. "
            f"Available columns: {list(df.columns)}"
        )
