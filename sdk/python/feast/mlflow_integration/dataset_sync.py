"""Dataset sync engine: pull MLflow GenAI Dataset records into Feast stores."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional

import pandas as pd

if TYPE_CHECKING:
    from feast import FeatureStore

logger = logging.getLogger(__name__)

WATERMARK_TAG_KEY = "feast_last_sync_time"
DEFAULT_BATCH_SIZE = 10_000
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0


@dataclass
class SyncResult:
    """Outcome of a dataset sync operation."""

    records_fetched: int = 0
    records_ingested: int = 0
    new_records: int = 0
    updated_records: int = 0
    errors: List[str] = field(default_factory=list)


def sync_mlflow_dataset_to_feast(
    store: "FeatureStore",
    dataset_name: str,
    feature_view_name: str,
    field_mapping: Optional[Dict[str, str]] = None,
    tracking_uri: Optional[str] = None,
    dataset_id: Optional[str] = None,
    experiment_ids: Optional[List[str]] = None,
    incremental: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
) -> SyncResult:
    """Pull records from an MLflow GenAI Dataset and ingest into Feast.

    Steps:
    1. Connect to MLflow and fetch dataset via ``get_dataset(name=...)``
    2. Convert to pandas DataFrame via ``dataset.to_df()``
    3. Flatten nested columns (inputs, expectations, source, tags)
    4. Apply field_mapping overrides
    5. Filter for incremental (last_update_time > last_sync_time) if enabled
    6. Write to online store via ``store.write_to_online_store()``
    7. Push to offline store via ``store.push()``

    Args:
        store: Feast FeatureStore instance.
        dataset_name: MLflow GenAI dataset name.
        feature_view_name: Target Feast FeatureView name.
        field_mapping: Custom field mapping overrides.
        tracking_uri: MLflow tracking URI override.
        dataset_id: Direct dataset ID (alternative to name).
        experiment_ids: Filter by MLflow experiment IDs.
        incremental: Only sync records updated since last sync.
        batch_size: Number of rows to write per batch.
        dry_run: If True, fetch and flatten but don't write to stores.

    Returns:
        SyncResult with counts of fetched/ingested/new/updated records.
    """
    import mlflow

    result = SyncResult()

    effective_uri = _resolve_tracking_uri(store, tracking_uri)
    if effective_uri:
        mlflow.set_tracking_uri(effective_uri)

    dataset = _fetch_dataset_with_retry(dataset_name, dataset_id)
    if dataset is None:
        result.errors.append(f"Failed to fetch MLflow dataset '{dataset_name}'")
        return result

    df = dataset.to_df()
    result.records_fetched = len(df)

    if df.empty:
        logger.info("MLflow dataset '%s' has no records.", dataset_name)
        return result

    df = flatten_mlflow_dataset_df(df, field_mapping=field_mapping)

    if incremental:
        last_sync = _get_last_sync_time(dataset)
        if last_sync is not None:
            before_count = len(df)
            df = df[df["event_timestamp"] > last_sync]
            result.new_records = len(df)
            logger.info(
                "Incremental filter: %d → %d records (since %s)",
                before_count,
                len(df),
                last_sync.isoformat(),
            )
        else:
            result.new_records = len(df)
    else:
        result.new_records = len(df)

    if df.empty:
        logger.info("No new records to sync.")
        return result

    if dry_run:
        logger.info("Dry run: would ingest %d records.", len(df))
        result.records_ingested = 0
        return result

    for start in range(0, len(df), batch_size):
        batch = df.iloc[start : start + batch_size]
        try:
            store.write_to_online_store(feature_view_name, batch)
        except Exception as e:
            result.errors.append(f"Online write error at offset {start}: {e}")
            logger.error("Failed to write batch to online store: %s", e)
            continue

        try:
            store.push(feature_view_name, batch)
        except Exception as e:
            logger.warning(
                "Push to offline store failed at offset %d: %s (continuing)", start, e
            )

        result.records_ingested += len(batch)

    _set_last_sync_time(dataset)
    result.updated_records = result.records_ingested

    logger.info(
        "Sync complete: fetched=%d, ingested=%d",
        result.records_fetched,
        result.records_ingested,
    )
    return result


def flatten_mlflow_dataset_df(
    df: pd.DataFrame,
    field_mapping: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Flatten MLflow's nested dict columns into flat Feast-compatible columns.

    Default flattening rules:
    - inputs.X → input_X
    - expectations.X → X (direct, since these are the "features")
    - source.trace.trace_id → trace_id
    - tags.X → tag_X
    - last_update_time → event_timestamp
    - dataset_record_id → dataset_record_id (preserved as-is)

    User-provided field_mapping overrides defaults. Keys are dot-delimited
    paths like ``expectations.expected_response``, values are target column
    names.
    """
    flat: Dict[str, list] = {}
    n = len(df)

    if "dataset_record_id" in df.columns:
        flat["dataset_record_id"] = df["dataset_record_id"].tolist()

    if "inputs" in df.columns:
        _expand_dict_column(df["inputs"], flat, prefix="input_")

    if "expectations" in df.columns:
        _expand_dict_column(df["expectations"], flat, prefix="")

    if "source" in df.columns:
        trace_ids = []
        for val in df["source"]:
            if isinstance(val, dict):
                trace = val.get("trace", {})
                trace_ids.append(
                    trace.get("trace_id") if isinstance(trace, dict) else None
                )
            else:
                trace_ids.append(None)
        flat["trace_id"] = trace_ids

    if "tags" in df.columns:
        _expand_dict_column(df["tags"], flat, prefix="tag_")

    if "last_update_time" in df.columns:
        flat["event_timestamp"] = pd.to_datetime(
            df["last_update_time"], utc=True
        ).tolist()
    elif "create_time" in df.columns:
        flat["event_timestamp"] = pd.to_datetime(df["create_time"], utc=True).tolist()
    else:
        flat["event_timestamp"] = [datetime.now(timezone.utc)] * n

    result_df = pd.DataFrame(flat)

    if field_mapping:
        result_df = _apply_field_mapping(result_df, df, field_mapping)

    return result_df


def _expand_dict_column(series: pd.Series, flat: Dict[str, list], prefix: str) -> None:
    """Expand a Series of dicts into flat columns with a given prefix."""
    keys_seen: set = set()
    for val in series:
        if isinstance(val, dict):
            keys_seen.update(val.keys())

    for key in sorted(keys_seen):
        col_name = f"{prefix}{key}"
        flat[col_name] = [
            val.get(key) if isinstance(val, dict) else None for val in series
        ]


def _apply_field_mapping(
    result_df: pd.DataFrame,
    original_df: pd.DataFrame,
    field_mapping: Dict[str, str],
) -> pd.DataFrame:
    """Apply user-provided field mapping overrides.

    Mapping keys are dot-delimited paths (e.g. ``expectations.expected_response``).
    If the source column already exists in result_df under its default name,
    it's renamed. Otherwise the value is extracted from the original nested data.
    """
    for src_path, target_name in field_mapping.items():
        parts = src_path.split(".")

        default_name = _default_column_name(parts)
        if default_name in result_df.columns:
            result_df = result_df.rename(columns={default_name: target_name})
        elif len(parts) >= 2 and parts[0] in original_df.columns:
            values = []
            for val in original_df[parts[0]]:
                v = val
                for part in parts[1:]:
                    if isinstance(v, dict):
                        v = v.get(part)
                    else:
                        v = None
                        break
                values.append(v)
            result_df[target_name] = values

    return result_df


def _default_column_name(parts: List[str]) -> str:
    """Compute the default flat column name for a dot-path."""
    if len(parts) == 1:
        return parts[0]

    top = parts[0]
    rest = "_".join(parts[1:])

    if top == "inputs":
        return f"input_{rest}"
    elif top == "expectations":
        return rest
    elif top == "tags":
        return f"tag_{rest}"
    elif top == "source":
        return rest
    return f"{top}_{rest}"


def _resolve_tracking_uri(
    store: "FeatureStore", override: Optional[str]
) -> Optional[str]:
    """Resolve MLflow tracking URI from override, config, or env."""
    import os

    if override:
        return override

    mlflow_cfg = getattr(store.config, "mlflow", None)
    if mlflow_cfg is not None:
        uri = getattr(mlflow_cfg, "tracking_uri", None)
        if uri:
            return uri

    return os.environ.get("MLFLOW_TRACKING_URI")


def _fetch_dataset_with_retry(dataset_name: str, dataset_id: Optional[str] = None):
    """Fetch MLflow dataset with exponential backoff retry."""
    import mlflow.genai.datasets

    for attempt in range(MAX_RETRIES):
        try:
            if dataset_id:
                return mlflow.genai.datasets.get_dataset(dataset_id=dataset_id)
            return mlflow.genai.datasets.get_dataset(name=dataset_name)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logger.error(
                    "Failed to fetch dataset '%s' after %d attempts: %s",
                    dataset_name,
                    MAX_RETRIES,
                    e,
                )
                return None
            wait = RETRY_BACKOFF_BASE**attempt
            logger.warning(
                "Retry %d/%d fetching dataset '%s': %s (waiting %.1fs)",
                attempt + 1,
                MAX_RETRIES,
                dataset_name,
                e,
                wait,
            )
            time.sleep(wait)
    return None


def _get_last_sync_time(dataset) -> Optional[datetime]:
    """Read the last sync watermark from MLflow dataset tags."""
    tags = getattr(dataset, "tags", None)
    if not tags:
        return None
    watermark = tags.get(WATERMARK_TAG_KEY)
    if not watermark:
        return None
    try:
        return datetime.fromisoformat(watermark)
    except (ValueError, TypeError):
        return None


def _set_last_sync_time(dataset) -> None:
    """Set the sync watermark tag on the MLflow dataset."""
    try:
        import mlflow

        now = datetime.now(timezone.utc).isoformat()
        if hasattr(dataset, "set_tag"):
            dataset.set_tag(WATERMARK_TAG_KEY, now)
        elif hasattr(dataset, "dataset_id"):
            mlflow.genai.datasets.set_dataset_tag(
                dataset_id=dataset.dataset_id,
                key=WATERMARK_TAG_KEY,
                value=now,
            )
    except Exception as e:
        logger.warning("Failed to set sync watermark: %s", e)
