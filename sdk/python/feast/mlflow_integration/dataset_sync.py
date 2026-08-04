"""Dataset sync engine: pull MLflow GenAI Dataset records into Feast stores."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

from feast.feature_store import PushMode
from feast.mlflow_integration.config import (
    DEFAULT_BATCH_SIZE,
    WATERMARK_TAG_KEY,
    resolve_tracking_uri,
)

if TYPE_CHECKING:
    from feast import FeatureStore

logger = logging.getLogger(__name__)

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


def _dataset_sync_defaults(store: "FeatureStore") -> tuple[Dict[str, str], str, int]:
    """Read field mapping, watermark key, and batch size from store config."""
    mlflow_cfg = getattr(store.config, "mlflow", None)
    sync_cfg = getattr(mlflow_cfg, "dataset_sync", None) if mlflow_cfg else None
    if sync_cfg is None:
        return {}, WATERMARK_TAG_KEY, DEFAULT_BATCH_SIZE
    return (
        dict(getattr(sync_cfg, "default_field_mapping", {}) or {}),
        str(getattr(sync_cfg, "watermark_key", WATERMARK_TAG_KEY)),
        int(getattr(sync_cfg, "default_batch_size", DEFAULT_BATCH_SIZE)),
    )


def sync_mlflow_dataset_to_feast(
    store: "FeatureStore",
    feature_view_name: str,
    dataset_name: Optional[str] = None,
    field_mapping: Optional[Dict[str, str]] = None,
    tracking_uri: Optional[str] = None,
    dataset_id: Optional[str] = None,
    incremental: bool = True,
    batch_size: Optional[int] = None,
    dry_run: bool = False,
) -> SyncResult:
    """Pull records from an MLflow GenAI Dataset and ingest into Feast.

    When the target FeatureView/LabelView uses an ``MlflowDatasetSource``,
    ``dataset_name`` / ``dataset_id`` / ``field_mapping`` / ``tracking_uri``
    are taken from that source unless explicitly overridden by arguments.

    Steps:
    1. Resolve MLflow dataset identity (source or CLI args)
    2. Fetch dataset via ``get_dataset`` → ``to_df()``
    3. Flatten nested columns and apply field mapping
    4. Incremental filter via watermark tag when enabled
    5. Write online via ``write_to_online_store``
    6. Write offline via ``write_to_offline_store`` (batch_source) or
       ``push(..., OFFLINE)`` for legacy PushSource targets
    """
    try:
        import mlflow
    except ImportError as e:
        raise ImportError(
            "The 'mlflow' package is required for dataset sync. "
            "Install it with: pip install 'feast[mlflow]'"
        ) from e

    result = SyncResult()
    default_mapping, watermark_key, default_batch = _dataset_sync_defaults(store)
    mlflow_source = _get_mlflow_dataset_source(store, feature_view_name)

    effective_dataset_name = dataset_name or (
        mlflow_source.dataset_name if mlflow_source else None
    )
    effective_dataset_id = dataset_id or (
        mlflow_source.dataset_id if mlflow_source else None
    )
    source_mapping = (
        dict(mlflow_source.field_mapping)
        if mlflow_source and mlflow_source.field_mapping
        else {}
    )
    effective_mapping = {**default_mapping, **source_mapping, **(field_mapping or {})}
    effective_batch = default_batch if batch_size is None else batch_size
    source_uri = mlflow_source.tracking_uri if mlflow_source else None

    if not effective_dataset_name and not effective_dataset_id:
        result.errors.append(
            f"No MLflow dataset identity for '{feature_view_name}'. "
            "Declare MlflowDatasetSource on the view or pass dataset_name/dataset_id."
        )
        return result

    effective_uri = _resolve_tracking_uri(store, tracking_uri or source_uri)
    if effective_uri:
        mlflow.set_tracking_uri(effective_uri)

    dataset = _fetch_dataset_with_retry(
        effective_dataset_name or "", effective_dataset_id
    )
    if dataset is None:
        label = effective_dataset_name or effective_dataset_id
        result.errors.append(f"Failed to fetch MLflow dataset '{label}'")
        return result

    df = dataset.to_df()
    result.records_fetched = len(df)

    if df.empty:
        logger.info(
            "MLflow dataset '%s' has no records.",
            effective_dataset_name or effective_dataset_id,
        )
        return result

    df = flatten_mlflow_dataset_df(df, field_mapping=effective_mapping or None)

    if incremental:
        last_sync = _get_last_sync_time(dataset, watermark_key=watermark_key)
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

    df = _align_df_to_feature_view(store, feature_view_name, df)

    for start in range(0, len(df), effective_batch):
        batch = df.iloc[start : start + effective_batch]
        try:
            store.write_to_online_store(feature_view_name, batch)
        except Exception as e:
            result.errors.append(f"Online write error at offset {start}: {e}")
            logger.error("Failed to write batch to online store: %s", e)
            continue

        _write_offline_batch(store, feature_view_name, batch, result, start)
        result.records_ingested += len(batch)

    if not result.errors:
        _set_last_sync_time(dataset, watermark_key=watermark_key)
    else:
        logger.warning(
            "Skipping watermark update due to %d sync error(s); "
            "failed records will be retried on the next incremental sync.",
            len(result.errors),
        )
    result.updated_records = result.records_ingested

    logger.info(
        "Sync complete: fetched=%d, ingested=%d",
        result.records_fetched,
        result.records_ingested,
    )
    return result


def sync_all_mlflow_dataset_sources(
    store: "FeatureStore",
    *,
    incremental: bool = True,
    batch_size: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, SyncResult]:
    """Sync every FeatureView/LabelView whose source is ``MlflowDatasetSource``."""
    results: Dict[str, SyncResult] = {}
    for view_name in _list_mlflow_dataset_view_names(store):
        results[view_name] = sync_mlflow_dataset_to_feast(
            store=store,
            feature_view_name=view_name,
            incremental=incremental,
            batch_size=batch_size,
            dry_run=dry_run,
        )
    return results


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
            elif hasattr(val, "source_data"):
                source_data = val.source_data
                trace_ids.append(
                    source_data.get("trace_id")
                    if isinstance(source_data, dict)
                    else None
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
    if override:
        return override

    mlflow_cfg = getattr(store.config, "mlflow", None)
    if mlflow_cfg is not None and hasattr(mlflow_cfg, "get_tracking_uri"):
        return mlflow_cfg.get_tracking_uri()

    return resolve_tracking_uri(None)


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


def _get_last_sync_time(
    dataset, watermark_key: str = WATERMARK_TAG_KEY
) -> Optional[datetime]:
    """Read the last sync watermark from MLflow dataset tags."""
    tags = getattr(dataset, "tags", None)
    if not tags:
        return None
    watermark = tags.get(watermark_key)
    if not watermark:
        return None
    try:
        return datetime.fromisoformat(watermark)
    except (ValueError, TypeError):
        return None


def _set_last_sync_time(dataset, watermark_key: str = WATERMARK_TAG_KEY) -> None:
    """Set the sync watermark tag on the MLflow dataset."""
    try:
        import mlflow.genai.datasets

        now = datetime.now(timezone.utc).isoformat()
        if hasattr(dataset, "dataset_id"):
            mlflow.genai.datasets.set_dataset_tags(
                dataset_id=dataset.dataset_id,
                tags={watermark_key: now},
            )
    except Exception as e:
        logger.warning("Failed to set sync watermark: %s", e)


def _align_df_to_feature_view(
    store: "FeatureStore", feature_view_name: str, df: pd.DataFrame
) -> pd.DataFrame:
    """Align DataFrame columns to match the target FeatureView or LabelView schema.

    Adds missing schema columns as None and keeps entity/timestamp columns.
    Extra columns not in the schema are dropped to avoid write errors.
    """
    fv: Any = None
    try:
        fv = store.get_feature_view(feature_view_name)
    except Exception:
        pass

    if fv is None and hasattr(store, "get_label_view"):
        try:
            fv = store.get_label_view(feature_view_name)
        except Exception:
            pass

    if fv is None:
        return df

    schema_cols = {f.name for f in fv.features}
    entity_cols = {col.name for col in fv.entity_columns}

    if not schema_cols and not entity_cols:
        return df

    required_cols: set[str] = schema_cols | entity_cols | {"event_timestamp"}

    for col in fv.schema:
        if col.name in fv.entities:
            required_cols.add(col.name)

    for join_key in fv.join_keys:
        required_cols.add(join_key)

    feature_types = {f.name: f.dtype for f in fv.features}
    for col_name in required_cols:
        if col_name not in df.columns:
            if col_name in feature_types:
                df[col_name] = pd.Series([None] * len(df), dtype="object")
            else:
                df[col_name] = None

    keep = [c for c in df.columns if c in required_cols]
    return df[keep]


def _get_view(store: "FeatureStore", feature_view_name: str) -> Any:
    """Return a FeatureView or LabelView by name, or None."""
    try:
        return store.get_feature_view(feature_view_name)
    except Exception:
        pass
    if hasattr(store, "get_label_view"):
        try:
            return store.get_label_view(feature_view_name)
        except Exception:
            pass
    return None


def _get_mlflow_dataset_source(store: "FeatureStore", feature_view_name: str) -> Any:
    """Return MlflowDatasetSource from a view, if present."""
    from feast.infra.data_sources.mlflow.mlflow_dataset_source import (
        MlflowDatasetSource,
    )

    view = _get_view(store, feature_view_name)
    if view is None:
        return None

    for attr in ("stream_source", "source", "data_source"):
        src = getattr(view, attr, None)
        if isinstance(src, MlflowDatasetSource):
            return src
    return None


def _list_mlflow_dataset_view_names(store: "FeatureStore") -> List[str]:
    """Names of FeatureViews/LabelViews backed by MlflowDatasetSource."""
    from feast.infra.data_sources.mlflow.mlflow_dataset_source import (
        MlflowDatasetSource,
    )

    names: List[str] = []
    for view in list(store.list_feature_views()) + list(
        store.list_label_views() if hasattr(store, "list_label_views") else []
    ):
        for attr in ("stream_source", "source", "data_source"):
            src = getattr(view, attr, None)
            if isinstance(src, MlflowDatasetSource):
                names.append(view.name)
                break
    return names


def _resolve_push_source_name(
    store: "FeatureStore", feature_view_name: str
) -> Optional[str]:
    """Resolve the PushSource name for a FeatureView or LabelView.

    ``store.push()`` expects the push source name, not the feature view name.
    This inspects the view's source/stream_source to find it.
    """
    view = _get_view(store, feature_view_name)
    if view is None:
        return None

    for attr in ("stream_source", "source"):
        src = getattr(view, attr, None)
        if src is not None and hasattr(src, "name"):
            from feast.data_source import PushSource as _PS

            if isinstance(src, _PS):
                return src.name
    return None


def _write_offline_batch(
    store: "FeatureStore",
    feature_view_name: str,
    batch: pd.DataFrame,
    result: SyncResult,
    start: int,
) -> None:
    """Write a batch to the offline store via batch_source or PushSource."""
    mlflow_source = _get_mlflow_dataset_source(store, feature_view_name)
    if mlflow_source is not None:
        try:
            store.write_to_offline_store(feature_view_name, batch)
        except Exception as e:
            logger.warning(
                "Offline write failed at offset %d: %s (continuing)",
                start,
                e,
            )
        return

    push_source_name = _resolve_push_source_name(store, feature_view_name)
    if push_source_name:
        try:
            store.push(push_source_name, batch, to=PushMode.OFFLINE)
        except Exception as e:
            logger.warning(
                "Push to offline store failed at offset %d: %s (continuing)",
                start,
                e,
            )
