"""Dataset sync engine: pull MLflow GenAI Dataset records into Feast stores."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

from feast.feature_store import PushMode

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

    df = _align_df_to_feature_view(store, feature_view_name, df)
    push_source_name = _resolve_push_source_name(store, feature_view_name)

    for start in range(0, len(df), batch_size):
        batch = df.iloc[start : start + batch_size]
        try:
            store.write_to_online_store(feature_view_name, batch)
        except Exception as e:
            result.errors.append(f"Online write error at offset {start}: {e}")
            logger.error("Failed to write batch to online store: %s", e)
            continue

        if push_source_name:
            try:
                store.push(push_source_name, batch, to=PushMode.ONLINE_AND_OFFLINE)
            except Exception as e:
                logger.warning(
                    "Push to offline store failed at offset %d: %s (continuing)",
                    start,
                    e,
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


def sync_trace_assessments_to_feast(
    store: "FeatureStore",
    experiment_name: str,
    feature_view_name: str,
    tracking_uri: Optional[str] = None,
    filter_string: Optional[str] = None,
    max_results: int = 1000,
    assessment_names: Optional[List[str]] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
    pivot: bool = False,
    assessment_mapping: Optional[Dict[str, str]] = None,
    labeler_column: str = "labeler",
) -> SyncResult:
    """Pull assessments (expectations + feedback) from MLflow traces into Feast.

    Scans traces in a given experiment, extracts all assessments logged on them
    (via MLflow UI or ``mlflow.log_expectation`` / ``mlflow.log_feedback``), and
    writes them as rows into a Feast FeatureView or LabelView.

    **Flat mode** (default): each assessment becomes its own row with columns
    ``trace_id``, ``assessment_name``, ``assessment_type``, ``value``,
    ``source_id``, ``rationale``, ``event_timestamp``.

    **Pivot mode** (``pivot=True``): assessments for the same ``trace_id`` are
    pivoted into a single row whose columns match a LabelView schema. Use
    ``assessment_mapping`` to control how assessment names map to column names
    (e.g. ``{"expected_response": "corrected_response"}``). The ``source_id``
    from the assessment is written to ``labeler_column`` (default ``"labeler"``).

    Args:
        store: Feast FeatureStore instance.
        experiment_name: MLflow experiment name to scan for traces.
        feature_view_name: Target Feast FeatureView/LabelView name.
        tracking_uri: MLflow tracking URI override.
        filter_string: MLflow search_traces filter expression.
        max_results: Maximum number of traces to scan.
        assessment_names: If provided, only sync assessments with these names.
        batch_size: Number of rows to write per batch.
        dry_run: If True, extract but don't write to stores.
        pivot: When True, pivot assessments into LabelView-compatible rows
            (one row per trace_id) instead of one row per assessment.
        assessment_mapping: Maps assessment names to target column names.
            Only used when ``pivot=True``. Unmapped assessment names are
            used as column names directly.
        labeler_column: Target column name for the assessment source_id
            when ``pivot=True``. Defaults to ``"labeler"``.

    Returns:
        SyncResult with counts of fetched/ingested records.
    """
    import mlflow

    result = SyncResult()

    effective_uri = _resolve_tracking_uri(store, tracking_uri)
    if effective_uri:
        mlflow.set_tracking_uri(effective_uri)

    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        result.errors.append(f"MLflow experiment '{experiment_name}' not found")
        return result

    traces = mlflow.search_traces(
        experiment_ids=[experiment.experiment_id],
        max_results=max_results,
        **({"filter_string": filter_string} if filter_string else {}),
    )

    rows: List[Dict] = []
    trace_iter = (
        traces.iterrows()
        if isinstance(traces, pd.DataFrame)
        else ((None, t) for t in traces)
    )
    for _, trace_row in trace_iter:
        trace_id = _get_trace_id_from_row(trace_row)
        assessments = _get_assessments_from_row(trace_row)
        if not assessments:
            continue

        for assessment in assessments:
            name = _assess_get(assessment, "assessment_name") or _assess_get(
                assessment, "name"
            )
            if not name:
                continue
            if assessment_names and name not in assessment_names:
                continue

            row: Dict = {
                "trace_id": trace_id,
                "assessment_name": name,
            }

            expectation_val = _assess_get(assessment, "expectation")
            feedback_val = _assess_get(assessment, "feedback")

            if expectation_val is not None:
                row["assessment_type"] = "expectation"
                row["value"] = (
                    str(expectation_val.get("value", ""))
                    if isinstance(expectation_val, dict)
                    else str(getattr(expectation_val, "value", ""))
                )
            elif feedback_val is not None:
                row["assessment_type"] = "feedback"
                row["value"] = (
                    str(feedback_val.get("value", ""))
                    if isinstance(feedback_val, dict)
                    else str(getattr(feedback_val, "value", ""))
                )
            else:
                continue

            source = _assess_get(assessment, "source")
            if source:
                row["source_id"] = (
                    source.get("source_id", "")
                    if isinstance(source, dict)
                    else getattr(source, "source_id", "")
                )
            else:
                row["source_id"] = ""

            row["rationale"] = _assess_get(assessment, "rationale") or ""

            create_time = _assess_get(assessment, "create_time_ms") or _assess_get(
                assessment, "create_time"
            )
            if create_time:
                if isinstance(create_time, (int, float)):
                    row["event_timestamp"] = datetime.fromtimestamp(
                        create_time / 1000, tz=timezone.utc
                    )
                elif isinstance(create_time, str):
                    row["event_timestamp"] = datetime.fromisoformat(
                        create_time.replace("Z", "+00:00")
                    )
                else:
                    row["event_timestamp"] = datetime.now(tz=timezone.utc)
            else:
                row["event_timestamp"] = datetime.now(tz=timezone.utc)

            rows.append(row)

    result.records_fetched = len(rows)

    if not rows:
        logger.info("No assessments found in experiment '%s'.", experiment_name)
        return result

    if pivot:
        df = _pivot_assessments(rows, assessment_mapping, labeler_column)
    else:
        df = pd.DataFrame(rows)

    result.new_records = len(df)

    if dry_run:
        logger.info("Dry run: would ingest %d assessment records.", len(df))
        return result

    push_source_name = _resolve_push_source_name(store, feature_view_name)
    df = _align_df_to_feature_view(store, feature_view_name, df)

    for start in range(0, len(df), batch_size):
        batch = df.iloc[start : start + batch_size]
        try:
            store.write_to_online_store(feature_view_name, batch)
        except Exception as e:
            result.errors.append(f"Online write error at offset {start}: {e}")
            logger.error("Failed to write batch to online store: %s", e)
            continue

        if push_source_name:
            try:
                store.push(push_source_name, batch, to=PushMode.ONLINE_AND_OFFLINE)
            except Exception as e:
                logger.warning(
                    "Push to offline store failed at offset %d: %s (continuing)",
                    start,
                    e,
                )

        result.records_ingested += len(batch)

    logger.info(
        "Assessment sync complete: fetched=%d, ingested=%d",
        result.records_fetched,
        result.records_ingested,
    )
    return result


def _pivot_assessments(
    rows: List[Dict],
    assessment_mapping: Optional[Dict[str, str]],
    labeler_column: str,
) -> pd.DataFrame:
    """Pivot flat assessment rows into one row per trace_id.

    Groups assessments by ``trace_id``, maps each ``assessment_name`` to a
    target column (via ``assessment_mapping`` or identity), and collapses
    ``source_id`` into the ``labeler_column``. Uses the latest
    ``event_timestamp`` across the group.
    """
    mapping = assessment_mapping or {}
    grouped: Dict[str, Dict] = {}

    for row in rows:
        trace_id = row["trace_id"]
        if trace_id not in grouped:
            grouped[trace_id] = {
                "trace_id": trace_id,
                "event_timestamp": row["event_timestamp"],
            }

        target = grouped[trace_id]

        col_name = mapping.get(row["assessment_name"], row["assessment_name"])
        target[col_name] = row["value"]

        if row.get("source_id"):
            target[labeler_column] = row["source_id"]

        if row["event_timestamp"] > target["event_timestamp"]:
            target["event_timestamp"] = row["event_timestamp"]

    return pd.DataFrame(list(grouped.values()))


def _get_trace_id_from_row(row) -> str:
    """Extract trace_id from a DataFrame row or Trace object."""
    if isinstance(row, pd.Series):
        tid = row.get("trace_id")
        if tid is not None:
            return str(tid)

    if hasattr(row, "info"):
        if hasattr(row.info, "trace_id"):
            return row.info.trace_id
        if hasattr(row.info, "request_id"):
            return row.info.request_id

    return str(getattr(row, "trace_id", "unknown"))


def _get_assessments_from_row(row) -> list:
    """Get assessments from a DataFrame row or Trace object."""
    if isinstance(row, pd.Series):
        assessments = row.get("assessments")
        if assessments is not None and isinstance(assessments, list):
            return assessments

    if hasattr(row, "info") and hasattr(row.info, "assessments"):
        return list(row.info.assessments)

    return []


def _assess_get(assessment, key):
    """Get a value from an assessment dict or object."""
    if isinstance(assessment, dict):
        return assessment.get(key)
    return getattr(assessment, key, None)


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


def _resolve_push_source_name(
    store: "FeatureStore", feature_view_name: str
) -> Optional[str]:
    """Resolve the PushSource name for a FeatureView or LabelView.

    ``store.push()`` expects the push source name, not the feature view name.
    This inspects the view's source/stream_source to find it.
    """
    view: Any = None
    try:
        view = store.get_feature_view(feature_view_name)
    except Exception:
        pass

    if view is None and hasattr(store, "get_label_view"):
        try:
            view = store.get_label_view(feature_view_name)
        except Exception:
            pass

    if view is None:
        return None

    for attr in ("stream_source", "source"):
        src = getattr(view, attr, None)
        if src is not None and hasattr(src, "name"):
            from feast.data_source import PushSource as _PS

            if isinstance(src, _PS):
                return src.name
    return None


def _set_last_sync_time(dataset) -> None:
    """Set the sync watermark tag on the MLflow dataset."""
    try:
        import mlflow.genai.datasets

        now = datetime.now(timezone.utc).isoformat()
        if hasattr(dataset, "dataset_id"):
            mlflow.genai.datasets.set_dataset_tags(
                dataset_id=dataset.dataset_id,
                tags={WATERMARK_TAG_KEY: now},
            )
    except Exception as e:
        logger.warning("Failed to set sync watermark: %s", e)
