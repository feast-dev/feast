"""Assessment sync engine: pull MLflow trace assessments into Feast stores."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

from feast.feature_store import PushMode
from feast.mlflow_integration.config import (
    DEFAULT_BATCH_SIZE,
    resolve_tracking_uri,
)

if TYPE_CHECKING:
    from feast import FeatureStore

logger = logging.getLogger(__name__)


@dataclass
class SyncResult:
    """Outcome of a dataset sync operation."""

    records_fetched: int = 0
    records_ingested: int = 0
    new_records: int = 0
    updated_records: int = 0
    errors: List[str] = field(default_factory=list)


def _sync_defaults(store: "FeatureStore") -> int:
    """Read batch size from store config."""
    mlflow_cfg = getattr(store.config, "mlflow", None)
    sync_cfg = getattr(mlflow_cfg, "dataset_sync", None) if mlflow_cfg else None
    if sync_cfg is None:
        return DEFAULT_BATCH_SIZE
    return int(getattr(sync_cfg, "default_batch_size", DEFAULT_BATCH_SIZE))


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


def sync_trace_assessments_to_feast(
    store: "FeatureStore",
    experiment_name: str,
    feature_view_name: str,
    tracking_uri: Optional[str] = None,
    filter_string: Optional[str] = None,
    max_results: int = 1000,
    assessment_names: Optional[List[str]] = None,
    batch_size: Optional[int] = None,
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
    try:
        import mlflow
    except ImportError as e:
        raise ImportError(
            "The 'mlflow' package is required for assessment sync. "
            "Install it with: pip install 'feast[mlflow]'"
        ) from e

    result = SyncResult()
    default_batch = _sync_defaults(store)
    effective_batch = default_batch if batch_size is None else batch_size

    effective_uri = _resolve_tracking_uri(store, tracking_uri)
    if effective_uri:
        mlflow.set_tracking_uri(effective_uri)

    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        result.errors.append(f"MLflow experiment '{experiment_name}' not found")
        return result

    search_kwargs: Dict[str, Any] = {
        "experiment_ids": [experiment.experiment_id],
        "max_results": max_results,
    }
    if filter_string:
        search_kwargs["filter_string"] = filter_string

    # Prefer return_type="list" so Trace objects (with assessments) come back
    # in one RPC. Fall back to DataFrame / get_trace like trace_extractor.
    traces = _search_traces_for_assessments(mlflow, search_kwargs)

    rows: List[Dict] = []
    for trace_row in traces:
        trace_id = _get_trace_id_from_row(trace_row)
        assessments = _get_assessments_from_row(trace_row)
        if not assessments:
            continue

        for assessment in assessments:
            name = _assessment_name(assessment)
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


def _assessment_name(assessment) -> Optional[str]:
    """Return a concrete assessment name string, or None.

    Only accepts ``str`` values so auto-created mock attributes do not
    shadow a real ``name`` field.
    """
    for key in ("assessment_name", "name"):
        val = _assess_get(assessment, key)
        if isinstance(val, str) and val:
            return val
    return None


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


def _resolve_push_source_name(
    store: "FeatureStore", feature_view_name: str
) -> Optional[str]:
    """Resolve the PushSource name for a FeatureView or LabelView."""
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
    """Write a batch to the offline store via PushSource."""
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


def _search_traces_for_assessments(mlflow: Any, search_kwargs: Dict[str, Any]) -> list:
    """Fetch traces for assessment sync with bulk / list fallbacks.

    Mirrors ``feast.trace_export.trace_extractor._search_traces_bulk`` so MLflow
    3.x DataFrames without an ``assessments`` column still yield Trace objects.
    """
    try:
        result = mlflow.search_traces(**search_kwargs, return_type="list")
        if isinstance(result, list):
            return result
    except TypeError:
        logger.debug(
            "mlflow.search_traces(return_type='list') not supported; "
            "falling back to DataFrame path"
        )

    traces_df = mlflow.search_traces(**search_kwargs)
    if traces_df is None or getattr(traces_df, "empty", True):
        return []

    if isinstance(traces_df, list):
        return traces_df

    if "trace" in traces_df.columns:
        embedded = [t for t in traces_df["trace"].tolist() if t is not None]
        if embedded:
            return embedded

    if "assessments" in traces_df.columns:
        return [row for _, row in traces_df.iterrows()]

    traces: list = []
    for _, row in traces_df.iterrows():
        trace_id = row.get("trace_id") or row.get("request_id")
        if not trace_id:
            continue
        try:
            traces.append(mlflow.get_trace(str(trace_id)))
        except Exception:
            logger.warning("Failed to fetch trace %s", trace_id, exc_info=True)
    return traces
