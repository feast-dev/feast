"""Shared constants and helpers for monitoring across all offline store backends.

Every backend needs the same table names, column lists, primary keys,
empty-metric templates, and result-row normalization.  Centralizing them
here avoids ~8x duplication and prevents column-list drift.
"""

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

# ------------------------------------------------------------------ #
#  Table names
# ------------------------------------------------------------------ #

MON_TABLE_FEATURE = "feast_monitoring_feature_metrics"
MON_TABLE_FEATURE_VIEW = "feast_monitoring_feature_view_metrics"
MON_TABLE_FEATURE_SERVICE = "feast_monitoring_feature_service_metrics"

# ------------------------------------------------------------------ #
#  Column definitions — (ordered, used by INSERT / SELECT / Parquet)
# ------------------------------------------------------------------ #

FEATURE_METRICS_COLUMNS: List[str] = [
    "project_id",
    "feature_view_name",
    "feature_name",
    "metric_date",
    "granularity",
    "data_source_type",
    "computed_at",
    "is_baseline",
    "feature_type",
    "row_count",
    "null_count",
    "null_rate",
    "mean",
    "stddev",
    "min_val",
    "max_val",
    "p50",
    "p75",
    "p90",
    "p95",
    "p99",
    "histogram",
]

FEATURE_METRICS_PK: List[str] = [
    "project_id",
    "feature_view_name",
    "feature_name",
    "metric_date",
    "granularity",
    "data_source_type",
]

FEATURE_VIEW_METRICS_COLUMNS: List[str] = [
    "project_id",
    "feature_view_name",
    "metric_date",
    "granularity",
    "data_source_type",
    "computed_at",
    "is_baseline",
    "total_row_count",
    "total_features",
    "features_with_nulls",
    "avg_null_rate",
    "max_null_rate",
]

FEATURE_VIEW_METRICS_PK: List[str] = [
    "project_id",
    "feature_view_name",
    "metric_date",
    "granularity",
    "data_source_type",
]

FEATURE_SERVICE_METRICS_COLUMNS: List[str] = [
    "project_id",
    "feature_service_name",
    "metric_date",
    "granularity",
    "data_source_type",
    "computed_at",
    "is_baseline",
    "total_feature_views",
    "total_features",
    "avg_null_rate",
    "max_null_rate",
]

FEATURE_SERVICE_METRICS_PK: List[str] = [
    "project_id",
    "feature_service_name",
    "metric_date",
    "granularity",
    "data_source_type",
]


def monitoring_table_meta(
    metric_type: str,
) -> Tuple[str, List[str], List[str]]:
    """Return (table_name, columns, pk_columns) for a metric type.

    Raises ValueError for unknown metric types.
    """
    if metric_type == "feature":
        return MON_TABLE_FEATURE, FEATURE_METRICS_COLUMNS, FEATURE_METRICS_PK
    if metric_type == "feature_view":
        return (
            MON_TABLE_FEATURE_VIEW,
            FEATURE_VIEW_METRICS_COLUMNS,
            FEATURE_VIEW_METRICS_PK,
        )
    if metric_type == "feature_service":
        return (
            MON_TABLE_FEATURE_SERVICE,
            FEATURE_SERVICE_METRICS_COLUMNS,
            FEATURE_SERVICE_METRICS_PK,
        )
    raise ValueError(f"Unknown monitoring metric_type: '{metric_type}'")


# ------------------------------------------------------------------ #
#  Tiny helpers duplicated across backends
# ------------------------------------------------------------------ #


def opt_float(val: Any) -> Optional[float]:
    """Safely cast a value to float, returning None if input is None."""
    return float(val) if val is not None else None


def empty_numeric_metric(feature_name: str) -> Dict[str, Any]:
    """Return a metric dict with all-None stats for a numeric feature."""
    return {
        "feature_name": feature_name,
        "feature_type": "numeric",
        "row_count": 0,
        "null_count": 0,
        "null_rate": 0.0,
        "mean": None,
        "stddev": None,
        "min_val": None,
        "max_val": None,
        "p50": None,
        "p75": None,
        "p90": None,
        "p95": None,
        "p99": None,
        "histogram": None,
    }


def empty_categorical_metric(feature_name: str) -> Dict[str, Any]:
    """Return a metric dict with all-None stats for a categorical feature."""
    return {
        "feature_name": feature_name,
        "feature_type": "categorical",
        "row_count": 0,
        "null_count": 0,
        "null_rate": 0.0,
        "mean": None,
        "stddev": None,
        "min_val": None,
        "max_val": None,
        "p50": None,
        "p75": None,
        "p90": None,
        "p95": None,
        "p99": None,
        "histogram": None,
    }


# ------------------------------------------------------------------ #
#  Result-row normalization (used after SQL fetch or Parquet read)
# ------------------------------------------------------------------ #


def normalize_monitoring_row(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a monitoring metric dict for JSON serialization.

    - Parses ``histogram`` from JSON string if needed.
    - Converts ``metric_date`` / ``computed_at`` to ISO strings.
    - Normalizes ``is_baseline`` to Python bool.
    """
    hist = record.get("histogram")
    if isinstance(hist, str):
        try:
            record["histogram"] = json.loads(hist)
        except (json.JSONDecodeError, TypeError):
            pass

    for key in ("metric_date", "computed_at"):
        val = record.get(key)
        if isinstance(val, (date, datetime)):
            record[key] = val.isoformat()

    baseline = record.get("is_baseline")
    if baseline is not None:
        record["is_baseline"] = bool(baseline)

    return record


# ------------------------------------------------------------------ #
#  View-level aggregate builder (shared by batch + log save paths)
# ------------------------------------------------------------------ #


def build_view_aggregate(
    metrics_list: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compute view-level aggregate stats from per-feature metrics.

    Returns a dict with keys: total_row_count, total_features,
    features_with_nulls, avg_null_rate, max_null_rate.
    """
    null_rates = [
        m["null_rate"] for m in metrics_list if m.get("null_rate") is not None
    ]
    return {
        "total_row_count": metrics_list[0]["row_count"] if metrics_list else 0,
        "total_features": len(metrics_list),
        "features_with_nulls": sum(
            1 for m in metrics_list if (m.get("null_count") or 0) > 0
        ),
        "avg_null_rate": sum(null_rates) / len(null_rates) if null_rates else 0.0,
        "max_null_rate": max(null_rates) if null_rates else 0.0,
    }
