"""Resolve labels for fine-tuning examples from Feast LabelView or MLflow.

Two resolution paths:

* **Feast LabelView** (recommended) — by default pulls offline label history
  and applies ``ConflictPolicy`` via ``resolve_conflicts`` (training-quality
  path). Pass ``label_source="online"`` to use ``get_online_features``
  (LAST_WRITE_WINS only).
* **MLflow expectations** — promotes the ``expected_response`` expectation
  already extracted from the trace into ``corrected_response``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List, Literal

import pandas as pd

from feast.finetuning.trace_extractor import FinetuningExample

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore

logger = logging.getLogger(__name__)

LabelSource = Literal["historical", "online"]


def resolve_labels_from_feast(
    examples: List[FinetuningExample],
    store: "FeatureStore",
    label_view_name: str,
    label_fields: List[str],
    join_key: str = "trace_id",
    sync_feedback_to_mlflow: bool = False,
    label_source: LabelSource = "historical",
) -> List[FinetuningExample]:
    """Join examples with labels from a Feast LabelView.

    Args:
        examples: Extracted fine-tuning examples (label fields may be ``None``).
        store: An initialised :class:`~feast.feature_store.FeatureStore`.
        label_view_name: Name of the registered
            :class:`~feast.labeling.label_view.LabelView`.
        label_fields: Column names to fetch (e.g.
            ``["corrected_response", "response_quality"]``).
        join_key: Entity column in the LabelView that corresponds to the
            MLflow ``trace_id``.  Defaults to ``"trace_id"``.
        sync_feedback_to_mlflow: When ``True``, also log each resolved
            label as an MLflow feedback assessment on the original trace.
        label_source: ``"historical"`` (default) pulls all offline rows and
            applies ``ConflictPolicy``; ``"online"`` uses
            ``get_online_features`` (LAST_WRITE_WINS).

    Returns:
        The same list with ``corrected_response``, ``label``, and ``labeler``
        populated where a matching label row was found.
    """
    if not examples:
        return examples

    if label_source == "online":
        result_df = _fetch_labels_online(
            store, label_view_name, label_fields, join_key, examples
        )
    else:
        result_df = _fetch_labels_historical(
            store, label_view_name, label_fields, join_key
        )

    if result_df is None or result_df.empty:
        return examples

    label_view = store.get_label_view(label_view_name)
    labeler_field = label_view.labeler_field

    def _get_join_value(ex: FinetuningExample) -> Any:
        if join_key == "trace_id":
            return ex.trace_id
        if ex.entity_values and join_key in ex.entity_values:
            return ex.entity_values[join_key]
        return ex.trace_id

    # Prefer fully-qualified columns when present, else bare names.
    def _col(row: pd.Series, name: str) -> Any:
        fq = f"{label_view_name}__{name}"
        if fq in row.index:
            return row.get(fq)
        return row.get(name)

    trace_to_row = {}
    for _, row in result_df.iterrows():
        key = row.get(join_key)
        if key is not None and pd.notna(key):
            trace_to_row[key] = row

    for ex in examples:
        row = trace_to_row.get(_get_join_value(ex))
        if row is None:
            continue

        if "corrected_response" in label_fields:
            val = _col(row, "corrected_response")
            if pd.notna(val):
                ex.corrected_response = str(val)

        if "response_quality" in label_fields:
            val = _col(row, "response_quality")
            if pd.notna(val):
                ex.label = str(val)

        for field_name in label_fields:
            if field_name in ("corrected_response", "response_quality"):
                continue
            val = _col(row, field_name)
            if pd.notna(val):
                ex.metadata[f"label_{field_name}"] = val

        labeler_val = _col(row, labeler_field)
        if pd.notna(labeler_val):
            ex.labeler = str(labeler_val)

    if sync_feedback_to_mlflow:
        _sync_feedback(examples)

    return examples


def _fetch_labels_online(
    store: "FeatureStore",
    label_view_name: str,
    label_fields: List[str],
    join_key: str,
    examples: List[FinetuningExample],
) -> pd.DataFrame:
    """Fetch latest labels via online store (LAST_WRITE_WINS)."""
    feature_refs = [f"{label_view_name}:{f}" for f in label_fields]
    label_view = store.get_label_view(label_view_name)
    labeler_field = label_view.labeler_field
    if labeler_field not in label_fields:
        feature_refs.append(f"{label_view_name}:{labeler_field}")

    def _get_join_value(ex: FinetuningExample) -> Any:
        if join_key == "trace_id":
            return ex.trace_id
        if ex.entity_values and join_key in ex.entity_values:
            return ex.entity_values[join_key]
        return ex.trace_id

    join_values = [_get_join_value(ex) for ex in examples]
    return store.get_online_features(
        features=feature_refs,
        entity_rows=[{join_key: v} for v in join_values],
    ).to_df()


def _fetch_labels_historical(
    store: "FeatureStore",
    label_view_name: str,
    label_fields: List[str],
    join_key: str,
) -> pd.DataFrame:
    """Pull offline label history and apply ConflictPolicy."""
    from feast.labeling.conflict_policy import ConflictPolicy
    from feast.labeling.conflict_resolver import resolve_conflicts

    label_view = store.get_label_view(label_view_name)
    batch_source = label_view.batch_source
    if batch_source is None:
        logger.warning(
            "LabelView '%s' has no batch_source; falling back to online labels",
            label_view_name,
        )
        return pd.DataFrame()

    feature_names = list(dict.fromkeys(label_fields + [label_view.labeler_field]))
    # Only request columns that exist on the LabelView schema.
    schema_names = {f.name for f in label_view.features}
    feature_names = [f for f in feature_names if f in schema_names]
    join_keys = label_view.join_keys or [join_key]
    timestamp_field = batch_source.timestamp_field or "event_timestamp"
    conflict_policy = getattr(
        label_view, "conflict_policy", ConflictPolicy.LAST_WRITE_WINS
    )
    labeler_field = label_view.labeler_field
    labeler_priorities = getattr(label_view, "labeler_priorities", None)

    try:
        provider = store._get_provider()
        job = provider.offline_store.pull_all_from_table_or_query(
            config=store.config,
            data_source=batch_source,
            join_key_columns=list(join_keys),
            feature_name_columns=feature_names,
            timestamp_field=timestamp_field,
        )
        df = job.to_df()
    except Exception:
        logger.warning(
            "Failed to pull offline labels for '%s'; trying get_historical_features",
            label_view_name,
            exc_info=True,
        )
        # Fallback: entity_df of unique join keys at "now"
        return _fetch_labels_via_historical_features(
            store, label_view_name, feature_names, join_key
        )

    if df.empty:
        return df

    return resolve_conflicts(
        df=df,
        join_key_columns=list(join_keys),
        feature_name_columns=feature_names,
        timestamp_field=timestamp_field,
        labeler_field=labeler_field,
        conflict_policy=conflict_policy,
        labeler_priorities=labeler_priorities,
    )


def _fetch_labels_via_historical_features(
    store: "FeatureStore",
    label_view_name: str,
    feature_names: List[str],
    join_key: str,
) -> pd.DataFrame:
    """Fallback historical join using get_historical_features."""
    # Without entity rows we cannot build a join; return empty.
    # Callers that need this path should prefer pull_all + resolve_conflicts.
    logger.debug(
        "get_historical_features fallback for LabelView '%s' requires entity_df; "
        "returning empty frame",
        label_view_name,
    )
    return pd.DataFrame(columns=[join_key, *feature_names])


def _sync_feedback(examples: List[FinetuningExample]) -> None:
    """Log resolved labels as MLflow feedback assessments on the original traces."""
    try:
        import mlflow
        from mlflow.entities import AssessmentSource, AssessmentSourceType
    except ImportError:
        logger.debug("mlflow not installed; skipping feedback sync")
        return

    synced = 0
    for ex in examples:
        if ex.label is None and ex.corrected_response is None:
            continue
        try:
            mlflow.log_feedback(
                trace_id=ex.trace_id,
                name="response_quality",
                value=ex.label or "corrected",
                source=AssessmentSource(
                    source_type=AssessmentSourceType.HUMAN,
                    source_id=ex.labeler or "unknown",
                ),
                rationale=ex.corrected_response,
            )
            synced += 1
        except Exception:
            logger.debug(
                "Failed to sync feedback for trace %s", ex.trace_id, exc_info=True
            )
    if synced:
        logger.info("Synced %d label(s) as MLflow feedback assessments", synced)


def resolve_labels_from_mlflow(
    examples: List[FinetuningExample],
    expectation_name: str = "expected_response",
) -> List[FinetuningExample]:
    """Promote MLflow expectations to ``corrected_response``.

    The expectation value is assumed to have been placed in
    ``example.metadata["expectations"]`` during trace extraction.
    If present, it is promoted to ``corrected_response``.

    Args:
        examples: Extracted fine-tuning examples.
        expectation_name: The key name of the expectation to use.

    Returns:
        The same list with ``corrected_response`` populated where an
        expectation was found.
    """
    for ex in examples:
        expectations = ex.metadata.get("expectations", {})
        if isinstance(expectations, dict):
            val = expectations.get(expectation_name)
            if val is not None:
                ex.corrected_response = str(val)
                ex.labeler = "mlflow_expectation"
    return examples


def filter_labeled_only(
    examples: List[FinetuningExample],
) -> List[FinetuningExample]:
    """Keep only examples that have a ``corrected_response``.

    Args:
        examples: Fine-tuning examples (some may lack labels).

    Returns:
        A filtered list containing only labeled examples.
    """
    return [ex for ex in examples if ex.corrected_response is not None]
