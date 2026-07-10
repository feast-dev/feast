"""Resolve labels for fine-tuning examples from Feast LabelView or MLflow.

Two resolution paths:

* **Feast LabelView** (recommended) — uses ``get_historical_features`` so
  the offline store's :class:`~feast.labeling.conflict_policy.ConflictPolicy`
  is applied automatically.
* **MLflow expectations** — promotes the ``expected_response`` expectation
  already extracted from the trace into ``corrected_response``.
"""

import logging
from typing import TYPE_CHECKING, Any, List

import pandas as pd

from feast.finetuning.trace_extractor import FinetuningExample

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore

logger = logging.getLogger(__name__)


def resolve_labels_from_feast(
    examples: List[FinetuningExample],
    store: "FeatureStore",
    label_view_name: str,
    label_fields: List[str],
    join_key: str = "trace_id",
    sync_feedback_to_mlflow: bool = False,
) -> List[FinetuningExample]:
    """Join examples with the latest labels from a Feast LabelView.

    Uses ``store.get_online_features`` to fetch the most recent label per
    entity key (LAST_WRITE_WINS). For conflict-resolved offline reads, use
    the UI ``/list-labels`` endpoint or call ``resolve_conflicts()`` directly.

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

    Returns:
        The same list with ``corrected_response``, ``label``, and ``labeler``
        populated where a matching label row was found.
    """
    if not examples:
        return examples

    def _get_join_value(ex: "FinetuningExample") -> Any:
        if join_key == "trace_id":
            return ex.trace_id
        if ex.entity_values and join_key in ex.entity_values:
            return ex.entity_values[join_key]
        return ex.trace_id

    feature_refs = [f"{label_view_name}:{f}" for f in label_fields]

    label_view = store.get_label_view(label_view_name)
    labeler_field = label_view.labeler_field
    if labeler_field not in label_fields:
        feature_refs.append(f"{label_view_name}:{labeler_field}")

    join_values = [_get_join_value(ex) for ex in examples]

    result_df = store.get_online_features(
        features=feature_refs,
        entity_rows=[{join_key: v} for v in join_values],
    ).to_df()

    trace_to_row = {row[join_key]: row for _, row in result_df.iterrows()}

    for ex in examples:
        row = trace_to_row.get(_get_join_value(ex))
        if row is None:
            continue

        if "corrected_response" in label_fields:
            val = row.get("corrected_response")
            if pd.notna(val):
                ex.corrected_response = str(val)

        if "response_quality" in label_fields:
            val = row.get("response_quality")
            if pd.notna(val):
                ex.label = str(val)

        for field_name in label_fields:
            if field_name in ("corrected_response", "response_quality"):
                continue
            val = row.get(field_name)
            if pd.notna(val):
                ex.metadata[f"label_{field_name}"] = val

        labeler_val = row.get(labeler_field)
        if pd.notna(labeler_val):
            ex.labeler = str(labeler_val)

    if sync_feedback_to_mlflow:
        _sync_feedback(examples)

    return examples


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
