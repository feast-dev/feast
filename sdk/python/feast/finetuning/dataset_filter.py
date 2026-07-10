"""Filter fine-tuning examples by MLflow dataset membership.

Pipeline 3: Only export traces that a human curated into a named
MLflow dataset (via the "Add to dataset" UI or programmatic API).
"""

import logging
from typing import List

from feast.finetuning.trace_extractor import FinetuningExample

logger = logging.getLogger(__name__)


def filter_by_mlflow_dataset(
    examples: List[FinetuningExample],
    dataset_name: str,
    tracking_uri: str,
) -> List[FinetuningExample]:
    """Keep only examples whose trace_id is in the named MLflow dataset.

    Uses ``mlflow.genai.datasets.get_dataset()`` to retrieve dataset records,
    extracts trace_ids from record sources, and filters the examples list.

    Args:
        examples: Extracted fine-tuning examples to filter.
        dataset_name: Name of the MLflow dataset (e.g. ``"finetuning"``).
        tracking_uri: MLflow tracking server URI.

    Returns:
        Filtered list containing only examples whose trace_id appears
        in the dataset's records.
    """
    try:
        import mlflow
        from mlflow.genai.datasets import get_dataset
    except ImportError:
        raise ImportError(
            "The 'mlflow' package (>= 3.0) is required for dataset filtering. "
            "Install it with: pip install 'mlflow>=3.0'"
        )

    mlflow.set_tracking_uri(tracking_uri)
    try:
        dataset = get_dataset(name=dataset_name)
    except Exception as e:
        if "Multiple datasets" in str(e):
            client = mlflow.tracking.MlflowClient()
            matches = [d for d in client.search_datasets() if d.name == dataset_name]
            if not matches:
                raise
            dataset = get_dataset(dataset_id=matches[0].dataset_id)
            logger.info("Multiple datasets named '%s'; using most recent: %s", dataset_name, dataset.dataset_id)
        else:
            raise
    df = dataset.to_df()

    dataset_trace_ids: set = set()
    for _, row in df.iterrows():
        # source_id column directly contains the trace_id for TRACE-sourced records
        source_id = row.get("source_id")
        if source_id:
            dataset_trace_ids.add(str(source_id))
            continue

        # Fallback: parse the source object/dict
        source = row.get("source")
        if hasattr(source, "source_data"):
            source_data = source.source_data
            if isinstance(source_data, dict):
                trace_id = source_data.get("trace_id")
                if trace_id:
                    dataset_trace_ids.add(str(trace_id))
        elif isinstance(source, dict):
            source_data = source.get("source_data", {})
            if isinstance(source_data, dict):
                trace_id = source_data.get("trace_id")
                if trace_id:
                    dataset_trace_ids.add(str(trace_id))

    filtered = [ex for ex in examples if ex.trace_id in dataset_trace_ids]
    logger.info(
        "Dataset '%s' has %d trace(s); matched %d/%d examples",
        dataset_name,
        len(dataset_trace_ids),
        len(filtered),
        len(examples),
    )
    return filtered
