"""Pluggable fine-tuning dataset exporters.

Each exporter takes a list of :class:`~feast.finetuning.trace_extractor.FinetuningExample`
objects and writes them to a file in a specific format.  New formats are added
by subclassing :class:`BaseExporter` and registering in :data:`EXPORTERS`.
"""

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Type

from feast.finetuning.trace_extractor import FinetuningExample

_logger = logging.getLogger(__name__)


class BaseExporter(ABC):
    """Abstract base for fine-tuning dataset exporters."""

    @abstractmethod
    def export(self, examples: List[FinetuningExample], output_path: str) -> int:
        """Write *examples* to *output_path*.

        Returns:
            The number of examples successfully written.
        """

    def register_in_mlflow(
        self,
        output_path: str,
        experiment_name: Optional[str] = None,
        run_id: Optional[str] = None,
        context: str = "fine-tuning",
        tags: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """Save the exported JSONL as an MLflow artifact.

        Args:
            output_path: Path to the exported JSONL file.
            experiment_name: MLflow experiment to log to. If *run_id* is
                provided this is ignored. Otherwise, sets the active
                experiment before creating a new run.
            run_id: Attach the artifact to an existing run instead of
                creating a new one. Takes precedence over *experiment_name*.
            context: MLflow dataset context (e.g. ``"fine-tuning"``,
                ``"red-teaming"``, ``"evaluation"``).
            tags: Optional key-value tags to set on the MLflow run.

        Returns:
            The MLflow run ID, or ``None`` if registration failed.
        """
        try:
            import mlflow

            if run_id:
                with mlflow.start_run(run_id=run_id) as run:
                    mlflow.log_artifact(output_path)
                    mlflow.set_tag("feast.export_context", context)
                    mlflow.set_tag("feast.export_format", self.__class__.__name__)
                    if tags:
                        for k, v in tags.items():
                            mlflow.set_tag(k, v)
                    return run.info.run_id
            else:
                if experiment_name:
                    mlflow.set_experiment(experiment_name)
                with mlflow.start_run(
                    run_name=f"feast-export-{Path(output_path).stem}"
                ) as run:
                    mlflow.log_artifact(output_path)
                    mlflow.set_tag("feast.export_context", context)
                    mlflow.set_tag("feast.export_format", self.__class__.__name__)
                    if tags:
                        for k, v in tags.items():
                            mlflow.set_tag(k, v)
                    return run.info.run_id
        except Exception:
            _logger.warning("Failed to register artifact in MLflow", exc_info=True)
            return None


class OpenAIChatExporter(BaseExporter):
    """Export to OpenAI chat fine-tuning JSONL format.

    Each line is a JSON object with a ``messages`` array::

        {"messages": [
            {"role": "system", "content": "..."},
            {"role": "user", "content": "..."},
            {"role": "assistant", "content": "<corrected or original>"}
        ]}

    The assistant message uses ``corrected_response`` if available,
    otherwise ``original_completion``.  Examples with neither are skipped.
    """

    def export(self, examples: List[FinetuningExample], output_path: str) -> int:
        examples = sorted(examples, key=lambda ex: ex.trace_id)
        count = 0
        with open(output_path, "w", encoding="utf-8") as fh:
            for ex in examples:
                assistant_content = ex.corrected_response or ex.original_completion
                if not assistant_content:
                    continue

                messages = _build_messages(ex, assistant_content)
                line = json.dumps({"messages": messages}, ensure_ascii=False)
                fh.write(line + "\n")
                count += 1
        return count


class FeastEnrichedExporter(BaseExporter):
    """Export OpenAI chat JSONL enriched with Feast metadata.

    Same ``messages`` array as :class:`OpenAIChatExporter` plus a
    ``feast_metadata`` object with trace provenance, feature references,
    entity values, and label information::

        {"messages": [...], "feast_metadata": {
            "trace_id": "tr-abc123",
            "feature_refs": ["driver_hourly_stats:conv_rate"],
            "entity_values": {"driver_id": 1001},
            "label": "poor",
            "labeler": "human-reviewer@co.com",
            "original_completion": "Driver 1001 has demonstrated..."
        }}
    """

    def export(self, examples: List[FinetuningExample], output_path: str) -> int:
        examples = sorted(examples, key=lambda ex: ex.trace_id)
        count = 0
        with open(output_path, "w", encoding="utf-8") as fh:
            for ex in examples:
                assistant_content = ex.corrected_response or ex.original_completion
                if not assistant_content:
                    continue

                messages = _build_messages(ex, assistant_content)

                feast_metadata: Dict = {
                    "trace_id": ex.trace_id,
                    "feature_refs": ex.feature_refs,
                    "feature_views": ex.feature_views,
                    "entity_values": ex.entity_values,
                }
                if ex.label is not None:
                    feast_metadata["label"] = ex.label
                if ex.labeler is not None:
                    feast_metadata["labeler"] = ex.labeler
                if ex.original_completion is not None:
                    feast_metadata["original_completion"] = ex.original_completion

                record = {
                    "messages": messages,
                    "feast_metadata": feast_metadata,
                }
                line = json.dumps(record, ensure_ascii=False)
                fh.write(line + "\n")
                count += 1
        return count


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

EXPORTERS: Dict[str, Type[BaseExporter]] = {
    "openai": OpenAIChatExporter,
    "enriched": FeastEnrichedExporter,
}


def get_exporter(format_name: str) -> BaseExporter:
    """Return an exporter instance for *format_name*.

    Args:
        format_name: One of the keys in :data:`EXPORTERS`
            (``"openai"`` or ``"enriched"``).

    Raises:
        ValueError: If *format_name* is not recognised.
    """
    cls = EXPORTERS.get(format_name)
    if cls is None:
        valid = ", ".join(sorted(EXPORTERS))
        raise ValueError(f"Unknown export format {format_name!r}. Choose from: {valid}")
    return cls()


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _build_messages(
    ex: FinetuningExample, assistant_content: str
) -> List[Dict[str, str]]:
    """Build the messages array, replacing the last assistant turn."""
    messages: List[Dict[str, str]] = []

    for msg in ex.messages:
        if msg.get("role") == "assistant":
            continue
        messages.append(msg)

    messages.append({"role": "assistant", "content": assistant_content})
    return messages
