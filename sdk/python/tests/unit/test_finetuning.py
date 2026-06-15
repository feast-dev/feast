"""Unit tests for feast.finetuning package."""

from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# FinetuningExample dataclass
# ---------------------------------------------------------------------------


class TestFinetuningExample:
    def test_defaults(self):
        from feast.finetuning.trace_extractor import FinetuningExample

        ex = FinetuningExample(
            trace_id="tr-1",
            messages=[{"role": "user", "content": "hello"}],
        )
        assert ex.trace_id == "tr-1"
        assert ex.label is None
        assert ex.corrected_response is None
        assert ex.labeler is None
        assert ex.feature_refs == []
        assert ex.entity_values == {}


# ---------------------------------------------------------------------------
# Exporters
# ---------------------------------------------------------------------------


class TestOpenAIChatExporter:
    def test_exports_corrected_response(self):
        from feast.finetuning.exporters import OpenAIChatExporter
        from feast.finetuning.trace_extractor import FinetuningExample

        examples = [
            FinetuningExample(
                trace_id="tr-1",
                messages=[{"role": "user", "content": "What is 2+2?"}],
                original_completion="5",
                corrected_response="4",
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name

        try:
            exporter = OpenAIChatExporter()
            count = exporter.export(examples, path)
            assert count == 1

            with open(path) as fh:
                record = json.loads(fh.readline())
            assert record["messages"][-1]["role"] == "assistant"
            assert record["messages"][-1]["content"] == "4"
        finally:
            os.unlink(path)

    def test_skips_examples_without_completion(self):
        from feast.finetuning.exporters import OpenAIChatExporter
        from feast.finetuning.trace_extractor import FinetuningExample

        examples = [
            FinetuningExample(
                trace_id="tr-1",
                messages=[{"role": "user", "content": "hello"}],
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name

        try:
            exporter = OpenAIChatExporter()
            count = exporter.export(examples, path)
            assert count == 0
        finally:
            os.unlink(path)


class TestFeastEnrichedExporter:
    def test_includes_metadata(self):
        from feast.finetuning.exporters import FeastEnrichedExporter
        from feast.finetuning.trace_extractor import FinetuningExample

        examples = [
            FinetuningExample(
                trace_id="tr-1",
                messages=[{"role": "user", "content": "hello"}],
                original_completion="hi there",
                feature_refs=["driver_stats:conv_rate"],
                feature_views=["driver_stats"],
                entity_values={"driver_id": 1001},
                label="good",
                labeler="reviewer@co.com",
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name

        try:
            exporter = FeastEnrichedExporter()
            count = exporter.export(examples, path)
            assert count == 1

            with open(path) as fh:
                record = json.loads(fh.readline())
            assert "feast_metadata" in record
            meta = record["feast_metadata"]
            assert meta["trace_id"] == "tr-1"
            assert meta["feature_refs"] == ["driver_stats:conv_rate"]
            assert meta["label"] == "good"
        finally:
            os.unlink(path)


class TestGetExporter:
    def test_valid_formats(self):
        from feast.finetuning.exporters import get_exporter

        assert get_exporter("openai") is not None
        assert get_exporter("enriched") is not None

    def test_invalid_format(self):
        from feast.finetuning.exporters import get_exporter

        with pytest.raises(ValueError, match="Unknown export format"):
            get_exporter("invalid_format")


# ---------------------------------------------------------------------------
# Label resolver
# ---------------------------------------------------------------------------


class TestResolveLabelsMlflow:
    def test_promotes_expectations(self):
        from feast.finetuning.label_resolver import resolve_labels_from_mlflow
        from feast.finetuning.trace_extractor import FinetuningExample

        examples = [
            FinetuningExample(
                trace_id="tr-1",
                messages=[{"role": "user", "content": "q"}],
                metadata={"expectations": {"expected_response": "correct answer"}},
            )
        ]

        result = resolve_labels_from_mlflow(examples)
        assert result[0].corrected_response == "correct answer"
        assert result[0].labeler == "mlflow_expectation"

    def test_noop_when_no_expectations(self):
        from feast.finetuning.label_resolver import resolve_labels_from_mlflow
        from feast.finetuning.trace_extractor import FinetuningExample

        examples = [
            FinetuningExample(
                trace_id="tr-1",
                messages=[{"role": "user", "content": "q"}],
            )
        ]
        result = resolve_labels_from_mlflow(examples)
        assert result[0].corrected_response is None


class TestFilterLabeledOnly:
    def test_filters_correctly(self):
        from feast.finetuning.label_resolver import filter_labeled_only
        from feast.finetuning.trace_extractor import FinetuningExample

        examples = [
            FinetuningExample(
                trace_id="tr-1",
                messages=[],
                corrected_response="fixed",
            ),
            FinetuningExample(
                trace_id="tr-2",
                messages=[],
            ),
        ]
        result = filter_labeled_only(examples)
        assert len(result) == 1
        assert result[0].trace_id == "tr-1"


class TestSyncFeedback:
    def test_sync_calls_log_feedback(self):
        from feast.finetuning.label_resolver import _sync_feedback
        from feast.finetuning.trace_extractor import FinetuningExample

        examples = [
            FinetuningExample(
                trace_id="tr-1",
                messages=[],
                label="poor",
                corrected_response="better answer",
                labeler="reviewer@co.com",
            ),
            FinetuningExample(
                trace_id="tr-2",
                messages=[],
            ),
        ]

        mock_mlflow = MagicMock()
        mock_source_cls = MagicMock()
        mock_source_type = MagicMock()
        mock_source_type.HUMAN = "HUMAN"

        with patch.dict(
            "sys.modules",
            {
                "mlflow": mock_mlflow,
                "mlflow.entities": MagicMock(
                    AssessmentSource=mock_source_cls,
                    AssessmentSourceType=mock_source_type,
                ),
            },
        ):
            _sync_feedback(examples)

        assert mock_mlflow.log_feedback.call_count == 1
        call_kwargs = mock_mlflow.log_feedback.call_args[1]
        assert call_kwargs["trace_id"] == "tr-1"
        assert call_kwargs["value"] == "poor"


# ---------------------------------------------------------------------------
# Dataset registration
# ---------------------------------------------------------------------------


class TestRegisterInMlflow:
    def test_register_returns_run_id(self):
        from feast.finetuning.exporters import OpenAIChatExporter
        from feast.finetuning.trace_extractor import FinetuningExample

        examples = [
            FinetuningExample(
                trace_id="tr-1",
                messages=[{"role": "user", "content": "hello"}],
                original_completion="hi",
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            path = f.name

        try:
            exporter = OpenAIChatExporter()
            exporter.export(examples, path)

            mock_run = MagicMock()
            mock_run.info.run_id = "run-abc123"

            mock_mlflow = MagicMock()
            mock_mlflow.start_run.return_value.__enter__ = MagicMock(
                return_value=mock_run
            )
            mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

            with patch.dict("sys.modules", {"mlflow": mock_mlflow}):
                # Need to also patch pandas for the import inside register_in_mlflow
                run_id = exporter.register_in_mlflow(
                    output_path=path,
                    context="fine-tuning",
                    tags={"use_case": "red-teaming"},
                )

            assert run_id == "run-abc123"
            mock_mlflow.log_input.assert_called_once()
            mock_mlflow.log_artifact.assert_called_once_with(path)
            mock_mlflow.set_tag.assert_called_once_with("use_case", "red-teaming")
        finally:
            os.unlink(path)
