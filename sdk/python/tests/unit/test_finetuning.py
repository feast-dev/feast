"""Unit tests for feast.finetuning package."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
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
            mock_mlflow.log_artifact.assert_called_once_with(path)
            mock_mlflow.set_tag.assert_any_call("feast.export_context", "fine-tuning")
            mock_mlflow.set_tag.assert_any_call("use_case", "red-teaming")
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Dataset sync: flatten_mlflow_dataset_df
# ---------------------------------------------------------------------------


class TestFlattenMlflowDatasetDf:
    def _make_df(self, records=None):
        if records is None:
            records = [
                {
                    "dataset_record_id": "rec-1",
                    "inputs": {"question": "What is MLflow?", "context": "docs"},
                    "expectations": {
                        "expected_response": "MLflow is...",
                        "guidelines": "Be concise",
                    },
                    "source": {"trace": {"trace_id": "tr-xyz789"}},
                    "tags": {"priority": "high", "reviewer": "jane"},
                    "last_update_time": "2026-06-15T12:00:00Z",
                },
                {
                    "dataset_record_id": "rec-2",
                    "inputs": {"question": "What is Feast?"},
                    "expectations": {"expected_response": "Feast is..."},
                    "source": {"trace": {"trace_id": "tr-abc123"}},
                    "tags": {"priority": "low"},
                    "last_update_time": "2026-06-16T10:00:00Z",
                },
            ]
        return pd.DataFrame(records)

    def test_basic_flattening(self):
        from feast.mlflow_integration.dataset_sync import flatten_mlflow_dataset_df

        df = self._make_df()
        result = flatten_mlflow_dataset_df(df)

        assert "dataset_record_id" in result.columns
        assert "input_question" in result.columns
        assert "input_context" in result.columns
        assert "expected_response" in result.columns
        assert "guidelines" in result.columns
        assert "trace_id" in result.columns
        assert "tag_priority" in result.columns
        assert "tag_reviewer" in result.columns
        assert "event_timestamp" in result.columns

        assert result.iloc[0]["dataset_record_id"] == "rec-1"
        assert result.iloc[0]["input_question"] == "What is MLflow?"
        assert result.iloc[0]["expected_response"] == "MLflow is..."
        assert result.iloc[0]["trace_id"] == "tr-xyz789"
        assert result.iloc[0]["tag_priority"] == "high"

    def test_missing_optional_columns(self):
        from feast.mlflow_integration.dataset_sync import flatten_mlflow_dataset_df

        df = pd.DataFrame(
            [
                {
                    "dataset_record_id": "rec-1",
                    "inputs": {"question": "Q1"},
                    "last_update_time": "2026-06-15T12:00:00Z",
                }
            ]
        )
        result = flatten_mlflow_dataset_df(df)

        assert "dataset_record_id" in result.columns
        assert "input_question" in result.columns
        assert "event_timestamp" in result.columns
        assert "trace_id" not in result.columns

    def test_null_dict_values(self):
        from feast.mlflow_integration.dataset_sync import flatten_mlflow_dataset_df

        df = pd.DataFrame(
            [
                {
                    "dataset_record_id": "rec-1",
                    "inputs": None,
                    "expectations": {"expected_response": "answer"},
                    "source": None,
                    "tags": None,
                    "last_update_time": "2026-06-15T12:00:00Z",
                }
            ]
        )
        result = flatten_mlflow_dataset_df(df)

        assert result.iloc[0]["expected_response"] == "answer"
        assert result.iloc[0]["trace_id"] is None

    def test_field_mapping_renames(self):
        from feast.mlflow_integration.dataset_sync import flatten_mlflow_dataset_df

        df = self._make_df()
        mapping = {"expectations.expected_response": "corrected_response"}
        result = flatten_mlflow_dataset_df(df, field_mapping=mapping)

        assert "corrected_response" in result.columns
        assert "expected_response" not in result.columns
        assert result.iloc[0]["corrected_response"] == "MLflow is..."

    def test_field_mapping_extracts_nested(self):
        from feast.mlflow_integration.dataset_sync import flatten_mlflow_dataset_df

        df = self._make_df()
        mapping = {"source.trace.trace_id": "mlflow_trace_id"}
        result = flatten_mlflow_dataset_df(df, field_mapping=mapping)

        assert "mlflow_trace_id" in result.columns
        assert result.iloc[0]["mlflow_trace_id"] == "tr-xyz789"

    def test_event_timestamp_is_parsed(self):
        from feast.mlflow_integration.dataset_sync import flatten_mlflow_dataset_df

        df = self._make_df()
        result = flatten_mlflow_dataset_df(df)

        ts = result.iloc[0]["event_timestamp"]
        assert isinstance(ts, datetime)
        assert ts.year == 2026

    def test_fallback_to_create_time(self):
        from feast.mlflow_integration.dataset_sync import flatten_mlflow_dataset_df

        df = pd.DataFrame(
            [
                {
                    "dataset_record_id": "rec-1",
                    "inputs": {"q": "hello"},
                    "create_time": "2026-01-01T00:00:00Z",
                }
            ]
        )
        result = flatten_mlflow_dataset_df(df)
        ts = result.iloc[0]["event_timestamp"]
        assert isinstance(ts, datetime)

    def test_empty_dataframe(self):
        from feast.mlflow_integration.dataset_sync import flatten_mlflow_dataset_df

        df = pd.DataFrame(columns=["dataset_record_id", "inputs", "last_update_time"])
        result = flatten_mlflow_dataset_df(df)
        assert len(result) == 0

    def test_second_record_missing_keys(self):
        """When different records have different nested keys, missing ones get None."""
        from feast.mlflow_integration.dataset_sync import flatten_mlflow_dataset_df

        df = self._make_df()
        result = flatten_mlflow_dataset_df(df)

        assert result.iloc[1]["input_context"] is None
        assert result.iloc[1]["tag_reviewer"] is None


# ---------------------------------------------------------------------------
# Dataset sync: sync_mlflow_dataset_to_feast
# ---------------------------------------------------------------------------


class TestSyncMlflowDatasetToFeast:
    def _make_mock_dataset(self, records):
        dataset = MagicMock()
        dataset.to_df.return_value = pd.DataFrame(records)
        dataset.tags = {}
        dataset.dataset_id = "ds-123"
        return dataset

    def _make_mock_store(self):
        store = MagicMock()
        store.config = MagicMock()
        store.config.mlflow = None
        return store

    @patch("feast.mlflow_integration.dataset_sync._set_last_sync_time")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_push_source_name",
        return_value="test_push_source",
    )
    @patch("feast.mlflow_integration.dataset_sync._fetch_dataset_with_retry")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri", return_value=None
    )
    def test_sync_full_refresh(
        self, mock_uri, mock_fetch, mock_push_src, mock_set_sync
    ):
        from feast.mlflow_integration.dataset_sync import sync_mlflow_dataset_to_feast

        records = [
            {
                "dataset_record_id": "rec-1",
                "inputs": {"question": "Q1"},
                "expectations": {"expected_response": "A1"},
                "source": {"trace": {"trace_id": "tr-1"}},
                "tags": {},
                "last_update_time": "2026-06-15T12:00:00Z",
            }
        ]
        mock_fetch.return_value = self._make_mock_dataset(records)
        store = self._make_mock_store()

        result = sync_mlflow_dataset_to_feast(
            store=store,
            dataset_name="test-dataset",
            feature_view_name="test_fv",
            incremental=False,
        )

        assert result.records_fetched == 1
        assert result.records_ingested == 1
        assert result.errors == []
        store.write_to_online_store.assert_called_once()
        store.push.assert_called_once()

    @patch("feast.mlflow_integration.dataset_sync._set_last_sync_time")
    @patch("feast.mlflow_integration.dataset_sync._fetch_dataset_with_retry")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri", return_value=None
    )
    def test_sync_incremental_filters_old_records(
        self, mock_uri, mock_fetch, mock_set_sync
    ):
        from feast.mlflow_integration.dataset_sync import sync_mlflow_dataset_to_feast

        records = [
            {
                "dataset_record_id": "rec-1",
                "inputs": {"question": "Q1"},
                "expectations": {},
                "source": {},
                "tags": {},
                "last_update_time": "2026-06-10T12:00:00Z",
            },
            {
                "dataset_record_id": "rec-2",
                "inputs": {"question": "Q2"},
                "expectations": {},
                "source": {},
                "tags": {},
                "last_update_time": "2026-06-20T12:00:00Z",
            },
        ]
        dataset = self._make_mock_dataset(records)
        dataset.tags = {"feast_last_sync_time": "2026-06-15T00:00:00+00:00"}
        mock_fetch.return_value = dataset
        store = self._make_mock_store()

        result = sync_mlflow_dataset_to_feast(
            store=store,
            dataset_name="test-dataset",
            feature_view_name="test_fv",
            incremental=True,
        )

        assert result.records_fetched == 2
        assert result.records_ingested == 1
        assert result.new_records == 1

    @patch("feast.mlflow_integration.dataset_sync._fetch_dataset_with_retry")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri", return_value=None
    )
    def test_sync_dry_run_does_not_write(self, mock_uri, mock_fetch):
        from feast.mlflow_integration.dataset_sync import sync_mlflow_dataset_to_feast

        records = [
            {
                "dataset_record_id": "rec-1",
                "inputs": {"question": "Q1"},
                "expectations": {},
                "source": {},
                "tags": {},
                "last_update_time": "2026-06-15T12:00:00Z",
            }
        ]
        mock_fetch.return_value = self._make_mock_dataset(records)
        store = self._make_mock_store()

        result = sync_mlflow_dataset_to_feast(
            store=store,
            dataset_name="test-dataset",
            feature_view_name="test_fv",
            dry_run=True,
        )

        assert result.records_fetched == 1
        assert result.records_ingested == 0
        store.write_to_online_store.assert_not_called()
        store.push.assert_not_called()

    @patch("feast.mlflow_integration.dataset_sync._fetch_dataset_with_retry")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri", return_value=None
    )
    def test_sync_handles_fetch_failure(self, mock_uri, mock_fetch):
        from feast.mlflow_integration.dataset_sync import sync_mlflow_dataset_to_feast

        mock_fetch.return_value = None
        store = self._make_mock_store()

        result = sync_mlflow_dataset_to_feast(
            store=store,
            dataset_name="missing-dataset",
            feature_view_name="test_fv",
        )

        assert result.records_fetched == 0
        assert len(result.errors) == 1
        assert "Failed to fetch" in result.errors[0]

    @patch("feast.mlflow_integration.dataset_sync._set_last_sync_time")
    @patch("feast.mlflow_integration.dataset_sync._fetch_dataset_with_retry")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri", return_value=None
    )
    def test_sync_handles_write_error(self, mock_uri, mock_fetch, mock_set_sync):
        from feast.mlflow_integration.dataset_sync import sync_mlflow_dataset_to_feast

        records = [
            {
                "dataset_record_id": "rec-1",
                "inputs": {"question": "Q1"},
                "expectations": {},
                "source": {},
                "tags": {},
                "last_update_time": "2026-06-15T12:00:00Z",
            }
        ]
        mock_fetch.return_value = self._make_mock_dataset(records)
        store = self._make_mock_store()
        store.write_to_online_store.side_effect = RuntimeError("Connection failed")

        result = sync_mlflow_dataset_to_feast(
            store=store,
            dataset_name="test-dataset",
            feature_view_name="test_fv",
            incremental=False,
        )

        assert result.records_ingested == 0
        assert len(result.errors) == 1
        assert "Connection failed" in result.errors[0]

    @patch("feast.mlflow_integration.dataset_sync._set_last_sync_time")
    @patch("feast.mlflow_integration.dataset_sync._fetch_dataset_with_retry")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri", return_value=None
    )
    def test_sync_with_field_mapping(self, mock_uri, mock_fetch, mock_set_sync):
        from feast.mlflow_integration.dataset_sync import sync_mlflow_dataset_to_feast

        records = [
            {
                "dataset_record_id": "rec-1",
                "inputs": {"question": "Q1"},
                "expectations": {"expected_response": "A1"},
                "source": {"trace": {"trace_id": "tr-1"}},
                "tags": {},
                "last_update_time": "2026-06-15T12:00:00Z",
            }
        ]
        mock_fetch.return_value = self._make_mock_dataset(records)
        store = self._make_mock_store()

        result = sync_mlflow_dataset_to_feast(
            store=store,
            dataset_name="test-dataset",
            feature_view_name="test_fv",
            field_mapping={"expectations.expected_response": "corrected_response"},
            incremental=False,
        )

        assert result.records_ingested == 1
        call_args = store.write_to_online_store.call_args
        written_df = call_args[0][1]
        assert "corrected_response" in written_df.columns


# ---------------------------------------------------------------------------
# DatasetSyncConfig
# ---------------------------------------------------------------------------


class TestDatasetSyncConfig:
    def test_default_values(self):
        from feast.mlflow_integration.config import DatasetSyncConfig

        cfg = DatasetSyncConfig()
        assert cfg.default_field_mapping == {}
        assert cfg.watermark_key == "feast_last_sync_time"
        assert cfg.default_batch_size == 10_000

    def test_custom_values(self):
        from feast.mlflow_integration.config import DatasetSyncConfig

        cfg = DatasetSyncConfig(
            default_field_mapping={"expectations.response": "answer"},
            watermark_key="custom_watermark",
            default_batch_size=5000,
        )
        assert cfg.default_field_mapping == {"expectations.response": "answer"}
        assert cfg.watermark_key == "custom_watermark"
        assert cfg.default_batch_size == 5000

    def test_mlflow_config_includes_dataset_sync(self):
        from feast.mlflow_integration.config import DatasetSyncConfig, MlflowConfig

        cfg = MlflowConfig(enabled=True)
        assert isinstance(cfg.dataset_sync, DatasetSyncConfig)
        assert cfg.dataset_sync.watermark_key == "feast_last_sync_time"


# ---------------------------------------------------------------------------
# Pivot assessments
# ---------------------------------------------------------------------------


class TestPivotAssessments:
    def test_basic_pivot(self):
        from feast.mlflow_integration.dataset_sync import _pivot_assessments

        ts = datetime(2026, 7, 10, 12, 0, 0)
        rows = [
            {
                "trace_id": "tr-1",
                "assessment_name": "expected_response",
                "assessment_type": "expectation",
                "value": "The correct answer",
                "source_id": "reviewer_alice",
                "rationale": "",
                "event_timestamp": ts,
            },
            {
                "trace_id": "tr-1",
                "assessment_name": "response_quality",
                "assessment_type": "feedback",
                "value": "good",
                "source_id": "reviewer_alice",
                "rationale": "Well structured",
                "event_timestamp": ts,
            },
        ]

        df = _pivot_assessments(rows, assessment_mapping=None, labeler_column="labeler")

        assert len(df) == 1
        row = df.iloc[0]
        assert row["trace_id"] == "tr-1"
        assert row["expected_response"] == "The correct answer"
        assert row["response_quality"] == "good"
        assert row["labeler"] == "reviewer_alice"

    def test_pivot_with_mapping(self):
        from feast.mlflow_integration.dataset_sync import _pivot_assessments

        ts = datetime(2026, 7, 10, 12, 0, 0)
        rows = [
            {
                "trace_id": "tr-1",
                "assessment_name": "expected_response",
                "assessment_type": "expectation",
                "value": "Fixed answer",
                "source_id": "gpt-4-judge",
                "rationale": "",
                "event_timestamp": ts,
            },
        ]

        mapping = {"expected_response": "corrected_response"}
        df = _pivot_assessments(
            rows, assessment_mapping=mapping, labeler_column="labeler"
        )

        assert len(df) == 1
        assert "corrected_response" in df.columns
        assert "expected_response" not in df.columns
        assert df.iloc[0]["corrected_response"] == "Fixed answer"
        assert df.iloc[0]["labeler"] == "gpt-4-judge"

    def test_pivot_multiple_traces(self):
        from feast.mlflow_integration.dataset_sync import _pivot_assessments

        ts1 = datetime(2026, 7, 10, 12, 0, 0)
        ts2 = datetime(2026, 7, 10, 13, 0, 0)
        rows = [
            {
                "trace_id": "tr-1",
                "assessment_name": "response_quality",
                "assessment_type": "feedback",
                "value": "good",
                "source_id": "alice",
                "rationale": "",
                "event_timestamp": ts1,
            },
            {
                "trace_id": "tr-2",
                "assessment_name": "response_quality",
                "assessment_type": "feedback",
                "value": "poor",
                "source_id": "bob",
                "rationale": "",
                "event_timestamp": ts2,
            },
        ]

        df = _pivot_assessments(rows, assessment_mapping=None, labeler_column="labeler")

        assert len(df) == 2
        tr1 = df[df["trace_id"] == "tr-1"].iloc[0]
        tr2 = df[df["trace_id"] == "tr-2"].iloc[0]
        assert tr1["response_quality"] == "good"
        assert tr1["labeler"] == "alice"
        assert tr2["response_quality"] == "poor"
        assert tr2["labeler"] == "bob"

    def test_pivot_uses_latest_timestamp(self):
        from feast.mlflow_integration.dataset_sync import _pivot_assessments

        ts_early = datetime(2026, 7, 10, 10, 0, 0)
        ts_late = datetime(2026, 7, 10, 14, 0, 0)
        rows = [
            {
                "trace_id": "tr-1",
                "assessment_name": "quality",
                "assessment_type": "feedback",
                "value": "good",
                "source_id": "alice",
                "rationale": "",
                "event_timestamp": ts_early,
            },
            {
                "trace_id": "tr-1",
                "assessment_name": "accuracy",
                "assessment_type": "feedback",
                "value": "high",
                "source_id": "alice",
                "rationale": "",
                "event_timestamp": ts_late,
            },
        ]

        df = _pivot_assessments(rows, assessment_mapping=None, labeler_column="labeler")
        assert df.iloc[0]["event_timestamp"] == ts_late

    def test_pivot_empty_source_id_skips_labeler(self):
        from feast.mlflow_integration.dataset_sync import _pivot_assessments

        ts = datetime(2026, 7, 10, 12, 0, 0)
        rows = [
            {
                "trace_id": "tr-1",
                "assessment_name": "quality",
                "assessment_type": "feedback",
                "value": "good",
                "source_id": "",
                "rationale": "",
                "event_timestamp": ts,
            },
        ]

        df = _pivot_assessments(rows, assessment_mapping=None, labeler_column="labeler")
        assert "labeler" not in df.columns or df.iloc[0].get("labeler", "") == ""


# ---------------------------------------------------------------------------
# Assessment extraction from traces
# ---------------------------------------------------------------------------


class TestExtractAssessments:
    def _make_mock_assessment(
        self,
        name,
        kind="expectation",
        value="test_value",
        source_id="reviewer",
        rationale=None,
    ):
        assessment = MagicMock()
        assessment.name = name

        if kind == "expectation":
            exp = MagicMock()
            exp.value = value
            assessment.expectation = exp
            assessment.feedback = None
        else:
            fb = MagicMock()
            fb.value = value
            assessment.feedback = fb
            assessment.expectation = None

        source = MagicMock()
        source.source_id = source_id
        assessment.source = source
        assessment.rationale = rationale
        return assessment

    def test_extract_expectations(self):
        from feast.finetuning.trace_extractor import _extract_assessments

        trace = MagicMock()
        trace.info.assessments = [
            self._make_mock_assessment(
                "expected_response", "expectation", "Correct answer"
            ),
        ]

        result = _extract_assessments(trace, kind="expectation")
        assert "expected_response" in result
        assert result["expected_response"] == "Correct answer"

    def test_extract_feedback(self):
        from feast.finetuning.trace_extractor import _extract_assessments

        trace = MagicMock()
        trace.info.assessments = [
            self._make_mock_assessment(
                "response_quality",
                "feedback",
                "good",
                source_id="gpt-4-judge",
                rationale="Well written",
            ),
        ]

        result = _extract_assessments(trace, kind="feedback")
        assert "response_quality" in result
        assert result["response_quality"]["value"] == "good"
        assert result["response_quality"]["source_id"] == "gpt-4-judge"
        assert result["response_quality"]["rationale"] == "Well written"

    def test_extract_llm_judge_feedback(self):
        """LLM-as-a-judge feedback is extracted the same as human feedback."""
        from feast.finetuning.trace_extractor import _extract_assessments

        trace = MagicMock()
        trace.info.assessments = [
            self._make_mock_assessment(
                "response_quality", "feedback", "excellent", source_id="gpt-4-judge"
            ),
            self._make_mock_assessment(
                "safety_check", "feedback", "pass", source_id="safety-scanner-v2"
            ),
        ]

        result = _extract_assessments(trace, kind="feedback")
        assert len(result) == 2
        assert result["response_quality"]["value"] == "excellent"
        assert result["response_quality"]["source_id"] == "gpt-4-judge"
        assert result["safety_check"]["value"] == "pass"
        assert result["safety_check"]["source_id"] == "safety-scanner-v2"

    def test_extract_no_assessments(self):
        from feast.finetuning.trace_extractor import _extract_assessments

        trace = MagicMock()
        trace.info.assessments = []

        result = _extract_assessments(trace, kind="expectation")
        assert result == {}

    def test_extract_ignores_wrong_kind(self):
        from feast.finetuning.trace_extractor import _extract_assessments

        trace = MagicMock()
        trace.info.assessments = [
            self._make_mock_assessment("response_quality", "feedback", "good"),
        ]

        result = _extract_assessments(trace, kind="expectation")
        assert result == {}


# ---------------------------------------------------------------------------
# Sync assessments with pivot mode
# ---------------------------------------------------------------------------


class TestSyncAssessmentsPivot:
    def _make_mock_trace(self, trace_id, assessments):
        trace = MagicMock()
        trace.info.trace_id = trace_id
        trace.info.assessments = assessments
        return trace

    def _make_assessment(self, name, kind, value, source_id="reviewer"):
        a = MagicMock()
        a.name = name
        if kind == "expectation":
            exp = MagicMock()
            exp.value = value
            a.expectation = exp
            a.feedback = None
        else:
            fb = MagicMock()
            fb.value = value
            a.feedback = fb
            a.expectation = None
        source = MagicMock()
        source.source_id = source_id
        a.source = source
        a.rationale = None
        a.create_time_ms = None
        return a

    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri", return_value=None
    )
    def test_pivot_mode_produces_labelview_rows(self, mock_uri):
        from feast.mlflow_integration.dataset_sync import (
            sync_trace_assessments_to_feast,
        )

        mock_mlflow = MagicMock()
        mock_experiment = MagicMock()
        mock_experiment.experiment_id = "exp-1"
        mock_mlflow.get_experiment_by_name.return_value = mock_experiment

        traces = [
            self._make_mock_trace(
                "tr-1",
                [
                    self._make_assessment(
                        "expected_response",
                        "expectation",
                        "Better answer",
                        "gpt-4-judge",
                    ),
                    self._make_assessment(
                        "response_quality", "feedback", "good", "gpt-4-judge"
                    ),
                ],
            ),
        ]
        mock_mlflow.search_traces.return_value = traces

        store = MagicMock()
        store.config = MagicMock()
        store.config.mlflow = None

        with patch.dict("sys.modules", {"mlflow": mock_mlflow}):
            result = sync_trace_assessments_to_feast(
                store=store,
                experiment_name="test",
                feature_view_name="agent_feedback",
                pivot=True,
                assessment_mapping={"expected_response": "corrected_response"},
                labeler_column="labeler",
                dry_run=True,
            )

        assert result.records_fetched == 2
        assert result.new_records == 1
