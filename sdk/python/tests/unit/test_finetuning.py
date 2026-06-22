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
            mock_mlflow.log_input.assert_called_once()
            mock_mlflow.log_artifact.assert_called_once_with(path)
            mock_mlflow.set_tag.assert_called_once_with("use_case", "red-teaming")
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
    @patch("feast.mlflow_integration.dataset_sync._fetch_dataset_with_retry")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri", return_value=None
    )
    def test_sync_full_refresh(self, mock_uri, mock_fetch, mock_set_sync):
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
# MlflowDatasetSource serialization
# ---------------------------------------------------------------------------


class TestMlflowDatasetSource:
    def test_to_proto_and_back(self):
        from feast.infra.offline_stores.contrib.mlflow_source import MlflowDatasetSource

        source = MlflowDatasetSource(
            name="test-source",
            dataset_name="agent-feedback-v3",
            dataset_id="ds-123",
            experiment_ids=["exp-1", "exp-2"],
            tracking_uri="http://mlflow:5000",
            field_mapping={"expectations.expected_response": "corrected_response"},
            timestamp_field="event_timestamp",
            record_id_field="dataset_record_id",
            description="Test MLflow source",
            tags={"team": "ml"},
            owner="test@example.com",
        )

        proto = source._to_proto_impl()
        restored = MlflowDatasetSource.from_proto(proto)

        assert restored.name == "test-source"
        assert restored.dataset_name == "agent-feedback-v3"
        assert restored.dataset_id == "ds-123"
        assert restored.experiment_ids == ["exp-1", "exp-2"]
        assert restored.tracking_uri == "http://mlflow:5000"
        assert restored.record_id_field == "dataset_record_id"
        assert restored.timestamp_field == "event_timestamp"
        assert restored.description == "Test MLflow source"
        assert restored.tags == {"team": "ml"}
        assert restored.owner == "test@example.com"

    def test_equality(self):
        from feast.infra.offline_stores.contrib.mlflow_source import MlflowDatasetSource

        kwargs = dict(
            name="src",
            dataset_name="ds",
            tracking_uri="http://mlflow:5000",
        )
        s1 = MlflowDatasetSource(**kwargs)
        s2 = MlflowDatasetSource(**kwargs)
        assert s1 == s2

    def test_source_type(self):
        from feast.infra.offline_stores.contrib.mlflow_source import MlflowDatasetSource
        from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto

        source = MlflowDatasetSource(name="s", dataset_name="ds")
        assert source.source_type() == DataSourceProto.CUSTOM_SOURCE


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
