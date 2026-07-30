"""Unit tests for MlflowDatasetSource and related wiring."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from feast import Entity, FeatureView, Field, FileSource
from feast.infra.data_sources.mlflow import MlflowDatasetSource
from feast.types import String
from feast.value_type import ValueType


def _batch_source(path: str = "data/mlflow_eval.parquet") -> FileSource:
    return FileSource(path=path, timestamp_field="event_timestamp")


class TestMlflowDatasetSource:
    def test_requires_dataset_identity(self):
        with pytest.raises(ValueError, match="dataset_name or dataset_id"):
            MlflowDatasetSource(
                name="src",
                batch_source=_batch_source(),
            )

    def test_requires_name(self):
        from feast.errors import DataSourceNoNameException

        with pytest.raises(DataSourceNoNameException):
            MlflowDatasetSource(
                name=None,  # type: ignore[arg-type]
                dataset_name="eval",
                batch_source=_batch_source(),
            )

    def test_proto_round_trip(self):
        src = MlflowDatasetSource(
            name="prod_eval",
            dataset_name="production_validation_set",
            dataset_id="d-abc",
            tracking_uri="http://mlflow:5000",
            field_mapping={"expectations.expected_response": "expected_response"},
            timestamp_field="event_timestamp",
            batch_source=_batch_source(),
            description="eval set",
            tags={"team": "ml"},
            owner="ml@example.com",
        )
        proto = src.to_proto()
        restored = MlflowDatasetSource.from_proto(proto)

        assert restored.name == "prod_eval"
        assert restored.dataset_name == "production_validation_set"
        assert restored.dataset_id == "d-abc"
        assert restored.tracking_uri == "http://mlflow:5000"
        assert restored.field_mapping == {
            "expectations.expected_response": "expected_response"
        }
        assert restored.timestamp_field == "event_timestamp"
        assert isinstance(restored.batch_source, FileSource)
        assert restored.batch_source.path == "data/mlflow_eval.parquet"
        assert restored.description == "eval set"
        assert restored.tags == {"team": "ml"}
        assert restored.owner == "ml@example.com"

    def test_validate_soft_no_mlflow(self):
        src = MlflowDatasetSource(
            name="prod_eval",
            dataset_name="production_validation_set",
            batch_source=_batch_source(),
        )
        # Should not import or contact MLflow
        src.validate(MagicMock())

    def test_feature_view_requires_batch_source(self):
        with pytest.raises(ValueError, match="batch_source"):
            # Construct via __new__ path is hard; unset batch_source after init
            # is blocked by constructor. Simulate FeatureView check by
            # monkeypatching batch_source to None on a Push-like object.
            src = MlflowDatasetSource(
                name="prod_eval",
                dataset_name="ds",
                batch_source=_batch_source(),
            )
            src.batch_source = None  # type: ignore[assignment]
            FeatureView(
                name="fv",
                entities=[],
                schema=[Field(name="x", dtype=String)],
                source=src,
            )

    def test_feature_view_uses_batch_source(self):
        batch = _batch_source()
        src = MlflowDatasetSource(
            name="prod_eval",
            dataset_name="production_validation_set",
            batch_source=batch,
            timestamp_field="event_timestamp",
        )
        entity = Entity(
            name="dataset_record_id",
            join_keys=["dataset_record_id"],
            value_type=ValueType.STRING,
        )
        fv = FeatureView(
            name="mlflow_eval_records",
            entities=[entity],
            schema=[Field(name="expected_response", dtype=String)],
            source=src,
        )
        assert fv.stream_source is src
        assert fv.batch_source is batch


class TestSyncFromMlflowDatasetSource:
    def _make_mock_dataset(self, records):
        dataset = MagicMock()
        dataset.to_df.return_value = pd.DataFrame(records)
        dataset.tags = {}
        dataset.dataset_id = "d-1"
        return dataset

    @patch("feast.mlflow_integration.dataset_sync._set_last_sync_time")
    @patch("feast.mlflow_integration.dataset_sync._fetch_dataset_with_retry")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri",
        return_value="http://mlflow:5000",
    )
    def test_sync_reads_identity_from_source(self, mock_uri, mock_fetch, mock_set_sync):
        from feast.mlflow_integration.dataset_sync import sync_mlflow_dataset_to_feast

        batch = _batch_source()
        src = MlflowDatasetSource(
            name="prod_eval",
            dataset_name="production_validation_set",
            field_mapping={"expectations.expected_response": "corrected_response"},
            batch_source=batch,
            timestamp_field="event_timestamp",
        )
        entity = Entity(
            name="dataset_record_id",
            join_keys=["dataset_record_id"],
            value_type=ValueType.STRING,
        )
        fv = FeatureView(
            name="mlflow_eval_records",
            entities=[entity],
            schema=[Field(name="corrected_response", dtype=String)],
            source=src,
        )

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

        store = MagicMock()
        store.config = MagicMock()
        store.config.mlflow = None
        store.get_feature_view.return_value = fv
        store.get_label_view.side_effect = Exception("not a label view")
        store.list_feature_views.return_value = [fv]
        store.list_label_views.return_value = []

        result = sync_mlflow_dataset_to_feast(
            store=store,
            feature_view_name="mlflow_eval_records",
            incremental=False,
        )

        assert result.records_fetched == 1
        assert result.records_ingested == 1
        mock_fetch.assert_called()
        # Offline write uses write_to_offline_store for MlflowDatasetSource
        store.write_to_offline_store.assert_called_once()
        store.push.assert_not_called()
        written = store.write_to_online_store.call_args[0][1]
        assert "corrected_response" in written.columns


class TestSearchTracesForAssessments:
    def test_prefers_return_type_list(self):
        from feast.mlflow_integration.dataset_sync import _search_traces_for_assessments

        mock_mlflow = MagicMock()
        traces = [MagicMock(name="t1")]
        mock_mlflow.search_traces.return_value = traces

        result = _search_traces_for_assessments(mock_mlflow, {"experiment_ids": ["1"]})
        assert result == traces
        mock_mlflow.search_traces.assert_called_once_with(
            experiment_ids=["1"], return_type="list"
        )

    def test_falls_back_when_df_lacks_assessments_column(self):
        from feast.mlflow_integration.dataset_sync import _search_traces_for_assessments

        mock_mlflow = MagicMock()
        fetched = MagicMock(name="fetched")

        def _search(**kwargs):
            if kwargs.get("return_type") == "list":
                raise TypeError("return_type not supported")
            return pd.DataFrame({"trace_id": ["tr-1"]})

        mock_mlflow.search_traces.side_effect = _search
        mock_mlflow.get_trace.return_value = fetched

        result = _search_traces_for_assessments(mock_mlflow, {"experiment_ids": ["1"]})
        assert result == [fetched]
        mock_mlflow.get_trace.assert_called_once_with("tr-1")


class TestResolveLabelsFromFeast:
    def test_historical_applies_conflict_policy(self):
        from feast.finetuning.label_resolver import resolve_labels_from_feast
        from feast.finetuning.trace_extractor import FinetuningExample
        from feast.labeling.conflict_policy import ConflictPolicy

        examples = [
            FinetuningExample(
                trace_id="tr-1", messages=[{"role": "user", "content": "hi"}]
            ),
        ]

        label_view = MagicMock()
        label_view.labeler_field = "labeler"
        label_view.conflict_policy = ConflictPolicy.LAST_WRITE_WINS
        label_view.labeler_priorities = None
        label_view.join_keys = ["trace_id"]
        label_view.features = [
            MagicMock(name="corrected_response"),
            MagicMock(name="labeler"),
        ]
        # Field.name used in schema filter
        label_view.features[0].name = "corrected_response"
        label_view.features[1].name = "labeler"
        batch_source = MagicMock()
        batch_source.timestamp_field = "event_timestamp"
        label_view.batch_source = batch_source

        store = MagicMock()
        store.get_label_view.return_value = label_view

        offline_df = pd.DataFrame(
            {
                "trace_id": ["tr-1", "tr-1"],
                "corrected_response": ["old", "new"],
                "labeler": ["bot", "human"],
                "event_timestamp": [
                    datetime(2026, 1, 1, tzinfo=timezone.utc),
                    datetime(2026, 1, 2, tzinfo=timezone.utc),
                ],
            }
        )
        job = MagicMock()
        job.to_df.return_value = offline_df
        store._get_provider.return_value.offline_store.pull_all_from_table_or_query.return_value = job

        result = resolve_labels_from_feast(
            examples,
            store=store,
            label_view_name="agent_feedback",
            label_fields=["corrected_response"],
            label_source="historical",
        )
        assert result[0].corrected_response == "new"
        assert result[0].labeler == "human"

    def test_online_uses_get_online_features(self):
        from feast.finetuning.label_resolver import resolve_labels_from_feast
        from feast.finetuning.trace_extractor import FinetuningExample

        examples = [
            FinetuningExample(
                trace_id="tr-1", messages=[{"role": "user", "content": "hi"}]
            ),
        ]
        label_view = MagicMock()
        label_view.labeler_field = "labeler"
        store = MagicMock()
        store.get_label_view.return_value = label_view
        store.get_online_features.return_value.to_df.return_value = pd.DataFrame(
            {
                "trace_id": ["tr-1"],
                "corrected_response": ["online-answer"],
                "labeler": ["reviewer"],
            }
        )

        result = resolve_labels_from_feast(
            examples,
            store=store,
            label_view_name="agent_feedback",
            label_fields=["corrected_response"],
            label_source="online",
        )
        assert result[0].corrected_response == "online-answer"
        store.get_online_features.assert_called_once()
