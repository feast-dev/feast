"""Unit tests for MlflowDatasetSource (dual-mode) and related wiring."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from feast import Entity, FeatureView, Field, FileSource
from feast.infra.data_sources.mlflow import MlflowDatasetSource
from feast.types import String
from feast.value_type import ValueType


def _batch_source(path: str = "data/mlflow_eval.parquet") -> FileSource:
    return FileSource(path=path, timestamp_field="event_timestamp")


# ---------------------------------------------------------------------------
# MlflowDatasetSource construction and validation
# ---------------------------------------------------------------------------


class TestMlflowDatasetSourceConstruction:
    def test_genai_mode(self):
        src = MlflowDatasetSource(
            name="genai_src",
            dataset_name="eval_ds",
            batch_source=_batch_source(),
        )
        assert src.is_genai_mode
        assert not src.is_artifact_mode
        assert src.dataset_name == "eval_ds"

    def test_artifact_mode(self):
        src = MlflowDatasetSource(
            name="artifact_src",
            run_id="abc123",
            artifact_path="outputs/data.parquet",
            batch_source=_batch_source(),
        )
        assert src.is_artifact_mode
        assert not src.is_genai_mode
        assert src.run_id == "abc123"
        assert src.artifact_format == "parquet"

    def test_csv_artifact_format(self):
        src = MlflowDatasetSource(
            name="csv_src",
            run_id="abc123",
            artifact_path="outputs/data.csv",
            artifact_format="csv",
            batch_source=_batch_source(),
        )
        assert src.artifact_format == "csv"

    def test_rejects_no_mode(self):
        with pytest.raises(ValueError, match="dataset_name/dataset_id"):
            MlflowDatasetSource(
                name="src",
                batch_source=_batch_source(),
            )

    def test_rejects_both_modes(self):
        with pytest.raises(ValueError, match="does not support setting both"):
            MlflowDatasetSource(
                name="src",
                dataset_name="eval_ds",
                run_id="abc123",
                artifact_path="data.parquet",
                batch_source=_batch_source(),
            )

    def test_rejects_unsupported_format(self):
        with pytest.raises(ValueError, match="Unsupported artifact_format"):
            MlflowDatasetSource(
                name="src",
                run_id="abc123",
                artifact_path="data.json",
                artifact_format="json",
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

    def test_requires_batch_source(self):
        from typeguard import TypeCheckError

        with pytest.raises((ValueError, TypeCheckError)):
            MlflowDatasetSource(
                name="src",
                dataset_name="eval",
                batch_source=None,  # type: ignore[arg-type]
            )

    def test_artifact_mode_requires_both_run_id_and_path(self):
        with pytest.raises(ValueError, match="dataset_name/dataset_id"):
            MlflowDatasetSource(
                name="src",
                run_id="abc123",
                batch_source=_batch_source(),
            )


# ---------------------------------------------------------------------------
# Proto serialization round-trip
# ---------------------------------------------------------------------------


class TestProtoRoundTrip:
    def test_genai_mode_round_trip(self):
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
        assert restored.is_genai_mode
        assert not restored.is_artifact_mode
        assert restored.run_id is None
        assert restored.artifact_path is None
        assert restored.field_mapping == {
            "expectations.expected_response": "expected_response"
        }
        assert restored.timestamp_field == "event_timestamp"
        assert isinstance(restored.batch_source, FileSource)
        assert restored.batch_source.path == "data/mlflow_eval.parquet"
        assert restored.description == "eval set"
        assert restored.tags == {"team": "ml"}
        assert restored.owner == "ml@example.com"

    def test_artifact_mode_round_trip(self):
        src = MlflowDatasetSource(
            name="run_data",
            run_id="run-xyz",
            artifact_path="outputs/features.parquet",
            artifact_format="parquet",
            tracking_uri="http://mlflow:5000",
            batch_source=_batch_source(),
            timestamp_field="ts",
        )
        proto = src.to_proto()
        restored = MlflowDatasetSource.from_proto(proto)

        assert restored.name == "run_data"
        assert restored.run_id == "run-xyz"
        assert restored.artifact_path == "outputs/features.parquet"
        assert restored.artifact_format == "parquet"
        assert restored.is_artifact_mode
        assert not restored.is_genai_mode
        assert restored.dataset_name is None

    def test_csv_artifact_round_trip(self):
        src = MlflowDatasetSource(
            name="csv_data",
            run_id="run-csv",
            artifact_path="data.csv",
            artifact_format="csv",
            batch_source=_batch_source(),
        )
        proto = src.to_proto()
        restored = MlflowDatasetSource.from_proto(proto)
        assert restored.artifact_format == "csv"

    def test_equality(self):
        batch = _batch_source()
        src1 = MlflowDatasetSource(
            name="src",
            dataset_name="ds",
            batch_source=batch,
        )
        src2 = MlflowDatasetSource(
            name="src",
            dataset_name="ds",
            batch_source=batch,
        )
        assert src1 == src2

    def test_inequality_different_mode(self):
        batch = _batch_source()
        genai = MlflowDatasetSource(
            name="src",
            dataset_name="ds",
            batch_source=batch,
        )
        artifact = MlflowDatasetSource(
            name="src",
            run_id="run1",
            artifact_path="data.parquet",
            batch_source=batch,
        )
        assert genai != artifact


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------


class TestValidate:
    def test_validate_genai_passes(self):
        src = MlflowDatasetSource(
            name="src",
            dataset_name="ds",
            batch_source=_batch_source(),
        )
        src.validate(MagicMock())

    def test_validate_artifact_passes(self):
        src = MlflowDatasetSource(
            name="src",
            run_id="run1",
            artifact_path="data.parquet",
            batch_source=_batch_source(),
        )
        src.validate(MagicMock())


# ---------------------------------------------------------------------------
# FeatureView integration — batch source behavior
# ---------------------------------------------------------------------------


class TestFeatureViewBatchSource:
    def test_mlflow_source_is_batch_not_stream(self):
        """MlflowDatasetSource should be the batch_source, not stream_source."""
        batch = _batch_source()
        src = MlflowDatasetSource(
            name="eval_src",
            dataset_name="eval_ds",
            batch_source=batch,
            timestamp_field="event_timestamp",
        )
        entity = Entity(
            name="record_id",
            join_keys=["record_id"],
            value_type=ValueType.STRING,
        )
        fv = FeatureView(
            name="mlflow_fv",
            entities=[entity],
            schema=[Field(name="feature_col", dtype=String)],
            source=src,
        )
        # MlflowDatasetSource should be the batch_source (not unwrapped)
        assert fv.batch_source is src
        # It should NOT be treated as a stream source
        assert fv.stream_source is None


# ---------------------------------------------------------------------------
# Auth token resolution
# ---------------------------------------------------------------------------


class TestAuthTokenResolution:
    def test_env_var_fallback(self):
        from feast.infra.data_sources.mlflow.auth import resolve_mlflow_token

        with patch.dict(os.environ, {"MLFLOW_TRACKING_TOKEN": "env-token"}):
            token = resolve_mlflow_token()
            assert token == "env-token"

    def test_no_auth_returns_none(self):
        from feast.infra.data_sources.mlflow.auth import resolve_mlflow_token

        with patch.dict(
            os.environ,
            {},
            clear=True,
        ):
            with patch(
                "feast.infra.data_sources.mlflow.auth._from_request_context",
                return_value=None,
            ):
                with patch(
                    "feast.infra.data_sources.mlflow.auth._from_service_account",
                    return_value=None,
                ):
                    token = resolve_mlflow_token()
                    assert token is None

    def test_request_context_token_takes_priority(self):
        from feast.infra.data_sources.mlflow.auth import resolve_mlflow_token

        with patch(
            "feast.infra.data_sources.mlflow.auth._from_request_context",
            return_value="req-token",
        ):
            with patch.dict(os.environ, {"MLFLOW_TRACKING_TOKEN": "env-token"}):
                token = resolve_mlflow_token()
                assert token == "req-token"

    def test_security_manager_token(self):
        from feast.infra.data_sources.mlflow.auth import _from_request_context
        from feast.permissions.security_manager import SecurityManager

        sm = MagicMock(spec=SecurityManager)
        sm.current_request_token = "bearer-abc"

        with patch(
            "feast.permissions.security_manager.get_security_manager",
            return_value=sm,
        ):
            token = _from_request_context()
            assert token == "bearer-abc"

    def test_sa_token_file(self, tmp_path):
        from feast.infra.data_sources.mlflow.auth import _from_service_account

        token_file = tmp_path / "token"
        token_file.write_text("sa-token-123")

        with patch("feast.infra.data_sources.mlflow.auth._SA_TOKEN_PATH", token_file):
            token = _from_service_account()
            assert token == "sa-token-123"


# ---------------------------------------------------------------------------
# Schema introspection (GenAI mode with mock)
# ---------------------------------------------------------------------------


class TestSchemaIntrospection:
    def test_genai_schema_from_mock_dataset(self):
        src = MlflowDatasetSource(
            name="eval_src",
            dataset_name="eval_ds",
            batch_source=_batch_source(),
        )
        mock_df = pd.DataFrame({"col_a": ["hello"], "col_b": [42], "col_c": [1.5]})
        with patch.object(src, "_fetch_genai_dataframe", return_value=mock_df):
            cols = list(src.get_table_column_names_and_types(MagicMock()))
            assert len(cols) == 3
            col_names = [c[0] for c in cols]
            assert "col_a" in col_names
            assert "col_b" in col_names
            assert "col_c" in col_names

    def test_falls_back_to_batch_source_on_error(self):
        batch = _batch_source()
        src = MlflowDatasetSource(
            name="eval_src",
            dataset_name="eval_ds",
            batch_source=batch,
        )
        with patch.object(
            src, "_fetch_genai_dataframe", side_effect=Exception("MLflow down")
        ):
            with patch.object(
                batch,
                "get_table_column_names_and_types",
                return_value=[("x", "string")],
            ):
                cols = list(src.get_table_column_names_and_types(MagicMock()))
                assert cols == [("x", "string")]

    def test_artifact_mode_uses_batch_source_schema(self):
        batch = _batch_source()
        src = MlflowDatasetSource(
            name="art_src",
            run_id="run1",
            artifact_path="data.parquet",
            batch_source=batch,
        )
        with patch.object(
            batch,
            "get_table_column_names_and_types",
            return_value=[("y", "float64")],
        ):
            cols = list(src.get_table_column_names_and_types(MagicMock()))
            assert cols == [("y", "float64")]


# ---------------------------------------------------------------------------
# Dataset sync from MlflowDatasetSource (from PR #6405, kept for flywheel)
# ---------------------------------------------------------------------------


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
        store.write_to_offline_store.assert_called_once()
        store.push.assert_not_called()
        written = store.write_to_online_store.call_args[0][1]
        assert "corrected_response" in written.columns
