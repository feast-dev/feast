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
        assert fv.batch_source is src
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
# Schema introspection
# ---------------------------------------------------------------------------


class TestSchemaIntrospection:
    def test_genai_schema_from_mlflow_metadata(self):
        """Schema resolved via dataset.schema (no full data fetch)."""
        src = MlflowDatasetSource(
            name="eval_src",
            dataset_name="eval_ds",
            batch_source=_batch_source(),
        )
        mock_dataset = MagicMock()
        mock_dataset.schema = (
            '{"col_a": "object", "col_b": "int64", "col_c": "float64"}'
        )

        with patch(
            "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
            return_value=None,
        ):
            with patch("mlflow.genai.datasets.get_dataset", return_value=mock_dataset):
                cols = list(src.get_table_column_names_and_types(MagicMock()))
                assert len(cols) == 3
                col_names = [c[0] for c in cols]
                assert "col_a" in col_names
                assert "col_b" in col_names
                assert "col_c" in col_names
                col_types = {c[0]: c[1] for c in cols}
                assert col_types["col_b"] == "int64"

    def test_genai_schema_fallback_to_head_fetch(self):
        """When dataset.schema is None, falls back to .to_df().head(1)."""
        src = MlflowDatasetSource(
            name="eval_src",
            dataset_name="eval_ds",
            batch_source=_batch_source(),
        )
        mock_df = pd.DataFrame(
            {
                "inputs": [{"question": "Q1"}],
                "expectations": [{"answer": "A1"}],
                "last_update_time": ["2026-06-15T12:00:00Z"],
                "dataset_record_id": ["rec-1"],
            }
        )
        mock_dataset = MagicMock()
        mock_dataset.schema = None
        mock_dataset.to_df.return_value = mock_df

        with patch(
            "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
            return_value=None,
        ):
            with patch("mlflow.genai.datasets.get_dataset", return_value=mock_dataset):
                cols = list(src.get_table_column_names_and_types(MagicMock()))
                col_names = [c[0] for c in cols]
                assert "dataset_record_id" in col_names
                assert "input_question" in col_names
                assert "event_timestamp" in col_names

    def test_falls_back_to_batch_source_on_error(self):
        """MLflow failures fall back to batch_source schema."""
        batch = _batch_source()
        src = MlflowDatasetSource(
            name="eval_src",
            dataset_name="eval_ds",
            batch_source=batch,
        )
        with patch(
            "mlflow.genai.datasets.get_dataset",
            side_effect=Exception("MLflow down"),
        ):
            with patch.object(
                batch,
                "get_table_column_names_and_types",
                return_value=[("x", "string")],
            ):
                cols = list(src.get_table_column_names_and_types(MagicMock()))
                assert cols == [("x", "string")]

    def test_artifact_mode_uses_batch_source_schema(self):
        """Artifact mode delegates directly to batch_source."""
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

    def test_schema_cache_avoids_repeated_calls(self):
        """Second call returns cached result without hitting MLflow."""
        src = MlflowDatasetSource(
            name="eval_src",
            dataset_name="eval_ds",
            batch_source=_batch_source(),
        )
        mock_dataset = MagicMock()
        mock_dataset.schema = '{"col_a": "object"}'

        with patch(
            "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
            return_value=None,
        ):
            with patch(
                "mlflow.genai.datasets.get_dataset", return_value=mock_dataset
            ) as mock_get:
                src.get_table_column_names_and_types(MagicMock())
                src.get_table_column_names_and_types(MagicMock())
                assert mock_get.call_count == 1

    def test_invalidate_cache_forces_refetch(self):
        """invalidate_cache() causes next call to hit MLflow again."""
        src = MlflowDatasetSource(
            name="eval_src",
            dataset_name="eval_ds",
            batch_source=_batch_source(),
        )
        mock_dataset = MagicMock()
        mock_dataset.schema = '{"col_a": "object"}'

        with patch(
            "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
            return_value=None,
        ):
            with patch(
                "mlflow.genai.datasets.get_dataset", return_value=mock_dataset
            ) as mock_get:
                src.get_table_column_names_and_types(MagicMock())
                src.invalidate_cache()
                src.get_table_column_names_and_types(MagicMock())
                assert mock_get.call_count == 2


# ---------------------------------------------------------------------------
# to_arrow caching
# ---------------------------------------------------------------------------


class TestToArrowCache:
    def test_arrow_cache_avoids_repeated_downloads(self):
        """Second to_arrow() call returns cached PyArrow Table."""
        import pyarrow as pa

        src = MlflowDatasetSource(
            name="art_src",
            run_id="run1",
            artifact_path="data.parquet",
            batch_source=_batch_source(),
        )
        expected_table = pa.table({"x": [1, 2, 3]})

        with patch.object(
            src, "_fetch_arrow", return_value=expected_table
        ) as mock_fetch:
            result1 = src.to_arrow()
            result2 = src.to_arrow()
            assert result1 is result2
            assert mock_fetch.call_count == 1

    def test_use_cache_false_forces_refetch(self):
        """to_arrow(use_cache=False) bypasses cache."""
        import pyarrow as pa

        src = MlflowDatasetSource(
            name="art_src",
            run_id="run1",
            artifact_path="data.parquet",
            batch_source=_batch_source(),
        )
        expected_table = pa.table({"x": [1, 2, 3]})

        with patch.object(
            src, "_fetch_arrow", return_value=expected_table
        ) as mock_fetch:
            src.to_arrow()
            src.to_arrow(use_cache=False)
            assert mock_fetch.call_count == 2


# ---------------------------------------------------------------------------
# Dataset sync from MlflowDatasetSource
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
        assert not result.errors
        mock_fetch.assert_called()
        store.write_to_offline_store.assert_called_once()
        store.push.assert_not_called()
        written = store.write_to_online_store.call_args[0][1]
        assert "corrected_response" in written.columns

    @patch("feast.mlflow_integration.dataset_sync._fetch_dataset_with_retry")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri",
        return_value="http://mlflow:5000",
    )
    def test_offline_write_error_blocks_watermark(self, mock_uri, mock_fetch):
        """Offline write failures must prevent watermark advancement."""
        from feast.mlflow_integration.dataset_sync import sync_mlflow_dataset_to_feast

        batch = _batch_source()
        src = MlflowDatasetSource(
            name="eval_src",
            dataset_name="ds",
            batch_source=batch,
            timestamp_field="event_timestamp",
        )
        entity = Entity(
            name="dataset_record_id",
            join_keys=["dataset_record_id"],
            value_type=ValueType.STRING,
        )
        fv = FeatureView(
            name="eval_fv",
            entities=[entity],
            schema=[Field(name="question", dtype=String)],
            source=src,
        )

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

        store = MagicMock()
        store.config = MagicMock()
        store.config.mlflow = None
        store.get_feature_view.return_value = fv
        store.get_label_view.side_effect = Exception("not a label view")
        store.write_to_offline_store.side_effect = RuntimeError("disk full")

        result = sync_mlflow_dataset_to_feast(
            store=store,
            feature_view_name="eval_fv",
            incremental=False,
        )

        assert len(result.errors) > 0
        assert "Offline write failed" in result.errors[0]

    @patch("feast.mlflow_integration.dataset_sync._set_last_sync_time")
    @patch("feast.mlflow_integration.dataset_sync._fetch_dataset_with_retry")
    @patch(
        "feast.mlflow_integration.dataset_sync._resolve_tracking_uri",
        return_value="http://mlflow:5000",
    )
    def test_sync_finds_source_via_batch_source_attr(
        self, mock_uri, mock_fetch, mock_set_sync
    ):
        """Verify _get_mlflow_dataset_source finds MlflowDatasetSource on batch_source."""
        from feast.mlflow_integration.dataset_sync import _get_mlflow_dataset_source

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
            name="test_fv",
            entities=[entity],
            schema=[Field(name="col", dtype=String)],
            source=src,
        )

        store = MagicMock()
        store.get_feature_view.return_value = fv
        store.get_label_view.side_effect = Exception("nope")

        result = _get_mlflow_dataset_source(store, "test_fv")
        assert result is src


# ---------------------------------------------------------------------------
# Error classification and retry behaviour
# ---------------------------------------------------------------------------


class TestErrorClassification:
    """Verify typed error wrapping for MLflow failures (AC4, AC5)."""

    def test_401_becomes_auth_error(self):
        from feast.infra.data_sources.mlflow.auth import (
            MlflowAuthError,
            classify_mlflow_error,
        )

        exc = Exception("INVALID_PARAMETER_VALUE: Unauthorized (401)")
        classified = classify_mlflow_error(exc)
        assert isinstance(classified, MlflowAuthError)
        assert "401" in str(classified)

    def test_403_becomes_auth_error(self):
        from feast.infra.data_sources.mlflow.auth import (
            MlflowAuthError,
            classify_mlflow_error,
        )

        exc = Exception("Forbidden (403): insufficient permissions")
        classified = classify_mlflow_error(exc)
        assert isinstance(classified, MlflowAuthError)
        assert "403" in str(classified)

    def test_404_becomes_not_found_error(self):
        from feast.infra.data_sources.mlflow.auth import (
            MlflowArtifactNotFoundError,
            classify_mlflow_error,
        )

        exc = Exception("RESOURCE_DOES_NOT_EXIST: Run 'abc123' not found (404)")
        classified = classify_mlflow_error(exc)
        assert isinstance(classified, MlflowArtifactNotFoundError)
        assert "not found" in str(classified).lower()

    def test_connection_error_becomes_connection_error(self):
        from feast.infra.data_sources.mlflow.auth import (
            MlflowConnectionError,
            classify_mlflow_error,
        )

        exc = ConnectionError("Connection refused")
        classified = classify_mlflow_error(exc)
        assert isinstance(classified, MlflowConnectionError)

    def test_timeout_becomes_connection_error(self):
        from feast.infra.data_sources.mlflow.auth import (
            MlflowConnectionError,
            classify_mlflow_error,
        )

        exc = TimeoutError("Read timed out")
        classified = classify_mlflow_error(exc)
        assert isinstance(classified, MlflowConnectionError)

    def test_generic_error_passes_through(self):
        from feast.infra.data_sources.mlflow.auth import classify_mlflow_error

        exc = ValueError("some internal error")
        classified = classify_mlflow_error(exc)
        assert classified is exc


class TestRetryClassification:
    """Verify which errors are retryable (transient) vs terminal."""

    def test_connection_error_is_retryable(self):
        from feast.infra.data_sources.mlflow.auth import is_retryable_error

        assert is_retryable_error(ConnectionError("refused"))
        assert is_retryable_error(TimeoutError("timed out"))
        assert is_retryable_error(OSError("Network unreachable"))

    def test_5xx_is_retryable(self):
        from feast.infra.data_sources.mlflow.auth import is_retryable_error

        exc = Exception("Internal Server Error (500)")
        assert is_retryable_error(exc)

    def test_429_is_retryable(self):
        from feast.infra.data_sources.mlflow.auth import is_retryable_error

        exc = Exception("Too Many Requests (429)")
        assert is_retryable_error(exc)

    def test_404_is_not_retryable(self):
        from feast.infra.data_sources.mlflow.auth import is_retryable_error

        exc = Exception("Not Found (404)")
        assert not is_retryable_error(exc)

    def test_401_is_not_retryable(self):
        from feast.infra.data_sources.mlflow.auth import is_retryable_error

        exc = Exception("Unauthorized (401)")
        assert not is_retryable_error(exc)

    def test_validation_error_is_not_retryable(self):
        from feast.infra.data_sources.mlflow.auth import is_retryable_error

        exc = ValueError("invalid parameter")
        assert not is_retryable_error(exc)


class TestFetchArrowRetry:
    """Verify _fetch_arrow retries transient failures then raises clear error."""

    def test_retries_on_connection_error_then_succeeds(self):
        import pyarrow as pa

        src = MlflowDatasetSource(
            name="retry_src",
            dataset_name="ds",
            batch_source=_batch_source(),
        )
        expected = pa.table({"x": [1]})

        call_count = {"n": 0}

        def side_effect(token, uri):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise ConnectionError("Connection refused")
            return expected

        with patch.object(src, "_fetch_genai_arrow", side_effect=side_effect):
            with patch(
                "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
                return_value=None,
            ):
                result = src._fetch_arrow()
                assert result.equals(expected)
                assert call_count["n"] == 3

    def test_raises_connection_error_after_max_retries(self):
        from feast.infra.data_sources.mlflow.auth import MlflowConnectionError

        src = MlflowDatasetSource(
            name="fail_src",
            dataset_name="ds",
            batch_source=_batch_source(),
        )

        with patch.object(
            src,
            "_fetch_genai_arrow",
            side_effect=ConnectionError("Connection refused"),
        ):
            with patch(
                "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
                return_value=None,
            ):
                with pytest.raises(MlflowConnectionError, match="after 3 attempts"):
                    src._fetch_arrow()

    def test_does_not_retry_auth_errors(self):
        from feast.infra.data_sources.mlflow.auth import MlflowAuthError

        src = MlflowDatasetSource(
            name="auth_src",
            dataset_name="ds",
            batch_source=_batch_source(),
        )

        call_count = {"n": 0}

        def side_effect(token, uri):
            call_count["n"] += 1
            raise Exception("Unauthorized (401)")

        with patch.object(src, "_fetch_genai_arrow", side_effect=side_effect):
            with patch(
                "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
                return_value=None,
            ):
                with pytest.raises(MlflowAuthError):
                    src._fetch_arrow()
                assert call_count["n"] == 1

    def test_does_not_retry_not_found_errors(self):
        from feast.infra.data_sources.mlflow.auth import MlflowArtifactNotFoundError

        src = MlflowDatasetSource(
            name="missing_src",
            run_id="deleted-run",
            artifact_path="data.parquet",
            batch_source=_batch_source(),
        )

        call_count = {"n": 0}

        def side_effect(token, uri):
            call_count["n"] += 1
            raise Exception("RESOURCE_DOES_NOT_EXIST: Run not found (404)")

        with patch.object(src, "_fetch_artifact_arrow", side_effect=side_effect):
            with patch(
                "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
                return_value=None,
            ):
                with pytest.raises(MlflowArtifactNotFoundError):
                    src._fetch_arrow()
                assert call_count["n"] == 1


class TestArtifactNotFound:
    """Verify descriptive error when artifact/dataset is missing (AC criterion)."""

    def test_missing_artifact_descriptive_error(self):
        from feast.infra.data_sources.mlflow.auth import MlflowArtifactNotFoundError

        src = MlflowDatasetSource(
            name="prod_features",
            run_id="run-xyz-123",
            artifact_path="outputs/model_features.parquet",
            batch_source=_batch_source(),
        )

        with patch(
            "mlflow.artifacts.download_artifacts",
            side_effect=Exception(
                "RESOURCE_DOES_NOT_EXIST: Run 'run-xyz-123' not found (404)"
            ),
        ):
            with patch(
                "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
                return_value="token",
            ):
                with pytest.raises(MlflowArtifactNotFoundError) as exc_info:
                    src._fetch_arrow()
                assert "run-xyz-123" in str(exc_info.value)
                assert "prod_features" in str(exc_info.value)

    def test_missing_genai_dataset_descriptive_error(self):
        from feast.infra.data_sources.mlflow.auth import MlflowArtifactNotFoundError

        src = MlflowDatasetSource(
            name="eval_source",
            dataset_name="deleted_dataset",
            batch_source=_batch_source(),
        )

        with patch(
            "mlflow.genai.datasets.get_dataset",
            side_effect=Exception(
                "RESOURCE_DOES_NOT_EXIST: Dataset 'deleted_dataset' not found (404)"
            ),
        ):
            with patch(
                "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
                return_value=None,
            ):
                with pytest.raises(MlflowArtifactNotFoundError) as exc_info:
                    src._fetch_arrow()
                assert "deleted_dataset" in str(exc_info.value)
                assert "eval_source" in str(exc_info.value)


class TestAuthEnforcement:
    """Verify auth errors propagate clearly (AC4)."""

    def test_401_propagates_with_context(self):
        from feast.infra.data_sources.mlflow.auth import MlflowAuthError

        src = MlflowDatasetSource(
            name="secured_src",
            dataset_name="private_ds",
            batch_source=_batch_source(),
        )

        with patch(
            "mlflow.genai.datasets.get_dataset",
            side_effect=Exception(
                "PERMISSION_DENIED: Unauthorized (401). Token is invalid or expired."
            ),
        ):
            with patch(
                "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
                return_value="expired-token",
            ):
                with pytest.raises(MlflowAuthError) as exc_info:
                    src._fetch_arrow()
                assert "401" in str(exc_info.value)
                assert "MLFLOW_TRACKING_TOKEN" in str(exc_info.value)

    def test_403_propagates_with_context(self):
        from feast.infra.data_sources.mlflow.auth import MlflowAuthError

        src = MlflowDatasetSource(
            name="restricted_src",
            run_id="run-1",
            artifact_path="secret.parquet",
            batch_source=_batch_source(),
        )

        with patch(
            "mlflow.artifacts.download_artifacts",
            side_effect=Exception("Forbidden (403): insufficient permissions"),
        ):
            with patch(
                "feast.infra.data_sources.mlflow.auth.resolve_mlflow_token",
                return_value="valid-but-restricted",
            ):
                with pytest.raises(MlflowAuthError) as exc_info:
                    src._fetch_arrow()
                assert "403" in str(exc_info.value)
                assert "permission" in str(exc_info.value).lower()


class TestCABundleScope:
    """Verify CA bundle context manager for TLS."""

    def test_ca_bundle_sets_and_restores_env(self):
        from feast.infra.data_sources.mlflow.auth import mlflow_ca_bundle_scope

        original = os.environ.get("REQUESTS_CA_BUNDLE")
        with mlflow_ca_bundle_scope("/etc/pki/custom-ca.crt"):
            assert os.environ["REQUESTS_CA_BUNDLE"] == "/etc/pki/custom-ca.crt"
        assert os.environ.get("REQUESTS_CA_BUNDLE") == original

    def test_ca_bundle_none_is_noop(self):
        from feast.infra.data_sources.mlflow.auth import mlflow_ca_bundle_scope

        original = os.environ.get("REQUESTS_CA_BUNDLE")
        with mlflow_ca_bundle_scope(None):
            assert os.environ.get("REQUESTS_CA_BUNDLE") == original


class TestHeaderProvider:
    """Verify FeastMLflowHeaderProvider plugin behaviour."""

    def test_in_context_false_when_no_token(self):
        from feast.infra.data_sources.mlflow.auth import FeastMLflowHeaderProvider

        provider = FeastMLflowHeaderProvider()
        assert not provider.in_context()

    def test_in_context_true_with_token(self):
        from feast.infra.data_sources.mlflow.auth import (
            FeastMLflowHeaderProvider,
            mlflow_token_scope,
        )

        provider = FeastMLflowHeaderProvider()
        with mlflow_token_scope("test-token"):
            assert provider.in_context()
            headers = provider.request_headers()
            assert headers == {"Authorization": "Bearer test-token"}

    def test_headers_empty_without_token(self):
        from feast.infra.data_sources.mlflow.auth import FeastMLflowHeaderProvider

        provider = FeastMLflowHeaderProvider()
        assert provider.request_headers() == {}
