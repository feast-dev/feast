import os
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Tests for logger.py
# ---------------------------------------------------------------------------
class TestLogFeatureRetrieval:
    """Tests for feast.mlflow_integration.logger.log_feature_retrieval_to_mlflow."""

    def setup_method(self):
        import feast.mlflow_integration.logger as mod

        mod._mlflow = None
        mod._mlflow_checked = False

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_logs_tags_params_metrics_when_active_run(self, mock_get_mlflow):
        mock_mlflow = MagicMock()
        mock_run = MagicMock()
        mock_run.info.run_id = "run_abc123"
        mock_mlflow.active_run.return_value = mock_run
        mock_get_mlflow.return_value = mock_mlflow

        from feast.mlflow_integration.logger import log_feature_retrieval_to_mlflow

        result = log_feature_retrieval_to_mlflow(
            feature_refs=["fv:feat1", "fv:feat2"],
            entity_count=100,
            duration_seconds=1.5,
            retrieval_type="historical",
            project="my_project",
            feature_service_name="my_svc",
        )

        assert result is True
        client = mock_mlflow.MlflowClient()
        client.set_tag.assert_any_call("run_abc123", "feast.project", "my_project")
        client.set_tag.assert_any_call("run_abc123", "feast.retrieval_type", "historical")
        client.set_tag.assert_any_call("run_abc123", "feast.feature_service", "my_svc")
        client.set_tag.assert_any_call("run_abc123", "feast.feature_views", "fv")
        client.log_param.assert_any_call("run_abc123", "feast.feature_refs", "fv:feat1,fv:feat2")
        client.log_param.assert_any_call("run_abc123", "feast.entity_count", "100")
        client.log_param.assert_any_call("run_abc123", "feast.feature_count", "2")
        client.log_metric.assert_any_call("run_abc123", "feast.retrieval_duration_sec", 1.5)

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_logs_feature_service_from_object(self, mock_get_mlflow):
        mock_mlflow = MagicMock()
        mock_run = MagicMock()
        mock_run.info.run_id = "run_xyz"
        mock_mlflow.active_run.return_value = mock_run
        mock_get_mlflow.return_value = mock_mlflow

        mock_fs = MagicMock()
        mock_fs.name = "fraud_detection_service"

        from feast.mlflow_integration.logger import log_feature_retrieval_to_mlflow

        result = log_feature_retrieval_to_mlflow(
            feature_refs=["fv:feat1"],
            entity_count=10,
            duration_seconds=0.3,
            retrieval_type="online",
            feature_service=mock_fs,
        )

        assert result is True
        client = mock_mlflow.MlflowClient()
        client.set_tag.assert_any_call("run_xyz", "feast.feature_service", "fraud_detection_service")

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_noop_when_no_active_run(self, mock_get_mlflow):
        mock_mlflow = MagicMock()
        mock_mlflow.active_run.return_value = None
        mock_get_mlflow.return_value = mock_mlflow

        from feast.mlflow_integration.logger import log_feature_retrieval_to_mlflow

        result = log_feature_retrieval_to_mlflow(
            feature_refs=["fv:feat1"], entity_count=10, duration_seconds=0.5,
        )
        assert result is False

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_noop_when_mlflow_not_installed(self, mock_get_mlflow):
        mock_get_mlflow.return_value = None

        from feast.mlflow_integration.logger import log_feature_retrieval_to_mlflow

        result = log_feature_retrieval_to_mlflow(
            feature_refs=["fv:feat1"], entity_count=10, duration_seconds=0.5,
        )
        assert result is False

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_truncates_long_feature_refs(self, mock_get_mlflow):
        mock_mlflow = MagicMock()
        mock_run = MagicMock()
        mock_run.info.run_id = "run_trunc"
        mock_mlflow.active_run.return_value = mock_run
        mock_get_mlflow.return_value = mock_mlflow

        from feast.mlflow_integration.logger import log_feature_retrieval_to_mlflow

        long_refs = [f"feature_view:feature_{i}" for i in range(100)]
        result = log_feature_retrieval_to_mlflow(
            feature_refs=long_refs, entity_count=50, duration_seconds=2.0,
        )

        assert result is True
        client = mock_mlflow.MlflowClient()
        logged_refs = [c for c in client.log_param.call_args_list if c[0][1] == "feast.feature_refs"]
        assert len(logged_refs) == 1
        assert len(logged_refs[0][0][2]) <= 500

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_handles_mlflow_exception_gracefully(self, mock_get_mlflow):
        mock_mlflow = MagicMock()
        mock_run = MagicMock()
        mock_run.info.run_id = "run_err"
        mock_mlflow.active_run.return_value = mock_run
        mock_mlflow.MlflowClient().set_tag.side_effect = Exception("boom")
        mock_get_mlflow.return_value = mock_mlflow

        from feast.mlflow_integration.logger import log_feature_retrieval_to_mlflow

        result = log_feature_retrieval_to_mlflow(
            feature_refs=["fv:feat1"], entity_count=5, duration_seconds=0.1,
        )
        assert result is False

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_extracts_feature_view_names(self, mock_get_mlflow):
        mock_mlflow = MagicMock()
        mock_run = MagicMock()
        mock_run.info.run_id = "run_fv"
        mock_mlflow.active_run.return_value = mock_run
        mock_get_mlflow.return_value = mock_mlflow

        from feast.mlflow_integration.logger import log_feature_retrieval_to_mlflow

        result = log_feature_retrieval_to_mlflow(
            feature_refs=["fv_a:feat1", "fv_b:feat2", "fv_a:feat3"],
            entity_count=10,
            duration_seconds=0.1,
        )

        assert result is True
        client = mock_mlflow.MlflowClient()
        client.set_tag.assert_any_call("run_fv", "feast.feature_views", "fv_a,fv_b")


# ---------------------------------------------------------------------------
# Tests for model_resolver.py
# ---------------------------------------------------------------------------
class TestResolveFeatureService:

    def test_invalid_uri_raises(self):
        from feast.mlflow_integration.model_resolver import (
            FeastMlflowModelResolutionError,
            resolve_feature_service_from_model_uri,
        )

        with pytest.raises(FeastMlflowModelResolutionError, match="Invalid model_uri"):
            resolve_feature_service_from_model_uri("bad-uri")

    @patch("mlflow.MlflowClient")
    def test_resolves_from_tag(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_mv = MagicMock()
        mock_mv.tags = {"feast.feature_service": "my_fraud_svc"}
        mock_mv.version = "1"
        mock_client.get_model_version.return_value = mock_mv

        from feast.mlflow_integration.model_resolver import resolve_feature_service_from_model_uri

        assert resolve_feature_service_from_model_uri("models:/fraud-model/1") == "my_fraud_svc"

    @patch("mlflow.MlflowClient")
    def test_falls_back_to_convention(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_mv = MagicMock()
        mock_mv.tags = {}
        mock_mv.version = "3"
        mock_client.get_model_version.return_value = mock_mv

        from feast.mlflow_integration.model_resolver import resolve_feature_service_from_model_uri

        assert resolve_feature_service_from_model_uri("models:/fraud-model/3") == "fraud-model_v3"

    @patch("mlflow.MlflowClient")
    def test_resolves_alias(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_mv = MagicMock()
        mock_mv.tags = {"feast.feature_service": "prod_features"}
        mock_mv.version = "5"
        mock_client.get_model_version_by_alias.return_value = mock_mv

        from feast.mlflow_integration.model_resolver import resolve_feature_service_from_model_uri

        assert resolve_feature_service_from_model_uri("models:/fraud-model/Production") == "prod_features"

    @patch("mlflow.MlflowClient")
    def test_validates_against_store(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_mv = MagicMock()
        mock_mv.tags = {"feast.feature_service": "nonexistent_svc"}
        mock_mv.version = "1"
        mock_mv.run_id = "run_123"
        mock_client.get_model_version.return_value = mock_mv
        mock_client.download_artifacts.side_effect = Exception("no artifact")

        mock_store = MagicMock()
        mock_store.get_feature_service.side_effect = Exception("not found")

        from feast.mlflow_integration.model_resolver import (
            FeastMlflowModelResolutionError,
            resolve_feature_service_from_model_uri,
        )

        with pytest.raises(FeastMlflowModelResolutionError, match="not found in the Feast registry"):
            resolve_feature_service_from_model_uri("models:/fraud-model/1", store=mock_store)


# ---------------------------------------------------------------------------
# Tests for entity_df_builder.py
# ---------------------------------------------------------------------------
class TestGetEntityDfFromMlflowRun:

    @patch("mlflow.MlflowClient")
    def test_loads_parquet_artifact(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_run = MagicMock()
        mock_run.data.params = {}
        mock_client.get_run.return_value = mock_run

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "entity_df.parquet")
            df = pd.DataFrame({
                "user_id": [1, 2, 3],
                "event_timestamp": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            })
            df.to_parquet(parquet_path)
            mock_client.download_artifacts.return_value = parquet_path

            from feast.mlflow_integration.entity_df_builder import get_entity_df_from_mlflow_run

            result = get_entity_df_from_mlflow_run("run_abc")
            assert len(result) == 3
            assert "event_timestamp" in result.columns

    @patch("mlflow.MlflowClient")
    def test_raises_when_no_entity_data(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_run = MagicMock()
        mock_run.data.params = {}
        mock_client.get_run.return_value = mock_run
        mock_client.download_artifacts.side_effect = Exception("not found")

        from feast.mlflow_integration.entity_df_builder import (
            FeastMlflowEntityDfError, get_entity_df_from_mlflow_run,
        )

        with pytest.raises(FeastMlflowEntityDfError, match="No entity data found"):
            get_entity_df_from_mlflow_run("run_abc")

    @patch("mlflow.MlflowClient")
    def test_raises_when_timestamp_col_missing(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_run = MagicMock()
        mock_run.data.params = {}
        mock_client.get_run.return_value = mock_run

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "entity_df.parquet")
            pd.DataFrame({"user_id": [1, 2], "ts": [1, 2]}).to_parquet(parquet_path)
            mock_client.download_artifacts.return_value = parquet_path

            from feast.mlflow_integration.entity_df_builder import (
                FeastMlflowEntityDfError, get_entity_df_from_mlflow_run,
            )

            with pytest.raises(FeastMlflowEntityDfError, match="missing required timestamp"):
                get_entity_df_from_mlflow_run("run_abc")

    @patch("mlflow.MlflowClient")
    def test_loads_from_param_path(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.download_artifacts.side_effect = Exception("no artifact")

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "entities.parquet")
            pd.DataFrame({
                "driver_id": [10, 20],
                "event_timestamp": pd.to_datetime(["2026-03-01", "2026-03-02"]),
            }).to_parquet(parquet_path)

            mock_run = MagicMock()
            mock_run.data.params = {"feast.entity_df_path": parquet_path}
            mock_client.get_run.return_value = mock_run

            from feast.mlflow_integration.entity_df_builder import get_entity_df_from_mlflow_run

            result = get_entity_df_from_mlflow_run("run_param")
            assert len(result) == 2


# ---------------------------------------------------------------------------
# Tests for config.py
# ---------------------------------------------------------------------------
class TestLogTrainingDataset:
    """Tests for feast.mlflow_integration.logger.log_training_dataset_to_mlflow."""

    def setup_method(self):
        import feast.mlflow_integration.logger as mod

        mod._mlflow = None
        mod._mlflow_checked = False

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_logs_dataset_when_active_run(self, mock_get_mlflow):
        mock_mlflow = MagicMock()
        mock_run = MagicMock()
        mock_mlflow.active_run.return_value = mock_run
        mock_dataset = MagicMock()
        mock_mlflow.data.from_pandas.return_value = mock_dataset
        mock_get_mlflow.return_value = mock_mlflow

        from feast.mlflow_integration.logger import log_training_dataset_to_mlflow

        df = pd.DataFrame({"user_id": [1, 2], "feature": [0.5, 0.8]})
        result = log_training_dataset_to_mlflow(df, dataset_name="my_data")

        assert result is True
        mock_mlflow.data.from_pandas.assert_called_once()
        mock_mlflow.log_input.assert_called_once_with(mock_dataset, context="training")

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_noop_when_no_active_run(self, mock_get_mlflow):
        mock_mlflow = MagicMock()
        mock_mlflow.active_run.return_value = None
        mock_get_mlflow.return_value = mock_mlflow

        from feast.mlflow_integration.logger import log_training_dataset_to_mlflow

        df = pd.DataFrame({"a": [1]})
        result = log_training_dataset_to_mlflow(df)
        assert result is False

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_noop_when_mlflow_not_installed(self, mock_get_mlflow):
        mock_get_mlflow.return_value = None

        from feast.mlflow_integration.logger import log_training_dataset_to_mlflow

        df = pd.DataFrame({"a": [1]})
        result = log_training_dataset_to_mlflow(df)
        assert result is False

    @patch("feast.mlflow_integration.logger._get_mlflow")
    def test_handles_exception_gracefully(self, mock_get_mlflow):
        mock_mlflow = MagicMock()
        mock_mlflow.active_run.return_value = MagicMock()
        mock_mlflow.data.from_pandas.side_effect = Exception("dataset error")
        mock_get_mlflow.return_value = mock_mlflow

        from feast.mlflow_integration.logger import log_training_dataset_to_mlflow

        df = pd.DataFrame({"a": [1]})
        result = log_training_dataset_to_mlflow(df)
        assert result is False


# ---------------------------------------------------------------------------
# Tests for config.py
# ---------------------------------------------------------------------------
class TestMlflowConfig:

    def test_defaults(self):
        from feast.mlflow_integration.config import MlflowConfig

        cfg = MlflowConfig()
        assert cfg.enabled is False
        assert cfg.tracking_uri is None
        assert cfg.auto_log is True
        assert cfg.auto_log_dataset is False

    def test_from_dict(self):
        from feast.mlflow_integration.config import MlflowConfig

        cfg = MlflowConfig(enabled=True, tracking_uri="http://localhost:5000", auto_log=False, auto_log_dataset=True)
        assert cfg.enabled is True
        assert cfg.tracking_uri == "http://localhost:5000"
        assert cfg.auto_log is False
        assert cfg.auto_log_dataset is True
