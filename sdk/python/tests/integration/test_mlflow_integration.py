# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from feast import Entity, FeatureService, FeatureStore, FeatureView, Field, FileSource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64

mlflow = pytest.importorskip("mlflow", reason="mlflow is not installed")
from mlflow.tracking import MlflowClient  # noqa: E402

from feast.mlflow_integration import (  # noqa: E402
    FeastMlflowEntityDfError,
    FeastMlflowModelResolutionError,
    MlflowConfig,
    get_entity_df_from_mlflow_run,
    log_feature_retrieval_to_mlflow,
    log_training_dataset_to_mlflow,
    resolve_feature_service_from_model_uri,
)
from feast.mlflow_integration.config import (  # noqa: E402
    MLFLOW_TAG_TRUNCATION_LIMIT,
    resolve_tracking_uri,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_mlflow_globals():
    """Reset module-level mlflow caching between tests.

    feature_store.py caches _mlflow_log_fn globally; logger.py caches the
    mlflow module and failure counters. Without resetting, state leaks
    across tests.
    """
    import feast.feature_store as fs_mod
    import feast.mlflow_integration.logger as logger_mod

    fs_mod._mlflow_log_fn = None
    fs_mod._mlflow_log_fn_loaded = False
    logger_mod._mlflow = None
    logger_mod._mlflow_checked = False
    logger_mod._consecutive_failures = 0
    logger_mod._last_warning_time = 0.0
    yield


@pytest.fixture()
def tracking_uri(tmp_path):
    uri = str(tmp_path / "mlruns")
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment("test_mlflow")
    yield uri
    mlflow.set_tracking_uri("")


@pytest.fixture()
def driver_parquet(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    end = datetime.now().replace(microsecond=0, second=0, minute=0)
    start = end - timedelta(days=7)
    timestamps = pd.date_range(start, end, freq="h")
    driver_ids = [1001, 1002, 1003]

    np.random.seed(42)
    rows = [
        {
            "driver_id": did,
            "event_timestamp": ts,
            "created": ts,
            "conv_rate": float(np.random.uniform(0, 1)),
            "acc_rate": float(np.random.uniform(0, 1)),
            "avg_daily_trips": int(np.random.randint(1, 100)),
        }
        for ts in timestamps
        for did in driver_ids
    ]
    df = pd.DataFrame(rows)
    path = str(data_dir / "driver_stats.parquet")
    df.to_parquet(path)
    return tmp_path, path


@pytest.fixture()
def feast_objects(driver_parquet):
    _, parquet_path = driver_parquet

    driver = Entity(name="driver", join_keys=["driver_id"])
    source = FileSource(
        name="driver_stats_source",
        path=parquet_path,
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    fv = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        ttl=timedelta(days=7),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
        ],
        online=True,
        source=source,
    )
    fs = FeatureService(name="driver_activity_v1", features=[fv])
    return driver, source, fv, fs


def _make_store(
    tmp_path,
    tracking_uri,
    *,
    enabled=True,
    auto_log=True,
    auto_log_entity_df=True,
    entity_df_max_rows=100_000,
):
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)

    config = RepoConfig(
        project="test_mlflow",
        provider="local",
        registry=str(data_dir / "registry.db"),
        online_store=SqliteOnlineStoreConfig(path=str(data_dir / "online.db")),
        entity_key_serialization_version=3,
        mlflow=MlflowConfig(
            enabled=enabled,
            tracking_uri=tracking_uri,
            auto_log=auto_log,
            auto_log_entity_df=auto_log_entity_df,
            entity_df_max_rows=entity_df_max_rows,
        ),
    )
    return FeatureStore(config=config)


@pytest.fixture()
def store_enabled(driver_parquet, tracking_uri, feast_objects):
    tmp_path, _ = driver_parquet
    store = _make_store(tmp_path, tracking_uri)
    store.apply(list(feast_objects))
    store.materialize(
        start_date=datetime.now() - timedelta(days=7),
        end_date=datetime.now(),
    )
    return store


@pytest.fixture()
def entity_df():
    np.random.seed(42)
    n = 50
    return pd.DataFrame(
        {
            "driver_id": np.random.choice([1001, 1002, 1003], n),
            "event_timestamp": [
                datetime.now() - timedelta(hours=i % 48) for i in range(n)
            ],
            "label": np.random.randint(0, 2, n),
        }
    )


class TestMlflowConfig:
    @pytest.mark.integration
    def test_defaults(self):
        cfg = MlflowConfig()
        assert cfg.enabled is False
        assert cfg.auto_log is True
        assert cfg.auto_log_entity_df is False
        assert cfg.entity_df_max_rows == 100_000
        assert cfg.tracking_uri is None

    @pytest.mark.integration
    def test_get_tracking_uri_explicit(self):
        cfg = MlflowConfig(tracking_uri="http://example.com:5000")
        assert cfg.get_tracking_uri() == "http://example.com:5000"

    @pytest.mark.integration
    def test_get_tracking_uri_env_fallback(self, monkeypatch):
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://env-uri:5000")
        cfg = MlflowConfig()
        assert cfg.get_tracking_uri() == "http://env-uri:5000"

    @pytest.mark.integration
    def test_get_tracking_uri_none_when_unset(self, monkeypatch):
        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
        cfg = MlflowConfig()
        assert cfg.get_tracking_uri() is None

    @pytest.mark.integration
    def test_resolve_tracking_uri_priority(self, monkeypatch):
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://env:5000")
        assert resolve_tracking_uri("http://explicit:5000") == "http://explicit:5000"
        assert resolve_tracking_uri(None) == "http://env:5000"
        monkeypatch.delenv("MLFLOW_TRACKING_URI")
        assert resolve_tracking_uri(None) is None


class TestLogFeatureRetrieval:
    @pytest.mark.integration
    def test_logs_all_tags_and_metric(self, tracking_uri):
        client = MlflowClient(tracking_uri=tracking_uri)
        refs = [
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "transformed:conv_rate_plus_val1",
        ]

        with mlflow.start_run(run_name="test_tags") as run:
            result = log_feature_retrieval_to_mlflow(
                feature_refs=refs,
                entity_count=200,
                duration_seconds=0.1234,
                retrieval_type="historical",
                feature_service_name="driver_activity_v1",
                project="test_project",
                tracking_uri=tracking_uri,
            )

        assert result is True
        data = client.get_run(run.info.run_id)
        tags = data.data.tags

        assert tags["feast.project"] == "test_project"
        assert tags["feast.retrieval_type"] == "historical"
        assert tags["feast.feature_service"] == "driver_activity_v1"
        assert tags["feast.entity_count"] == "200"
        assert tags["feast.feature_count"] == "3"
        assert "driver_hourly_stats" in tags["feast.feature_views"]
        assert "transformed" in tags["feast.feature_views"]
        assert "driver_hourly_stats:conv_rate" in tags["feast.feature_refs"]
        assert data.data.metrics["feast.job_submission_sec"] == 0.1234

    @pytest.mark.integration
    def test_noop_without_active_run(self, tracking_uri):
        result = log_feature_retrieval_to_mlflow(
            feature_refs=["fv:feat"],
            entity_count=1,
            duration_seconds=0.01,
            tracking_uri=tracking_uri,
        )
        assert result is False

    @pytest.mark.integration
    def test_feature_views_sorted_and_deduped(self, tracking_uri):
        client = MlflowClient(tracking_uri=tracking_uri)
        refs = ["z_view:f1", "a_view:f2", "z_view:f3", "a_view:f4"]

        with mlflow.start_run() as run:
            log_feature_retrieval_to_mlflow(
                feature_refs=refs,
                entity_count=1,
                duration_seconds=0.01,
                tracking_uri=tracking_uri,
            )

        tags = client.get_run(run.info.run_id).data.tags
        assert tags["feast.feature_views"] == "a_view,z_view"

    @pytest.mark.integration
    def test_truncation_for_long_refs(self, tracking_uri):
        client = MlflowClient(tracking_uri=tracking_uri)
        refs = [f"fv:feature_{i:04d}" for i in range(500)]

        with mlflow.start_run() as run:
            log_feature_retrieval_to_mlflow(
                feature_refs=refs,
                entity_count=1,
                duration_seconds=0.01,
                tracking_uri=tracking_uri,
            )

        tags = client.get_run(run.info.run_id).data.tags
        assert len(tags["feast.feature_refs"]) <= MLFLOW_TAG_TRUNCATION_LIMIT
        assert tags["feast.feature_refs"].endswith("...")

    @pytest.mark.integration
    def test_no_project_tag_when_project_is_none(self, tracking_uri):
        client = MlflowClient(tracking_uri=tracking_uri)

        with mlflow.start_run() as run:
            log_feature_retrieval_to_mlflow(
                feature_refs=["fv:f1"],
                entity_count=1,
                duration_seconds=0.01,
                project=None,
                tracking_uri=tracking_uri,
            )

        tags = client.get_run(run.info.run_id).data.tags
        assert "feast.project" not in tags

    @pytest.mark.integration
    def test_no_feature_service_tag_when_none(self, tracking_uri):
        client = MlflowClient(tracking_uri=tracking_uri)

        with mlflow.start_run() as run:
            log_feature_retrieval_to_mlflow(
                feature_refs=["fv:f1"],
                entity_count=1,
                duration_seconds=0.01,
                feature_service=None,
                feature_service_name=None,
                tracking_uri=tracking_uri,
            )

        tags = client.get_run(run.info.run_id).data.tags
        assert "feast.feature_service" not in tags


class TestLogTrainingDataset:
    @pytest.mark.integration
    def test_logs_dataset_input(self, tracking_uri):
        client = MlflowClient(tracking_uri=tracking_uri)
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

        with mlflow.start_run() as run:
            result = log_training_dataset_to_mlflow(df, dataset_name="test_ds")

        assert result is True
        run_data = client.get_run(run.info.run_id)
        assert len(run_data.inputs.dataset_inputs) > 0
        assert run_data.inputs.dataset_inputs[0].dataset.name == "test_ds"

    @pytest.mark.integration
    def test_noop_without_active_run(self, tracking_uri):
        df = pd.DataFrame({"a": [1]})
        assert log_training_dataset_to_mlflow(df) is False


class TestHistoricalAutoLog:
    @pytest.mark.integration
    def test_tags_logged_via_feature_service(
        self, store_enabled, entity_df, tracking_uri
    ):
        client = MlflowClient(tracking_uri=tracking_uri)

        with mlflow.start_run(run_name="hist_fs") as run:
            store_enabled.get_historical_features(
                features=store_enabled.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

        data = client.get_run(run.info.run_id)
        tags = data.data.tags

        assert tags["feast.project"] == "test_mlflow"
        assert tags["feast.retrieval_type"] == "historical"
        assert tags["feast.feature_service"] == "driver_activity_v1"
        assert "driver_hourly_stats" in tags["feast.feature_views"]
        assert tags["feast.entity_count"] == str(len(entity_df))
        assert int(tags["feast.feature_count"]) >= 3
        assert data.data.metrics["feast.job_submission_sec"] >= 0

    @pytest.mark.integration
    def test_tags_logged_via_feature_refs(self, store_enabled, entity_df, tracking_uri):
        client = MlflowClient(tracking_uri=tracking_uri)

        with mlflow.start_run(run_name="hist_refs") as run:
            store_enabled.get_historical_features(
                features=[
                    "driver_hourly_stats:conv_rate",
                    "driver_hourly_stats:acc_rate",
                ],
                entity_df=entity_df,
            ).to_df()

        tags = client.get_run(run.info.run_id).data.tags
        assert tags["feast.retrieval_type"] == "historical"
        assert tags["feast.feature_count"] == "2"
        assert "driver_hourly_stats:conv_rate" in tags["feast.feature_refs"]

    @pytest.mark.integration
    def test_entity_df_artifact_uploaded(self, store_enabled, entity_df, tracking_uri):
        client = MlflowClient(tracking_uri=tracking_uri)

        with mlflow.start_run(run_name="hist_artifact") as run:
            store_enabled.get_historical_features(
                features=store_enabled.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

        artifacts = [a.path for a in client.list_artifacts(run.info.run_id)]
        assert "entity_df.parquet" in artifacts

        params = client.get_run(run.info.run_id).data.params
        assert params["feast.entity_df_rows"] == str(len(entity_df))
        assert "driver_id" in params["feast.entity_df_columns"]
        assert "event_timestamp" in params["feast.entity_df_columns"]

        tags = client.get_run(run.info.run_id).data.tags
        assert tags["feast.entity_df_type"] == "dataframe"

    @pytest.mark.integration
    def test_entity_df_skipped_when_exceeds_max_rows(
        self, driver_parquet, tracking_uri, feast_objects
    ):
        tmp_path, _ = driver_parquet
        store = _make_store(tmp_path, tracking_uri, entity_df_max_rows=5)
        store.apply(list(feast_objects))

        client = MlflowClient(tracking_uri=tracking_uri)
        entity_df = pd.DataFrame(
            {
                "driver_id": [1001] * 10,
                "event_timestamp": [
                    datetime.now() - timedelta(hours=i) for i in range(10)
                ],
            }
        )

        with mlflow.start_run(run_name="hist_skip") as run:
            store.get_historical_features(
                features=store.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

        artifacts = [a.path for a in client.list_artifacts(run.info.run_id)]
        assert "entity_df.parquet" not in artifacts
        params = client.get_run(run.info.run_id).data.params
        assert params["feast.entity_df_rows"] == "10"

    @pytest.mark.integration
    def test_no_tags_without_active_run(self, store_enabled, entity_df):
        result = store_enabled.get_historical_features(
            features=store_enabled.get_feature_service("driver_activity_v1"),
            entity_df=entity_df,
        ).to_df()
        assert len(result) == len(entity_df)


class TestOnlineAutoLog:
    @pytest.mark.integration
    def test_tags_logged_for_online_retrieval(self, store_enabled, tracking_uri):
        client = MlflowClient(tracking_uri=tracking_uri)

        with mlflow.start_run(run_name="online") as run:
            store_enabled.get_online_features(
                features=store_enabled.get_feature_service("driver_activity_v1"),
                entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
            )

        data = client.get_run(run.info.run_id)
        tags = data.data.tags

        assert tags["feast.retrieval_type"] == "online"
        assert tags["feast.feature_service"] == "driver_activity_v1"
        assert tags["feast.entity_count"] == "2"
        assert int(tags["feast.feature_count"]) >= 3
        assert data.data.metrics["feast.job_submission_sec"] >= 0

    @pytest.mark.integration
    def test_entity_count_for_list_input(self, store_enabled, tracking_uri):
        client = MlflowClient(tracking_uri=tracking_uri)

        with mlflow.start_run(run_name="online_list") as run:
            store_enabled.get_online_features(
                features=["driver_hourly_stats:conv_rate"],
                entity_rows=[
                    {"driver_id": 1001},
                    {"driver_id": 1002},
                    {"driver_id": 1003},
                ],
            )

        tags = client.get_run(run.info.run_id).data.tags
        assert tags["feast.entity_count"] == "3"


class TestDisabledIntegration:
    @pytest.mark.integration
    def test_disabled_does_not_log(
        self, driver_parquet, tracking_uri, feast_objects, entity_df
    ):
        tmp_path, _ = driver_parquet
        store = _make_store(tmp_path, tracking_uri, enabled=False)
        store.apply(list(feast_objects))

        client = MlflowClient(tracking_uri=tracking_uri)
        with mlflow.start_run(run_name="disabled") as run:
            store.get_historical_features(
                features=store.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

        tags = client.get_run(run.info.run_id).data.tags
        assert "feast.project" not in tags
        assert "feast.feature_service" not in tags

    @pytest.mark.integration
    def test_auto_log_false_does_not_log(
        self, driver_parquet, tracking_uri, feast_objects, entity_df
    ):
        tmp_path, _ = driver_parquet
        store = _make_store(tmp_path, tracking_uri, enabled=True, auto_log=False)
        store.apply(list(feast_objects))

        client = MlflowClient(tracking_uri=tracking_uri)
        with mlflow.start_run(run_name="no_auto_log") as run:
            store.get_historical_features(
                features=store.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

        tags = client.get_run(run.info.run_id).data.tags
        assert "feast.project" not in tags

    @pytest.mark.integration
    def test_auto_log_entity_df_false_skips_artifact(
        self, driver_parquet, tracking_uri, feast_objects, entity_df
    ):
        tmp_path, _ = driver_parquet
        store = _make_store(tmp_path, tracking_uri, auto_log_entity_df=False)
        store.apply(list(feast_objects))

        client = MlflowClient(tracking_uri=tracking_uri)
        with mlflow.start_run(run_name="no_entity_df") as run:
            store.get_historical_features(
                features=store.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

        run_data = client.get_run(run.info.run_id).data
        artifacts = [a.path for a in client.list_artifacts(run.info.run_id)]
        assert "entity_df.parquet" not in artifacts

        assert "feast.entity_df_rows" in run_data.params
        assert run_data.tags["feast.entity_df_type"] == "dataframe"
        assert run_data.tags["feast.feature_service"] == "driver_activity_v1"


class TestEntityDfBuilder:
    @pytest.mark.integration
    def test_roundtrip_parquet(self, store_enabled, entity_df, tracking_uri):
        with mlflow.start_run(run_name="roundtrip") as run:
            store_enabled.get_historical_features(
                features=store_enabled.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

        recovered = get_entity_df_from_mlflow_run(
            run_id=run.info.run_id,
            tracking_uri=tracking_uri,
        )

        assert recovered.shape == entity_df.shape
        assert set(recovered.columns) == set(entity_df.columns)
        assert "event_timestamp" in recovered.columns

    @pytest.mark.integration
    def test_max_rows_limits_output(self, store_enabled, entity_df, tracking_uri):
        with mlflow.start_run(run_name="max_rows") as run:
            store_enabled.get_historical_features(
                features=store_enabled.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

        recovered = get_entity_df_from_mlflow_run(
            run_id=run.info.run_id,
            tracking_uri=tracking_uri,
            max_rows=5,
        )
        assert len(recovered) == 5

    @pytest.mark.integration
    def test_missing_artifact_raises(self, tracking_uri):
        with mlflow.start_run(run_name="empty") as run:
            mlflow.log_param("dummy", "value")

        with pytest.raises(FeastMlflowEntityDfError, match="No entity data found"):
            get_entity_df_from_mlflow_run(
                run_id=run.info.run_id,
                tracking_uri=tracking_uri,
            )

    @pytest.mark.integration
    def test_nonexistent_run_raises(self, tracking_uri):
        with pytest.raises(FeastMlflowEntityDfError, match="not found"):
            get_entity_df_from_mlflow_run(
                run_id="0000000000000000deadbeef00000000",
                tracking_uri=tracking_uri,
            )

    @pytest.mark.integration
    def test_missing_timestamp_column_raises(self, tracking_uri):
        df = pd.DataFrame({"driver_id": [1001], "value": [0.5]})
        with mlflow.start_run(run_name="bad_cols") as run:
            import tempfile

            with tempfile.TemporaryDirectory() as tmp_dir:
                path = os.path.join(tmp_dir, "entity_df.parquet")
                df.to_parquet(path, index=False)
                mlflow.log_artifact(path)

        with pytest.raises(
            FeastMlflowEntityDfError, match="missing required timestamp"
        ):
            get_entity_df_from_mlflow_run(
                run_id=run.info.run_id,
                tracking_uri=tracking_uri,
            )

    @pytest.mark.integration
    def test_custom_timestamp_column(self, tracking_uri):
        df = pd.DataFrame(
            {
                "driver_id": [1001],
                "ts": [datetime.now()],
            }
        )
        with mlflow.start_run(run_name="custom_ts") as run:
            import tempfile

            with tempfile.TemporaryDirectory() as tmp_dir:
                path = os.path.join(tmp_dir, "entity_df.parquet")
                df.to_parquet(path, index=False)
                mlflow.log_artifact(path)

        recovered = get_entity_df_from_mlflow_run(
            run_id=run.info.run_id,
            tracking_uri=tracking_uri,
            timestamp_column="ts",
        )
        assert len(recovered) == 1
        assert "ts" in recovered.columns


class TestModelResolver:
    def _train_and_register(self, store, entity_df, tracking_uri, model_name):
        """Train inside an mlflow run, log a model, register it."""
        from sklearn.linear_model import LogisticRegression

        with mlflow.start_run(run_name=f"train_{model_name}") as run:
            store.get_historical_features(
                features=store.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

            model = LogisticRegression()
            model.fit([[0, 0], [1, 1]], [0, 1])
            mlflow.sklearn.log_model(model, "model")

        mlflow.register_model(f"runs:/{run.info.run_id}/model", model_name)
        return run.info.run_id

    @pytest.mark.integration
    def test_resolve_from_run_tags(self, store_enabled, entity_df, tracking_uri):
        self._train_and_register(
            store_enabled, entity_df, tracking_uri, "test_resolve_run_tags"
        )

        fs_name = resolve_feature_service_from_model_uri(
            model_uri="models:/test_resolve_run_tags/1",
            store=store_enabled,
            tracking_uri=tracking_uri,
        )
        assert fs_name == "driver_activity_v1"

    @pytest.mark.integration
    def test_resolve_from_model_version_tag(
        self, store_enabled, entity_df, tracking_uri
    ):
        self._train_and_register(
            store_enabled, entity_df, tracking_uri, "test_resolve_mv_tag"
        )

        client = MlflowClient(tracking_uri=tracking_uri)
        client.set_model_version_tag(
            "test_resolve_mv_tag", "1", "feast.feature_service", "overridden_service"
        )

        fs_name = resolve_feature_service_from_model_uri(
            model_uri="models:/test_resolve_mv_tag/1",
            tracking_uri=tracking_uri,
        )
        assert fs_name == "overridden_service"

    @pytest.mark.integration
    def test_model_version_tag_takes_priority_over_run_tag(
        self, store_enabled, entity_df, tracking_uri
    ):
        self._train_and_register(
            store_enabled, entity_df, tracking_uri, "test_priority"
        )

        client = MlflowClient(tracking_uri=tracking_uri)
        client.set_model_version_tag(
            "test_priority", "1", "feast.feature_service", "explicit_override"
        )

        fs_name = resolve_feature_service_from_model_uri(
            model_uri="models:/test_priority/1",
            store=None,
            tracking_uri=tracking_uri,
        )
        assert fs_name == "explicit_override"

    @pytest.mark.integration
    def test_validates_feature_service_exists(
        self, store_enabled, entity_df, tracking_uri
    ):
        self._train_and_register(
            store_enabled, entity_df, tracking_uri, "test_validate_exists"
        )

        client = MlflowClient(tracking_uri=tracking_uri)
        client.set_model_version_tag(
            "test_validate_exists",
            "1",
            "feast.feature_service",
            "nonexistent_service",
        )

        with pytest.raises(
            FeastMlflowModelResolutionError, match="not found in the Feast registry"
        ):
            resolve_feature_service_from_model_uri(
                model_uri="models:/test_validate_exists/1",
                store=store_enabled,
                tracking_uri=tracking_uri,
            )

    @pytest.mark.integration
    def test_invalid_uri_raises(self, tracking_uri):
        with pytest.raises(FeastMlflowModelResolutionError, match="Invalid model_uri"):
            resolve_feature_service_from_model_uri(
                model_uri="not-a-valid-uri",
                tracking_uri=tracking_uri,
            )

    @pytest.mark.integration
    def test_nonexistent_model_raises(self, tracking_uri):
        with pytest.raises(FeastMlflowModelResolutionError, match="Could not resolve"):
            resolve_feature_service_from_model_uri(
                model_uri="models:/does_not_exist/1",
                tracking_uri=tracking_uri,
            )

    @pytest.mark.integration
    def test_no_feast_tag_anywhere_raises(self, tracking_uri):
        from sklearn.linear_model import LogisticRegression

        mlflow.set_experiment("test_mlflow")
        with mlflow.start_run(run_name="no_feast_tags") as run:
            model = LogisticRegression()
            model.fit([[0], [1]], [0, 1])
            mlflow.sklearn.log_model(model, "model")

        mlflow.register_model(f"runs:/{run.info.run_id}/model", "test_no_feast_tag")

        with pytest.raises(
            FeastMlflowModelResolutionError,
            match="Could not determine feature service",
        ):
            resolve_feature_service_from_model_uri(
                model_uri="models:/test_no_feast_tag/1",
                tracking_uri=tracking_uri,
            )

    @pytest.mark.integration
    def test_feature_mismatch_with_required_features_artifact(
        self, store_enabled, entity_df, tracking_uri
    ):
        from sklearn.linear_model import LogisticRegression

        with mlflow.start_run(run_name="mismatch") as run:
            store_enabled.get_historical_features(
                features=store_enabled.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

            model = LogisticRegression()
            model.fit([[0], [1]], [0, 1])
            mlflow.sklearn.log_model(model, "model")

            import tempfile

            with tempfile.TemporaryDirectory() as tmp_dir:
                path = os.path.join(tmp_dir, "required_features.json")
                with open(path, "w") as f:
                    json.dump(["driver_hourly_stats:nonexistent_feature"], f)
                mlflow.log_artifact(path)

        mlflow.register_model(f"runs:/{run.info.run_id}/model", "test_mismatch")

        with pytest.raises(FeastMlflowModelResolutionError, match="Feature mismatch"):
            resolve_feature_service_from_model_uri(
                model_uri="models:/test_mismatch/1",
                store=store_enabled,
                tracking_uri=tracking_uri,
            )

    @pytest.mark.integration
    def test_skips_validation_when_no_store(
        self, store_enabled, entity_df, tracking_uri
    ):
        self._train_and_register(
            store_enabled, entity_df, tracking_uri, "test_no_store"
        )

        client = MlflowClient(tracking_uri=tracking_uri)
        client.set_model_version_tag(
            "test_no_store",
            "1",
            "feast.feature_service",
            "anything_goes",
        )

        fs_name = resolve_feature_service_from_model_uri(
            model_uri="models:/test_no_store/1",
            store=None,
            tracking_uri=tracking_uri,
        )
        assert fs_name == "anything_goes"


class TestEndToEnd:
    @pytest.mark.integration
    def test_full_lifecycle(self, store_enabled, entity_df, tracking_uri):
        from sklearn.linear_model import LogisticRegression

        # Phase 1: Train and auto-log
        with mlflow.start_run(run_name="e2e_train") as train_run:
            training_df = store_enabled.get_historical_features(
                features=store_enabled.get_feature_service("driver_activity_v1"),
                entity_df=entity_df,
            ).to_df()

            X = training_df[["conv_rate", "acc_rate", "avg_daily_trips"]].fillna(0)
            y = entity_df["label"].values[: len(X)]
            model = LogisticRegression().fit(X, y)
            mlflow.sklearn.log_model(model, "model")

        # Phase 2: Register
        mlflow.register_model(f"runs:/{train_run.info.run_id}/model", "e2e_model")

        # Phase 3: Resolve
        fs_name = resolve_feature_service_from_model_uri(
            model_uri="models:/e2e_model/1",
            store=store_enabled,
            tracking_uri=tracking_uri,
        )
        assert fs_name == "driver_activity_v1"

        # Phase 4: Online features with resolved service
        with mlflow.start_run(run_name="e2e_serve") as serve_run:
            store_enabled.get_online_features(
                features=store_enabled.get_feature_service(fs_name),
                entity_rows=[{"driver_id": 1001}],
            )

        serve_tags = (
            MlflowClient(tracking_uri=tracking_uri)
            .get_run(serve_run.info.run_id)
            .data.tags
        )
        assert serve_tags["feast.retrieval_type"] == "online"
        assert serve_tags["feast.feature_service"] == "driver_activity_v1"

        # Phase 5: Reproduce
        recovered_df = get_entity_df_from_mlflow_run(
            run_id=train_run.info.run_id,
            tracking_uri=tracking_uri,
        )
        assert recovered_df.shape == entity_df.shape

        with mlflow.start_run(run_name="e2e_reproduce") as repro_run:
            store_enabled.get_historical_features(
                features=store_enabled.get_feature_service(fs_name),
                entity_df=recovered_df,
            ).to_df()

        repro_tags = (
            MlflowClient(tracking_uri=tracking_uri)
            .get_run(repro_run.info.run_id)
            .data.tags
        )
        assert repro_tags["feast.feature_service"] == "driver_activity_v1"
        assert repro_tags["feast.entity_count"] == str(len(entity_df))
