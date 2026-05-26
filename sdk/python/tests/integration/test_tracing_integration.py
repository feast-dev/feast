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

"""Integration tests for MLflow-native distributed tracing."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from feast import Entity, FeatureService, FeatureStore, FeatureView, Field, FileSource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64

mlflow = pytest.importorskip("mlflow", reason="mlflow is not installed")

import feast.mlflow as feast_mlflow  # noqa: E402
import feast.tracing as feast_tracing  # noqa: E402
from feast.mlflow_integration import MlflowConfig  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_globals():
    """Reset module-level state between tests."""
    feast_mlflow._client = None
    feast_mlflow._registered_store = None

    feast_tracing._initialized = False
    feast_tracing._enabled = False
    feast_tracing._mlflow_mod = None
    feast_tracing._HAS_DISTRIBUTED_CTX = False
    yield
    feast_tracing._initialized = False
    feast_tracing._enabled = False
    feast_tracing._mlflow_mod = None
    feast_tracing._HAS_DISTRIBUTED_CTX = False


@pytest.fixture()
def tracking_uri(tmp_path):
    uri = f"sqlite:///{tmp_path / 'mlflow.db'}"
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment("test_tracing")
    yield uri
    mlflow.set_tracking_uri("")


@pytest.fixture()
def driver_parquet(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    end = datetime.now().replace(microsecond=0, second=0, minute=0)
    start = end - timedelta(days=7)
    timestamps = pd.date_range(start, end, freq="h")
    driver_ids = [1001, 1002]

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


def _make_store(tmp_path, tracking_uri, *, enable_tracing=True):
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)

    config = RepoConfig(
        project="test_tracing",
        provider="local",
        registry=str(data_dir / "registry.db"),
        online_store=SqliteOnlineStoreConfig(path=str(data_dir / "online.db")),
        entity_key_serialization_version=3,
        mlflow=MlflowConfig(
            enabled=True,
            tracking_uri=tracking_uri,
            auto_log=True,
            enable_tracing=enable_tracing,
        ),
    )
    return FeatureStore(config=config)


@pytest.fixture()
def store_with_tracing(driver_parquet, tracking_uri, feast_objects):
    tmp_path, _ = driver_parquet
    store = _make_store(tmp_path, tracking_uri, enable_tracing=True)
    store.apply(list(feast_objects))
    store.materialize(
        start_date=datetime.now() - timedelta(days=7),
        end_date=datetime.now(),
    )
    return store


# ---------------------------------------------------------------------------
# Tests — Initialization
# ---------------------------------------------------------------------------


class TestTracingInitialization:
    def test_lazy_init_enables_when_configured(
        self, driver_parquet, tracking_uri, feast_objects
    ):
        tmp_path, _ = driver_parquet
        store = _make_store(tmp_path, tracking_uri, enable_tracing=True)
        store.apply(list(feast_objects))
        result = feast_tracing._lazy_init(store)
        assert result is True
        assert feast_tracing._enabled is True
        assert feast_tracing._mlflow_mod is not None

    def test_lazy_init_disabled_when_tracing_off(
        self, driver_parquet, tracking_uri, feast_objects
    ):
        tmp_path, _ = driver_parquet
        store = _make_store(tmp_path, tracking_uri, enable_tracing=False)
        result = feast_tracing._lazy_init(store)
        assert result is False


# ---------------------------------------------------------------------------
# Tests — Online features + trace context
# ---------------------------------------------------------------------------


class TestOnlineFeatureTracing:
    def test_trace_context_populated_after_retrieval(self, store_with_tracing):
        from feast.tracing_context import feast_trace_scope

        with feast_trace_scope() as ctx:
            store_with_tracing.get_online_features(
                features=["driver_hourly_stats:conv_rate"],
                entity_rows=[{"driver_id": 1001}],
            )
            assert len(ctx.feature_refs) > 0
            assert any("conv_rate" in ref for ref in ctx.feature_refs)

    def test_trace_context_with_multiple_features(self, store_with_tracing):
        from feast.tracing_context import feast_trace_scope

        with feast_trace_scope() as ctx:
            store_with_tracing.get_online_features(
                features=[
                    "driver_hourly_stats:conv_rate",
                    "driver_hourly_stats:acc_rate",
                ],
                entity_rows=[{"driver_id": 1001}],
            )
            assert len(ctx.feature_refs) >= 2
            assert ctx.feature_views == {"driver_hourly_stats"}

    def test_trace_context_with_feature_service(self, store_with_tracing):
        from feast.tracing_context import feast_trace_scope

        with feast_trace_scope() as ctx:
            fs = store_with_tracing.get_feature_service("driver_activity_v1")
            store_with_tracing.get_online_features(
                features=fs,
                entity_rows=[{"driver_id": 1001}],
            )
            assert len(ctx.feature_refs) > 0
            assert ctx.feature_service == "driver_activity_v1"

    def test_get_online_features_returns_data(self, store_with_tracing):
        response = store_with_tracing.get_online_features(
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
            ],
            entity_rows=[{"driver_id": 1001}],
        )
        result = response.to_dict()
        assert "driver_id" in result
        assert "conv_rate" in result

    def test_no_context_when_scope_not_active(self, store_with_tracing):
        """Retrieval without feast_trace_scope should not crash."""
        from feast.tracing_context import get_current_context

        store_with_tracing.get_online_features(
            features=["driver_hourly_stats:conv_rate"],
            entity_rows=[{"driver_id": 1001}],
        )
        assert get_current_context() is None

    def test_multiple_retrievals_accumulate(self, store_with_tracing):
        from feast.tracing_context import feast_trace_scope

        with feast_trace_scope() as ctx:
            store_with_tracing.get_online_features(
                features=["driver_hourly_stats:conv_rate"],
                entity_rows=[{"driver_id": 1001}],
            )
            store_with_tracing.get_online_features(
                features=["driver_hourly_stats:acc_rate"],
                entity_rows=[{"driver_id": 1002}],
            )
            assert len(ctx.feature_refs) >= 2


# ---------------------------------------------------------------------------
# Tests — Historical features + trace context
# ---------------------------------------------------------------------------


class TestHistoricalFeatureTracing:
    def test_trace_context_populated_after_historical_retrieval(
        self, store_with_tracing
    ):
        from feast.tracing_context import feast_trace_scope

        entity_df = pd.DataFrame(
            {
                "driver_id": [1001],
                "event_timestamp": [datetime.now() - timedelta(hours=1)],
            }
        )

        with feast_trace_scope() as ctx:
            store_with_tracing.get_historical_features(
                entity_df=entity_df,
                features=["driver_hourly_stats:conv_rate"],
            ).to_df()
            assert len(ctx.feature_refs) > 0
            assert any("conv_rate" in ref for ref in ctx.feature_refs)

    def test_historical_with_feature_service(self, store_with_tracing):
        from feast.tracing_context import feast_trace_scope

        entity_df = pd.DataFrame(
            {
                "driver_id": [1001],
                "event_timestamp": [datetime.now() - timedelta(hours=1)],
            }
        )

        with feast_trace_scope() as ctx:
            fs = store_with_tracing.get_feature_service("driver_activity_v1")
            store_with_tracing.get_historical_features(
                entity_df=entity_df,
                features=fs,
            ).to_df()
            assert len(ctx.feature_refs) > 0
            assert ctx.feature_service == "driver_activity_v1"


# ---------------------------------------------------------------------------
# Tests — Span attribute output format
# ---------------------------------------------------------------------------


class TestSpanAttributeFormat:
    def test_context_attributes_match_expected_schema(self, store_with_tracing):
        from feast.tracing_context import feast_trace_scope

        with feast_trace_scope() as ctx:
            fs = store_with_tracing.get_feature_service("driver_activity_v1")
            store_with_tracing.get_online_features(
                features=fs,
                entity_rows=[{"driver_id": 1001}],
            )

            attrs = ctx.get_context_attributes()
            assert "feast.context_features" in attrs
            assert "feast.context_feature_count" in attrs
            assert "feast.context_feature_views" in attrs
            assert "feast.context_feature_service" in attrs
            assert attrs["feast.context_feature_service"] == "driver_activity_v1"
            assert int(attrs["feast.context_feature_count"]) > 0
