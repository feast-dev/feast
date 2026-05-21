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

"""Integration tests for OTEL tracing with MLflow backend."""

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

try:
    from opentelemetry import trace as otel_trace  # noqa: F401
    from opentelemetry.sdk.trace import TracerProvider  # noqa: F401
    from opentelemetry.sdk.trace.export.in_memory import (
        InMemorySpanExporter,  # type: ignore[import-untyped] # noqa: F401
    )

    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False

pytestmark = pytest.mark.skipif(not HAS_OTEL, reason="opentelemetry-sdk not installed")


import feast.mlflow  # noqa: E402
from feast.mlflow_integration import MlflowConfig  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_globals():
    """Reset module-level state between tests."""
    feast.mlflow._client = None
    feast.mlflow._registered_store = None

    import feast.tracing

    feast.tracing._initialized = False
    feast.tracing._enabled = False
    feast.tracing._tracer = None
    yield
    feast.tracing._initialized = False
    feast.tracing._enabled = False
    feast.tracing._tracer = None


@pytest.fixture()
def tracking_uri(tmp_path):
    uri = str(tmp_path / "mlruns")
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
# Tests
# ---------------------------------------------------------------------------


class TestTracingInitialization:
    def test_lazy_init_enables_when_configured(self, store_with_tracing):
        import feast.tracing

        result = feast.tracing._lazy_init(store_with_tracing)
        assert result is True
        assert feast.tracing._enabled is True

    def test_lazy_init_disabled_when_tracing_off(
        self, driver_parquet, tracking_uri, feast_objects
    ):
        import feast.tracing

        tmp_path, _ = driver_parquet
        store = _make_store(tmp_path, tracking_uri, enable_tracing=False)
        result = feast.tracing._lazy_init(store)
        assert result is False


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
