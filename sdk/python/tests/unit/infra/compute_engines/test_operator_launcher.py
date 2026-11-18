"""Real end-to-end coverage for the operator engines' in-pod driver.

The operator-backed engines (`flink_application`, `spark_application`) submit a
Kubernetes custom resource whose pod runs `operator.launcher.run_materialization`
against the configured stores. The engine-level unit tests mock Kubernetes and
only inspect manifests/status/cleanup, so they can pass even if the actual
materialization is broken. These tests exercise the real launcher path:
`run_materialization` → `materialize_single_feature_view` → online write +
`apply_materialization`, and assert online values, registry intervals, and
failure reporting.

- `local` engine: always runs (the launcher loop is engine-agnostic).
- `flink.engine`: the real in-pod engine; gated on PyFlink being installed (the
  `flink` extra is a source-only install, so CI skips it until a Flink job
  exists). This is the regression that catches `flink.engine` breakage.
"""

import importlib.util
import os
import tempfile
from datetime import datetime, timedelta

import pytest

from feast import Entity, FeatureStore, FeatureView, FileSource, RepoConfig
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.field import Field
from feast.infra.compute_engines.operator.launcher import run_materialization
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.types import Float32, Int64, ValueType

_PYFLINK = importlib.util.find_spec("pyflink") is not None


def _make_store(
    data_dir: str, batch_engine=None
) -> tuple[FeatureStore, datetime, datetime]:
    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_df = create_driver_hourly_stats_df([1001, 1002], start_date, end_date)
    path = os.path.join(data_dir, "driver_stats.parquet")
    driver_df.to_parquet(path=path, allow_truncated_timestamps=True)

    repo_kwargs = dict(
        project="operator_launcher_test",
        registry=os.path.join(data_dir, "registry.db"),
        provider="local",
        entity_key_serialization_version=3,
        online_store=SqliteOnlineStoreConfig(path=os.path.join(data_dir, "online.db")),
    )
    if batch_engine is not None:  # omit so RepoConfig defaults to the local engine
        repo_kwargs["batch_engine"] = batch_engine
    store = FeatureStore(config=RepoConfig(**repo_kwargs))

    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        ttl=timedelta(days=30),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
        ],
        online=True,
        source=FileSource(
            name="driver_hourly_stats_source",
            path=path,
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
        ),
    )
    store.apply([driver, fv])
    return store, start_date, end_date


def _mat_config(fv_name: str, start: datetime, end: datetime) -> dict:
    return {
        "operation": "materialize",
        "tasks": [
            {
                "feature_view": fv_name,
                "start_time": start.isoformat(),
                "end_time": end.isoformat(),
            }
        ],
    }


def _run_and_assert_materialized(batch_engine) -> None:
    with tempfile.TemporaryDirectory() as data_dir:
        store, start, end = _make_store(data_dir, batch_engine=batch_engine)
        feast_config = store.config.model_dump(by_alias=True, mode="json")

        # This is exactly what the operator engines run inside the pod.
        run_materialization(
            feast_config, _mat_config("driver_hourly_stats", start, end)
        )

        # 1. Features were written to the online store.
        online = store.get_online_features(
            features=["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"],
            entity_rows=[{"driver_id": 1001}],
        ).to_dict()
        assert online["conv_rate"][0] is not None
        assert online["acc_rate"][0] is not None

        # 2. Registry materialization interval was committed.
        fv = store.get_feature_view("driver_hourly_stats")
        assert len(fv.materialization_intervals) >= 1


def test_launcher_materializes_local_engine() -> None:
    """Launcher writes online values and commits a registry interval (local engine)."""
    _run_and_assert_materialized(batch_engine=None)  # defaults to the local engine


@pytest.mark.skipif(not _PYFLINK, reason="requires the `flink` extra (PyFlink)")
def test_launcher_materializes_flink_engine() -> None:
    """Same path via the real in-pod `flink.engine` (PyFlink-gated)."""
    _run_and_assert_materialized(batch_engine={"type": "flink.engine"})


def test_launcher_reports_failure_and_exits_nonzero() -> None:
    """A feature view that cannot be materialized is reported as a failure and the
    driver exits non-zero so the operator marks the job FAILED."""
    with tempfile.TemporaryDirectory() as data_dir:
        store, start, end = _make_store(data_dir, batch_engine=None)
        feast_config = store.config.model_dump(by_alias=True, mode="json")

        with pytest.raises(SystemExit) as exc:
            run_materialization(feast_config, _mat_config("does_not_exist", start, end))
        assert exc.value.code == 1
