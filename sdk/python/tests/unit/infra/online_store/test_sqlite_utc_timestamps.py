"""Regression tests for the SQLite online store's UTC timestamp handling.

Feast treats naive datetimes as UTC (see ``to_naive_utc`` and
``make_tzaware``), so the SQLite datetime <-> epoch adapters must round-trip
UTC regardless of the process timezone. These tests flip the process TZ
between the write and read sides to cover hosts in different timezones.
"""

import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import pytest

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.infra.online_stores.helpers import compute_table_id
from feast.infra.online_stores.sqlite import (
    SqliteOnlineStore,
    SqliteOnlineStoreConfig,
    adapt_datetime_epoch,
    convert_timestamp,
)
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Int64
from feast.utils import _populate_response_from_feature_data
from feast.value_type import ValueType

TRIPS_VALUE = 7


@pytest.fixture
def clean_tz_env():
    """Restore the process TZ to whatever the test found, set or unset."""
    original: str | None = os.environ.get("TZ")
    try:
        yield
    finally:
        if original is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = original
        time.tzset()


def _entity_key(driver_id: int) -> EntityKeyProto:
    return EntityKeyProto(
        join_keys=["driver_id"], entity_values=[ValueProto(int64_val=driver_id)]
    )


def _set_process_tz(name: str) -> None:
    os.environ["TZ"] = name
    time.tzset()


def _stored_epochs(config: RepoConfig, view: FeatureView):
    """Read the raw on-disk event_ts/created_ts values without converters."""
    conn = sqlite3.connect(config.online_store.path)
    try:
        return conn.execute(
            f"SELECT event_ts, created_ts FROM "
            f"{compute_table_id(config.project, view, config.registry.enable_online_feature_view_versioning)}"
        ).fetchone()
    finally:
        conn.close()


def _ttl_status(read_ts: datetime, view: FeatureView) -> FieldStatus.ValueType:
    """Run the production TTL check on one online_read row and return its status."""
    response = GetOnlineFeaturesResponse()
    _populate_response_from_feature_data(
        requested_features=["trips"],
        read_rows=[(read_ts, {"trips": ValueProto(int64_val=TRIPS_VALUE)})],
        indexes=([0],),
        online_features_response=response,
        full_feature_names=False,
        table=view,
        output_len=1,
    )
    return response.results[0].statuses[0]


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="time.tzset() is Unix-only")
@pytest.mark.parametrize(
    ("write_tz", "read_tz"),
    [
        ("UTC", "UTC"),
        ("Asia/Shanghai", "UTC"),
        ("UTC", "Asia/Shanghai"),
        ("America/New_York", "UTC"),
    ],
)
def test_sqlite_online_store_utc_epochs_across_timezones(
    tmp_path, clean_tz_env, write_tz, read_tz
):
    config = RepoConfig(
        project="tz_probe",
        registry=str(tmp_path / "registry.db"),
        provider="local",
        online_store=SqliteOnlineStoreConfig(path=str(tmp_path / "online.db")),
        offline_store={"type": "dask"},
        entity_key_serialization_version=3,
    )
    driver = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    view = FeatureView(
        name="driver_stats",
        entities=[driver],
        ttl=timedelta(hours=1),
        schema=[Field(name="trips", dtype=Int64)],
        source=FileSource(path=str(tmp_path / "unused.parquet"), timestamp_field="ts"),
    )
    # Whole seconds: the store persists timestamps as integer epochs.
    event_ts = (datetime.now(tz=timezone.utc) - timedelta(minutes=5)).replace(
        microsecond=0
    )
    created_ts = event_ts + timedelta(seconds=30)
    event_epoch = int(event_ts.timestamp())
    created_epoch = int(created_ts.timestamp())

    _set_process_tz(write_tz)
    store = SqliteOnlineStore()
    store.update(config, [], [view], [], [driver], partial=False)
    store.online_write_batch(
        config,
        view,
        [
            (
                _entity_key(1),
                {"trips": ValueProto(int64_val=TRIPS_VALUE)},
                event_ts,
                created_ts,
            )
        ],
        None,
    )

    stored_event_epoch, stored_created_epoch = _stored_epochs(config, view)
    assert stored_event_epoch == event_epoch
    assert stored_created_epoch == created_epoch

    # Serve the same database from a process running in another timezone.
    _set_process_tz(read_tz)
    read_ts, features = SqliteOnlineStore().online_read(config, view, [_entity_key(1)])[
        0
    ]
    assert features["trips"].int64_val == TRIPS_VALUE
    assert read_ts == event_ts
    assert _ttl_status(read_ts, view) == FieldStatus.PRESENT


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="time.tzset() is Unix-only")
def test_adapt_datetime_epoch_treats_naive_datetimes_as_utc(clean_tz_env):
    _set_process_tz("Asia/Shanghai")
    assert adapt_datetime_epoch(datetime(2024, 1, 1)) == 1704067200


def test_adapt_datetime_epoch_converts_non_utc_aware_datetimes():
    offset = timezone(timedelta(hours=8))
    assert adapt_datetime_epoch(datetime(2024, 1, 1, 8, tzinfo=offset)) == 1704067200


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="time.tzset() is Unix-only")
def test_convert_timestamp_returns_naive_utc(clean_tz_env):
    _set_process_tz("Asia/Shanghai")
    converted = convert_timestamp(b"1704067200")
    assert converted == datetime(2024, 1, 1)
    assert converted.tzinfo is None
