from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import ibis
import pandas as pd
import pytest

from feast import Entity, FeatureView, Field, ValueType
from feast.infra.offline_stores.contrib.mssql_offline_store.mssql import (
    MsSqlServerOfflineStore,
    MsSqlServerOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.mssql_offline_store.mssqlserver_source import (
    MsSqlServerSource,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64

START = datetime(2025, 7, 1, tzinfo=timezone.utc)
END = datetime(2025, 7, 10, 12, 0, tzinfo=timezone.utc)

driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
customer = Entity(
    name="customer", join_keys=["customer_id"], value_type=ValueType.INT64
)

driver_fv = FeatureView(
    name="driver_stats",
    entities=[driver],
    ttl=timedelta(days=2),
    schema=[
        Field(name="driver_id", dtype=Int64),
        Field(name="conv_rate", dtype=Float32),
    ],
    source=MsSqlServerSource(
        name="driver_source",
        table_ref="driver_stats",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    ),
)

customer_fv = FeatureView(
    name="customer_stats",
    entities=[customer],
    ttl=timedelta(days=5),
    schema=[
        Field(name="customer_id", dtype=Int64),
        Field(name="spend", dtype=Float32),
    ],
    source=MsSqlServerSource(
        name="customer_source",
        table_ref="customer_stats",
        event_timestamp_column="event_timestamp",
        field_mapping={"cust": "customer_id"},
    ),
)

global_fv = FeatureView(
    name="global_stats",
    entities=[],
    ttl=timedelta(days=1),
    schema=[Field(name="total", dtype=Int64)],
    source=MsSqlServerSource(
        name="global_source",
        table_ref="global_stats",
        event_timestamp_column="event_timestamp",
    ),
)

pair_fv = FeatureView(
    name="pair_stats",
    entities=[driver, customer],
    ttl=timedelta(days=5),
    schema=[
        Field(name="driver_id", dtype=Int64),
        Field(name="customer_id", dtype=Int64),
        Field(name="rating", dtype=Float32),
    ],
    source=MsSqlServerSource(
        name="pair_source",
        table_ref="pair_stats",
        event_timestamp_column="event_timestamp",
    ),
)


def _utc(*timestamps):
    return pd.to_datetime(list(timestamps), utc=True)


@pytest.fixture
def repo_config():
    return RepoConfig(
        registry="file:///tmp/registry.db",
        project="test_project",
        provider="local",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=MsSqlServerOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )


@pytest.fixture
def connection():
    """Stands in for SQL Server with an in-memory DuckDB holding the sources."""
    con = ibis.duckdb.connect()
    sources = {
        "driver_stats": pd.DataFrame(
            {
                "driver_id": [1, 1, 1, 2, 3],
                "event_timestamp": _utc(
                    "2025-07-02", "2025-07-09", "2025-07-10", "2025-07-05", "2025-06-01"
                ),
                # The 2025-07-10 row arrived after END.
                "created": _utc(
                    "2025-07-02", "2025-07-09", "2025-07-11", "2025-07-05", "2025-06-01"
                ),
                "conv_rate": [0.1, 0.2, 0.25, 0.3, 0.4],
            }
        ),
        "customer_stats": pd.DataFrame(
            {
                "cust": [10, 20],
                "event_timestamp": _utc("2025-07-08", "2025-07-11"),
                "spend": [100.0, 200.0],
            }
        ),
        "pair_stats": pd.DataFrame(
            {
                "driver_id": [1, 1],
                "customer_id": [10, 20],
                "event_timestamp": _utc("2025-07-08", "2025-07-09"),
                "rating": [1.0, 2.0],
            }
        ),
        "global_stats": pd.DataFrame(
            {
                "event_timestamp": _utc("2025-07-01", "2025-07-10"),
                "total": [5, 7],
            }
        ),
    }
    for table_ref, df in sources.items():
        con.con.register(table_ref, df)

    def duckdb_expire_timestamp(timestamp_field, ttl):
        return f"{timestamp_field} + INTERVAL {int(ttl.total_seconds())} SECOND"

    module = "feast.infra.offline_stores.contrib.mssql_offline_store.mssql"
    with (
        patch(f"{module}.get_ibis_connection", return_value=con),
        patch(
            f"{module}.mssql_event_expire_timestamp_fn",
            side_effect=duckdb_expire_timestamp,
        ),
    ):
        yield con


def _get_historical_features(repo_config, feature_views, feature_refs, **kwargs):
    registry = MagicMock()
    registry.list_on_demand_feature_views.return_value = []
    return MsSqlServerOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=feature_views,
        feature_refs=feature_refs,
        entity_df=None,
        registry=registry,
        project="test_project",
        **kwargs,
    )


def _rows(df, columns):
    return sorted(
        (
            tuple(None if pd.isna(row[c]) else row[c] for c in columns)
            for _, row in df.iterrows()
        ),
        key=str,
    )


def test_returns_latest_values_of_entities_in_range(repo_config, connection):
    df = _get_historical_features(
        repo_config,
        [driver_fv],
        ["driver_stats:conv_rate"],
        start_date=START,
        end_date=END,
    ).to_df()

    assert _rows(df, ["driver_id", "conv_rate"]) == [(1, 0.25), (2, None)]
    assert (pd.to_datetime(df["event_timestamp"], utc=True) == END).all()


def test_fills_missing_join_keys_across_feature_views(repo_config, connection):
    df = _get_historical_features(
        repo_config,
        [driver_fv, customer_fv],
        ["driver_stats:conv_rate", "customer_stats:spend"],
        start_date=START,
        end_date=END,
        full_feature_names=True,
    ).to_df()

    assert _rows(
        df,
        [
            "driver_id",
            "customer_id",
            "driver_stats__conv_rate",
            "customer_stats__spend",
        ],
    ) == [(1, None, 0.25, None), (2, None, None, None), (None, 10, None, 100.0)]


def test_feature_view_without_entities_returns_one_row(repo_config, connection):
    df = _get_historical_features(
        repo_config,
        [global_fv],
        ["global_stats:total"],
        start_date=START,
        end_date=END,
    ).to_df()

    assert df["total"].tolist() == [7]


def test_defaults_start_date_to_ttl_window(repo_config, connection):
    """Without start_date the window is end_date minus the largest TTL (2 days)."""
    df = _get_historical_features(
        repo_config,
        [driver_fv],
        ["driver_stats:conv_rate"],
        end_date=END,
    ).to_df()

    assert _rows(df, ["driver_id", "conv_rate"]) == [(1, 0.25)]


def test_filter_by_created_timestamp(repo_config, connection):
    df = _get_historical_features(
        repo_config,
        [driver_fv],
        ["driver_stats:conv_rate"],
        start_date=START,
        end_date=END,
        filter_by_created_timestamp=True,
    ).to_df()

    assert _rows(df, ["driver_id", "conv_rate"]) == [(1, 0.2), (2, None)]


def test_returns_no_rows_without_events_in_range(repo_config, connection):
    df = _get_historical_features(
        repo_config,
        [driver_fv],
        ["driver_stats:conv_rate"],
        start_date=datetime(2025, 8, 1, tzinfo=timezone.utc),
        end_date=datetime(2025, 8, 2, tzinfo=timezone.utc),
    ).to_df()

    assert df.empty
    assert {"driver_id", "event_timestamp", "conv_rate"} <= set(df.columns)


def test_entity_df_mode_is_unchanged(repo_config, connection):
    entity_df = pd.DataFrame({"driver_id": [3], "event_timestamp": _utc("2025-06-02")})
    registry = MagicMock()
    registry.list_on_demand_feature_views.return_value = []

    df = MsSqlServerOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[driver_fv],
        feature_refs=["driver_stats:conv_rate"],
        entity_df=entity_df,
        registry=registry,
        project="test_project",
    ).to_df()

    assert _rows(df, ["driver_id", "conv_rate"]) == [(3, 0.4)]


def test_partial_join_key_map_keeps_unmapped_keys(repo_config, connection):
    df = _get_historical_features(
        repo_config,
        [pair_fv.with_join_key_map({"driver_id": "origin_id"})],
        ["pair_stats:rating"],
        start_date=START,
        end_date=END,
    ).to_df()

    assert _rows(df, ["origin_id", "customer_id", "rating"]) == [
        (1, 10, 1.0),
        (1, 20, 2.0),
    ]
