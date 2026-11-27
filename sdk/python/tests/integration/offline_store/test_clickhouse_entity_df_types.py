from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("clickhouse_connect")
pytest.importorskip("testcontainers.clickhouse")

from feast import Entity, FeatureView, Field  # noqa: E402
from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse import (  # noqa: E402
    ClickhouseOfflineStore,
    df_to_clickhouse_table,
)
from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (  # noqa: E402
    ClickhouseSource,
)
from feast.infra.offline_stores.contrib.clickhouse_offline_store.tests.data_source import (  # noqa: E402
    _make_offline_store_config,
    clickhouse_container,  # noqa: F401
)
from feast.infra.utils.clickhouse.connection_utils import get_client  # noqa: E402
from feast.repo_config import RepoConfig  # noqa: E402
from feast.types import Float64, Int64, String  # noqa: E402
from feast.value_type import ValueType  # noqa: E402


@pytest.mark.integration
def test_entity_df_upload_with_unsigned_date_decimal_and_arrow_string_columns(
    clickhouse_container,  # noqa: F811
):
    """get_historical_features uploads the entity dataframe to ClickHouse and joins it
    point-in-time against the feature tables. The request columns below used to fail in
    the upload (``Uint64`` is not a ClickHouse type; Arrow date, decimal and
    ``large_string`` columns had no mapping), so the values have to survive the upload
    and the join has to return the right features.
    """
    offline_config = _make_offline_store_config(clickhouse_container)
    repo_config = RepoConfig(
        project="test_entity_df_types",
        registry="test_registry",
        provider="local",
        offline_store=offline_config,
    )

    client = get_client(offline_config)

    # Feature tables, keyed by an integer column and by a string column.
    now = datetime.now(tz=timezone.utc).replace(microsecond=0)
    user_table = "entity_df_types_users"
    driver_table = "entity_df_types_drivers"
    user_features = pd.DataFrame(
        {
            "user_id": np.array([1, 1, 2], dtype="int64"),
            "event_timestamp": [
                now - timedelta(hours=3),
                now - timedelta(hours=1),
                now - timedelta(hours=2),
            ],
            "user_score": [10.0, 11.0, 20.0],
        }
    )
    driver_features = pd.DataFrame(
        {
            "driver_id": ["a", "b"],
            "event_timestamp": [now - timedelta(hours=2), now - timedelta(hours=2)],
            "driver_rating": [4.5, 3.5],
        }
    )
    for table, df in ((user_table, user_features), (driver_table, driver_features)):
        client.command(f"DROP TABLE IF EXISTS {table}")
        df_to_clickhouse_table(offline_config, df, table, "event_timestamp")

    user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.STRING)
    user_fv = FeatureView(
        name="user_stats",
        entities=[user],
        ttl=timedelta(days=1),
        source=ClickhouseSource(
            name=user_table, table=user_table, timestamp_field="event_timestamp"
        ),
        schema=[
            Field(name="user_id", dtype=Int64),
            Field(name="user_score", dtype=Float64),
        ],
    )
    driver_fv = FeatureView(
        name="driver_stats",
        entities=[driver],
        ttl=timedelta(days=1),
        source=ClickhouseSource(
            name=driver_table, table=driver_table, timestamp_field="event_timestamp"
        ),
        schema=[
            Field(name="driver_id", dtype=String),
            Field(name="driver_rating", dtype=Float64),
        ],
    )

    # Request dataframe: uint64 join key, Arrow-backed string join key, date and Decimal
    # request columns. The dates are outside ClickHouse's Date range (1970..2149), so a
    # silent clamp to 1970-01-01 would show up in the assertions.
    entity_df = pd.DataFrame(
        {
            "user_id": np.array([1, 2, 1], dtype="uint64"),
            "driver_id": pd.array(["a", "b", "a"], dtype="string[pyarrow]"),
            "event_timestamp": [now, now, now - timedelta(hours=2, minutes=30)],
            "request_day": [date(1850, 3, 1), date(2024, 1, 2), date(2300, 12, 31)],
            "request_price": [Decimal("1.50"), Decimal("2.25"), Decimal("1234.56")],
        }
    )

    registry = MagicMock()
    registry.list_on_demand_feature_views.return_value = []

    job = ClickhouseOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[user_fv, driver_fv],
        feature_refs=["user_stats:user_score", "driver_stats:driver_rating"],
        entity_df=entity_df,
        registry=registry,
        project="test_entity_df_types",
    )
    result = job.to_df().sort_values("request_day").reset_index(drop=True)

    assert len(result) == 3

    # The request columns survive the upload with their values.
    assert result["request_day"].tolist() == [
        date(1850, 3, 1),
        date(2024, 1, 2),
        date(2300, 12, 31),
    ]
    # ClickHouse hands Decimal columns back as float64; the values are exact here.
    assert result["request_price"].dtype == np.float64
    assert result["request_price"].tolist() == [1.5, 2.25, 1234.56]
    assert result["user_id"].dtype == np.uint64
    assert result["user_id"].tolist() == [1, 2, 1]
    assert result["driver_id"].tolist() == ["a", "b", "a"]

    # Point-in-time join. Row 0 (user 1, "a", now) takes the 1h-old user row and the
    # 2h-old driver row; row 1 (user 2, "b") takes its only rows; row 2 (user 1, "a",
    # 2h30m ago) takes the 3h-old user row and has no driver row yet.
    assert result["user_score"].tolist() == [11.0, 20.0, 10.0]
    assert result["driver_rating"].iloc[:2].tolist() == [4.5, 3.5]
    assert pd.isna(result["driver_rating"].iloc[2])

    for table in (user_table, driver_table):
        client.command(f"DROP TABLE IF EXISTS {table}")
