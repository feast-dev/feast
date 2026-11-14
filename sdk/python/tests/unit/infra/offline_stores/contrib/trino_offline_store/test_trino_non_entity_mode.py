from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from feast import Entity, FeatureView, Field, ValueType
from feast.infra.offline_stores.contrib.trino_offline_store.trino import (
    TrinoOfflineStore,
    TrinoOfflineStoreConfig,
    _build_non_entity_left_table_query,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import (
    TrinoSource,
)
from feast.infra.offline_stores.offline_utils import (
    gather_all_entities,
    get_feature_view_query_context,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64

START = datetime(2025, 7, 1, tzinfo=timezone.utc)
END = datetime(2025, 7, 10, 12, 0, tzinfo=timezone.utc)
START_LITERAL = "from_iso8601_timestamp('2025-07-01T00:00:00+00:00')"
END_LITERAL = "from_iso8601_timestamp('2025-07-10T12:00:00+00:00')"

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
    source=TrinoSource(
        name="driver_source",
        table="memory.feast.driver_stats",
        timestamp_field="event_timestamp",
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
    source=TrinoSource(
        name="customer_source",
        table="memory.feast.customer_stats",
        timestamp_field="event_ts",
        field_mapping={"cust": "customer_id", "ts": "event_ts"},
    ),
)

global_fv = FeatureView(
    name="global_stats",
    entities=[],
    ttl=timedelta(days=1),
    schema=[Field(name="total", dtype=Int64)],
    source=TrinoSource(
        name="global_source",
        table="memory.feast.global_stats",
        timestamp_field="event_timestamp",
    ),
)


@pytest.fixture
def repo_config():
    return RepoConfig(
        registry="file:///tmp/registry.db",
        project="test_project",
        provider="local",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=TrinoOfflineStoreConfig(
            host="localhost",
            port=8080,
            catalog="memory",
            user="feast",
            connector={"type": "memory"},
            dataset="feast",
        ),
        entity_key_serialization_version=3,
    )


@pytest.fixture
def trino_client():
    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._get_trino_client"
    ) as get_trino_client:
        yield get_trino_client.return_value


def _get_historical_features(repo_config, feature_views, feature_refs, **kwargs):
    registry = MagicMock()
    registry.list_on_demand_feature_views.return_value = []
    return TrinoOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=feature_views,
        feature_refs=feature_refs,
        entity_df=None,
        registry=registry,
        project="test_project",
        **kwargs,
    )


def _left_table_query(feature_views, feature_refs):
    query_context = get_feature_view_query_context(
        feature_refs=feature_refs,
        feature_views=feature_views,
        registry=MagicMock(),
        project="test_project",
        entity_df_timestamp_range=(START, END),
    )
    join_keys = gather_all_entities(query_context)
    return _build_non_entity_left_table_query(
        query_context=query_context,
        join_keys=join_keys,
        start_date=START,
        end_date=END,
        entity_df_event_timestamp_col="event_timestamp",
    )


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


def test_left_table_selects_entities_observed_in_range():
    query = _normalize(_left_table_query([driver_fv], ["driver_stats:conv_rate"]))

    assert (
        "SELECT DISTINCT driver_id AS driver_id "
        "FROM (memory.feast.driver_stats) "
        f"WHERE event_timestamp BETWEEN {START_LITERAL} AND {END_LITERAL}"
    ) in query
    assert f"SELECT *, {END_LITERAL} AS event_timestamp" in query
    assert "UNION" not in query


def test_left_table_uses_source_column_names_from_field_mapping():
    query = _normalize(_left_table_query([customer_fv], ["customer_stats:spend"]))

    assert "SELECT DISTINCT cust AS customer_id" in query
    assert f"WHERE ts BETWEEN {START_LITERAL} AND {END_LITERAL}" in query


def test_left_table_unions_feature_views_and_fills_missing_join_keys():
    query = _normalize(
        _left_table_query(
            [driver_fv, customer_fv],
            ["driver_stats:conv_rate", "customer_stats:spend"],
        )
    )

    assert (
        "SELECT DISTINCT driver_id AS driver_id, NULL AS customer_id "
        "FROM (memory.feast.driver_stats)"
    ) in query
    assert (
        "SELECT DISTINCT NULL AS driver_id, cust AS customer_id "
        "FROM (memory.feast.customer_stats)"
    ) in query
    assert query.count(" UNION ") == 1


def test_left_table_wraps_query_sources_in_parentheses():
    query_fv = FeatureView(
        name="driver_query_stats",
        entities=[driver],
        ttl=timedelta(days=2),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float32),
        ],
        source=TrinoSource(
            name="driver_query_source",
            query="SELECT * FROM memory.feast.driver_stats WHERE driver_id > 0",
            timestamp_field="event_timestamp",
        ),
    )

    query = _normalize(_left_table_query([query_fv], ["driver_query_stats:conv_rate"]))

    assert (
        "FROM (SELECT * FROM memory.feast.driver_stats WHERE driver_id > 0) "
        f"WHERE event_timestamp BETWEEN {START_LITERAL} AND {END_LITERAL}"
    ) in query


def test_left_table_skips_feature_views_without_entities():
    query = _normalize(
        _left_table_query(
            [driver_fv, global_fv],
            ["driver_stats:conv_rate", "global_stats:total"],
        )
    )

    assert "memory.feast.global_stats" not in query
    assert "FROM (memory.feast.driver_stats)" in query


def test_left_table_is_single_row_without_entities():
    query = _normalize(_left_table_query([global_fv], ["global_stats:total"]))

    assert query == f"(SELECT {END_LITERAL} AS event_timestamp)"


def test_left_table_converts_dates_to_utc():
    query_context = get_feature_view_query_context(
        feature_refs=["driver_stats:conv_rate"],
        feature_views=[driver_fv],
        registry=MagicMock(),
        project="test_project",
        entity_df_timestamp_range=(START, END),
    )
    tz = timezone(timedelta(hours=2))

    query = _build_non_entity_left_table_query(
        query_context=query_context,
        join_keys=["driver_id"],
        start_date=START.astimezone(tz),
        end_date=END.astimezone(tz),
        entity_df_event_timestamp_col="event_timestamp",
    )

    assert START_LITERAL in query
    assert END_LITERAL in query


def test_non_entity_mode_does_not_upload_entity_df(repo_config, trino_client):
    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._upload_entity_df_and_get_entity_schema"
    ) as upload_entity_df:
        job = _get_historical_features(
            repo_config,
            [driver_fv],
            ["driver_stats:conv_rate"],
            start_date=START,
            end_date=END,
        )

    upload_entity_df.assert_not_called()
    trino_client.execute_query.assert_not_called()
    assert job._temp_table is None


def test_non_entity_mode_builds_point_in_time_query(repo_config, trino_client):
    job = _get_historical_features(
        repo_config,
        [driver_fv, customer_fv],
        ["driver_stats:conv_rate", "customer_stats:spend"],
        full_feature_names=True,
        start_date=START,
        end_date=END,
    )
    query = _normalize(job.to_sql())

    assert "SELECT DISTINCT driver_id AS driver_id, NULL AS customer_id" in query
    assert "SELECT DISTINCT NULL AS driver_id, cust AS customer_id" in query
    assert (
        "SELECT driver_id, customer_id, event_timestamp, "
        "driver_stats__conv_rate, customer_stats__spend FROM entity_dataframe"
    ) in query
    assert job.full_feature_names is True


def test_non_entity_mode_metadata(repo_config, trino_client):
    job = _get_historical_features(
        repo_config,
        [driver_fv, customer_fv],
        ["driver_stats:conv_rate", "customer_stats:spend"],
        start_date=START,
        end_date=END,
    )

    assert job.metadata is not None
    assert job.metadata.features == ["driver_stats:conv_rate", "customer_stats:spend"]
    assert job.metadata.keys == ["driver_id", "customer_id"]
    assert job.metadata.min_event_timestamp == START
    assert job.metadata.max_event_timestamp == END


def test_non_entity_mode_defaults_start_date_to_max_ttl(repo_config, trino_client):
    job = _get_historical_features(
        repo_config,
        [driver_fv, customer_fv],
        ["driver_stats:conv_rate", "customer_stats:spend"],
        end_date=END,
    )

    assert job.metadata is not None
    assert job.metadata.min_event_timestamp == END - timedelta(days=5)
    assert job.metadata.max_event_timestamp == END


def test_non_entity_mode_filters_by_created_timestamp(repo_config, trino_client):
    job = _get_historical_features(
        repo_config,
        [driver_fv],
        ["driver_stats:conv_rate"],
        filter_by_created_timestamp=True,
        start_date=START,
        end_date=END,
    )

    assert "AND subquery.created_timestamp <= entity_dataframe.entity_timestamp" in (
        _normalize(job.to_sql())
    )


def test_entity_df_mode_still_uploads_entity_df(repo_config, trino_client):
    entity_df = pd.DataFrame({"driver_id": [1], "event_timestamp": [END]})
    registry = MagicMock()
    registry.list_on_demand_feature_views.return_value = []

    with patch(
        "feast.infra.offline_stores.contrib.trino_offline_store.trino._upload_entity_df_and_get_entity_schema",
        return_value=dict(zip(entity_df.columns, entity_df.dtypes)),
    ) as upload_entity_df:
        job = TrinoOfflineStore.get_historical_features(
            config=repo_config,
            feature_views=[driver_fv],
            feature_refs=["driver_stats:conv_rate"],
            entity_df=entity_df,
            registry=registry,
            project="test_project",
        )

    upload_entity_df.assert_called_once()
    assert job._temp_table is not None
    assert "SELECT DISTINCT" not in job.to_sql()
