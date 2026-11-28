import re
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from pytest_mock import MockFixture

from feast import Entity, FeatureView, Field, FileSource
from feast.infra.offline_stores.snowflake import (
    SnowflakeOfflineStore,
    SnowflakeOfflineStoreConfig,
    SnowflakeRetrievalJob,
)
from feast.infra.offline_stores.snowflake_source import SnowflakeSource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Array, Float32, Int64, String


@pytest.fixture(params=["s3", "s3gov"])
def retrieval_job(request):
    offline_store_config = SnowflakeOfflineStoreConfig(
        type="snowflake.offline",
        account="snow",
        user="snow",
        password="snow",
        role="snow",
        warehouse="snow",
        database="FEAST",
        schema="OFFLINE",
        storage_integration_name="FEAST_S3",
        blob_export_location=f"{request.param}://feast-snowflake-offload/export",
    )
    retrieval_job = SnowflakeRetrievalJob(
        query="SELECT * FROM snowflake",
        snowflake_conn=MagicMock(),
        config=RepoConfig(
            registry="s3://ml-test/repo/registry.db",
            project="test",
            provider="snowflake.offline",
            online_store=SqliteOnlineStoreConfig(type="sqlite"),
            offline_store=offline_store_config,
            entity_key_serialization_version=3,
        ),
        full_feature_names=True,
        on_demand_feature_views=[],
    )
    return retrieval_job


def test_to_remote_storage(retrieval_job):
    stored_files = ["just a path", "maybe another"]
    with (
        patch.object(
            retrieval_job, "to_snowflake", return_value=None
        ) as mock_to_snowflake,
        patch.object(
            retrieval_job, "_get_file_names_from_copy_into", return_value=stored_files
        ) as mock_get_file_names_from_copy,
    ):
        assert retrieval_job.to_remote_storage() == stored_files, (
            "should return the list of files"
        )
        mock_to_snowflake.assert_called_once()
        mock_get_file_names_from_copy.assert_called_once_with(ANY, ANY)
        native_path = mock_get_file_names_from_copy.call_args[0][1]
        assert re.match("^s3://.*", native_path), "path should be s3://*"


def test_snowflake_to_df_internal(
    retrieval_job: SnowflakeRetrievalJob, mocker: MockFixture
):
    mock_execute = mocker.patch(
        "feast.infra.offline_stores.snowflake.execute_snowflake_statement"
    )
    mock_execute.return_value.fetch_pandas_all.return_value = pd.DataFrame.from_dict(
        {"feature1": ['["1", "2", "3"]', None, "[]"]}  # For Valid, Null, and Empty
    )

    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Array(String)),
        ],
        source=FileSource(path="dummy.path"),  # Dummy value
    )
    retrieval_job._feature_views = [feature_view]
    retrieval_job._to_df_internal()


@pytest.mark.parametrize(
    "precision, max_value, expected",
    [
        (18, None, "NUMBER64"),
        (19, 42, "NUMBER32"),
        (19, 1234567890123, "NUMBER64"),
        (38, 1234567890123, "NUMBER64"),
    ],
)
def test_snowflake_source_integer_column_types(
    mocker: MockFixture, precision, max_value, expected
):
    config = RepoConfig(
        registry="s3://ml-test/repo/registry.db",
        project="test",
        provider="local",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=SnowflakeOfflineStoreConfig(
            type="snowflake.offline",
            account="snow",
            user="snow",
            password="snow",
            role="snow",
            warehouse="snow",
            database="FEAST",
            schema="PUBLIC",
        ),
        entity_key_serialization_version=3,
    )
    sample = MagicMock()
    sample.description = [
        SimpleNamespace(
            name="ID", type_code=0, precision=precision, scale=0, is_nullable=True
        )
    ]
    sample.fetch_pandas_all.return_value = pd.DataFrame({"ID": [1]})
    maximum = MagicMock()
    maximum.fetch_pandas_all.return_value = pd.DataFrame({"ID": [max_value]})
    mocker.patch("feast.infra.utils.snowflake.snowflake_utils.GetSnowflakeConnection")
    mocker.patch(
        "feast.infra.utils.snowflake.snowflake_utils.execute_snowflake_statement",
        side_effect=[sample, maximum],
    )
    source = SnowflakeSource(
        name="source", database="FEAST", schema="PUBLIC", table="T"
    )

    assert list(source.get_table_column_names_and_types(config)) == [("ID", expected)]


# Feature-table pruning bounds in the point-in-time join.
#
# The "__subquery" CTE filters the feature table by
#   timestamp_field <= max_event_timestamp
#   timestamp_field >= min_event_timestamp   (only when the view has a TTL)
# These predicates may only drop rows that the ASOF join and per-row TTL filter
# would discard anyway. The integration tests in
# tests/integration/offline_store/test_snowflake_pruning.py check the end-to-end
# result; these check the rendered bounds without a Snowflake account.

_PRUNING_UPPER_BOUND = re.compile(r"\"event_timestamp\" <= TIMESTAMP '([^']+)'")
_PRUNING_LOWER_BOUND = re.compile(r"\"event_timestamp\" >= TIMESTAMP '([^']+)'")


def _pruning_bounds(entity_df: pd.DataFrame | str, ttl: timedelta):
    """Render the point-in-time SQL and return its (lower, upper) pruning bounds."""
    driver = Entity(name="driver", join_keys=["driver_id"])
    feature_view = FeatureView(
        name="driver_fv",
        entities=[driver],
        ttl=ttl,
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="value", dtype=Float32),
        ],
        source=SnowflakeSource(
            database="FEAST",
            schema="PUBLIC",
            table="DRIVER_FEATURES",
            timestamp_field="event_timestamp",
        ),
    )
    config = RepoConfig(
        registry="s3://ml-test/repo/registry.db",
        project="test",
        provider="local",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=SnowflakeOfflineStoreConfig(
            type="snowflake.offline",
            account="snow",
            user="snow",
            password="snow",
            role="snow",
            warehouse="snow",
            database="FEAST",
            schema="OFFLINE",
        ),
        entity_key_serialization_version=3,
    )
    registry = MagicMock()
    registry.list_on_demand_feature_views.return_value = []

    with (
        patch("feast.infra.offline_stores.snowflake.GetSnowflakeConnection"),
        patch("feast.infra.offline_stores.snowflake._upload_entity_df"),
    ):
        job = SnowflakeOfflineStore.get_historical_features(
            config=config,
            feature_views=[feature_view],
            feature_refs=["driver_fv:value"],
            entity_df=entity_df,
            registry=registry,
            project="test",
        )
        with job._query_generator() as query:
            sql = query

    lower = _PRUNING_LOWER_BOUND.search(sql)
    upper = _PRUNING_UPPER_BOUND.search(sql)
    assert upper is not None, sql
    return (
        pd.Timestamp(lower.group(1)) if lower else None,
        pd.Timestamp(upper.group(1)),
    )


def test_pruning_upper_bound_keeps_sub_microsecond_entity_timestamps():
    """to_pydatetime() drops nanoseconds from the entity maximum.

    The uploaded entity row keeps them, so a feature in the same microsecond is
    still eligible for the ASOF join. The upper bound must not fall below the
    full-precision entity timestamp.
    """
    entity_ts = pd.Timestamp("2024-06-01 00:00:00.123456789")
    entity_df = pd.DataFrame({"driver_id": [1], "event_timestamp": [entity_ts]})

    _, upper = _pruning_bounds(entity_df, ttl=timedelta(0))

    assert upper >= entity_ts


def test_pruning_lower_bound_uses_utc_elapsed_time_across_dst():
    """With coerce_tz_aware disabled the TTL must be elapsed time, not wall-clock.

    Noon America/New_York on 2025-03-09 is 16:00 UTC, the day of the
    spring-forward transition. The per-row TTL in SQL subtracts 86400 seconds,
    so 16:00 UTC on 2025-03-08 is eligible. Subtracting timedelta(days=1) from a
    ZoneInfo datetime is wall-clock arithmetic and yields 17:00 UTC, pruning
    eligible features between 16:00 and 17:00 UTC.

    The timezone must be a ZoneInfo: pandas 2.x resolves tz strings to pytz,
    whose localized datetimes carry a fixed offset and hide the bug.
    """
    entity_df = pd.DataFrame(
        {
            "driver_id": [1],
            "event_timestamp": pd.Series(
                [pd.Timestamp("2025-03-09 12:00:00", tz=ZoneInfo("America/New_York"))]
            ),
        }
    )
    assert isinstance(
        entity_df["event_timestamp"].min().to_pydatetime().tzinfo, ZoneInfo
    )

    lower, _ = _pruning_bounds(entity_df, ttl=timedelta(days=1))

    assert lower is not None
    assert lower <= pd.Timestamp("2025-03-08 16:00:00")


def test_pruning_bounds_use_uploaded_entity_snapshot_for_sql_entity_df():
    """A SQL entity_df is executed again when the entity table is uploaded.

    get_historical_features() measures MIN/MAX over the entity SQL up front, but
    the join uses the temp table that _upload_entity_df() creates later. If the
    SQL returns newer rows by then, bounds from the first execution would prune
    their features. The bounds must come from the uploaded table.
    """
    entity_sql = 'SELECT "driver_id", "event_timestamp" FROM "ENTITIES"'
    first_range = (datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 0))
    uploaded_range = (datetime(2023, 12, 31, 0, 0), datetime(2024, 1, 1, 1, 0))
    range_queries = []

    def fake_execute(conn, query):
        cursor = MagicMock()
        if query.endswith("LIMIT 1"):
            cursor.fetch_pandas_all.return_value = pd.DataFrame(
                {
                    "driver_id": pd.Series([1], dtype="int64"),
                    "event_timestamp": pd.to_datetime(["2024-01-01"]),
                }
            )
        elif query.startswith("SELECT MIN("):
            range_queries.append(query)
            is_entity_sql = query.endswith(f"FROM ({entity_sql})")
            cursor.fetchall.return_value = [
                first_range if is_entity_sql else uploaded_range
            ]
        return cursor

    with patch(
        "feast.infra.offline_stores.snowflake.execute_snowflake_statement",
        side_effect=fake_execute,
    ):
        lower, upper = _pruning_bounds(entity_sql, ttl=timedelta(hours=1))

    assert len(range_queries) == 2
    assert range_queries[0].endswith(f"FROM ({entity_sql})")
    assert re.search(r'FROM "feast_entity_df_[0-9a-f]+"$', range_queries[1])
    assert upper >= pd.Timestamp(uploaded_range[1])
    assert lower <= pd.Timestamp(uploaded_range[0]) - pd.Timedelta(hours=1)
