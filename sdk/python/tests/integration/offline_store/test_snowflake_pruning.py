"""Regression tests for feature-table pruning in the Snowflake point-in-time join.

These exercise the ``__subquery`` timestamp bounds added in
https://github.com/feast-dev/feast/pull/7230. The pruning predicates are only
allowed to remove rows that the ASOF join and TTL filter would discard anyway,
so every test asserts that a row which is eligible for the point-in-time join is
still returned.
"""

import os
from datetime import timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.offline_stores.snowflake_source import SnowflakeSource
from feast.infra.utils.snowflake.snowflake_utils import (
    GetSnowflakeConnection,
    execute_snowflake_statement,
)
from tests.universal.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.universal.feature_repos.repo_configuration import (
    construct_test_environment,
)
from tests.universal.feature_repos.universal.data_sources.snowflake import (
    SnowflakeDataSourceCreator,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.getenv("SNOWFLAKE_CI_DEPLOYMENT"),
        reason="Snowflake account not configured in CI (SNOWFLAKE_CI_DEPLOYMENT not set)",
    ),
]


def _make_environment():
    return construct_test_environment(
        IntegrationTestRepoConfig(
            online_store="sqlite",
            offline_store_creator=SnowflakeDataSourceCreator,
        ),
        None,
    )


def _execute(environment, sql: str) -> None:
    with GetSnowflakeConnection(
        environment.data_source_creator.offline_store_config
    ) as conn:
        execute_snowflake_statement(conn, sql)


def _create_table(environment, suffix: str, columns: str) -> str:
    table = environment.data_source_creator.get_prefixed_table_name(suffix)
    _execute(environment, f'CREATE OR REPLACE TABLE "{table}" ({columns})')
    environment.data_source_creator.tables.append(table)
    return table


def _feature_source(table: str) -> SnowflakeSource:
    return SnowflakeSource(table=table, timestamp_field="event_timestamp")


def _driver_view(
    name: str, source: SnowflakeSource, ttl: timedelta = timedelta(0)
) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[Entity(name="driver", join_keys=["driver_id"])],
        ttl=ttl,
        source=source,
    )


def test_pruning_upper_bound_uses_uploaded_entity_snapshot():
    """A SQL entity_df re-runs when the job materializes.

    The range is measured at get_historical_features() time but the entity SQL is
    re-executed by _upload_entity_df() when to_sql()/to_df() runs. If the query
    returns a newer row the second time, the upper pruning bound must come from
    that uploaded snapshot, not from the first execution.
    """
    environment = _make_environment()
    environment.setup()
    store = environment.feature_store
    try:
        feature_table = _create_table(
            environment,
            "stale_bounds_features",
            '"driver_id" INT, "event_timestamp" TIMESTAMP_NTZ(9), "value" FLOAT',
        )
        entity_table = _create_table(
            environment,
            "stale_bounds_entities",
            '"driver_id" INT, "event_timestamp" TIMESTAMP_NTZ(9)',
        )
        _execute(
            environment,
            f'INSERT INTO "{feature_table}" VALUES '
            "(1, TIMESTAMP '2024-01-01 00:00:00.000000000', 1.0)",
        )
        _execute(
            environment,
            f'INSERT INTO "{entity_table}" VALUES '
            "(1, TIMESTAMP '2024-01-01 00:00:00.000000000')",
        )

        driver = Entity(name="driver", join_keys=["driver_id"])
        feature_view = _driver_view("stale_bounds_fv", _feature_source(feature_table))
        store.apply([driver, feature_view])

        job = store.get_historical_features(
            entity_df=f'SELECT "driver_id", "event_timestamp" FROM "{entity_table}"',
            features=["stale_bounds_fv:value"],
        )

        _execute(
            environment,
            f'INSERT INTO "{feature_table}" VALUES '
            "(2, TIMESTAMP '2024-01-01 01:00:00.000000000', 2.0)",
        )
        _execute(
            environment,
            f'INSERT INTO "{entity_table}" VALUES '
            "(2, TIMESTAMP '2024-01-01 01:00:00.000000000')",
        )

        df = job.to_df()

        row = df.loc[df["driver_id"] == 2]
        assert len(row) == 1
        assert row["value"].iloc[0] == 2.0
    finally:
        environment.teardown()


def test_pruning_upper_bound_keeps_sub_microsecond_matches():
    """The upper bound must not truncate sub-microsecond entity timestamps.

    pandas loses nanosecond digits when the entity timestamp is converted with
    to_pydatetime(), while the uploaded entity row and the feature table keep
    them. A feature in the same microsecond as the latest entity timestamp is
    still eligible for the ASOF join and must survive pruning.
    """
    environment = _make_environment()
    environment.setup()
    store = environment.feature_store
    try:
        feature_table = _create_table(
            environment,
            "sub_microsecond_features",
            '"driver_id" INT, "event_timestamp" TIMESTAMP_NTZ(9), "value" FLOAT',
        )
        _execute(
            environment,
            f'INSERT INTO "{feature_table}" VALUES '
            "(1, TIMESTAMP '2024-06-01 00:00:00.123456700', 42.0)",
        )

        driver = Entity(name="driver", join_keys=["driver_id"])
        feature_view = _driver_view(
            "sub_microsecond_fv", _feature_source(feature_table)
        )
        store.apply([driver, feature_view])

        entity_df = pd.DataFrame(
            {
                "driver_id": [1],
                "event_timestamp": pd.to_datetime(["2024-06-01 00:00:00.123456789"]),
            }
        )
        assert entity_df["event_timestamp"].iloc[0].nanosecond == 789

        df = store.get_historical_features(
            entity_df=entity_df,
            features=["sub_microsecond_fv:value"],
        ).to_df()

        assert df["value"].iloc[0] == 42.0
    finally:
        environment.teardown()


def test_pruning_lower_bound_uses_utc_elapsed_time_across_dst():
    """The TTL lower bound is elapsed time, not wall-clock time.

    With coerce_tz_aware disabled, the minimum entity timestamp keeps its
    ZoneInfo. Subtracting the TTL from that datetime does wall-clock arithmetic,
    so across a spring-forward transition the Python cutoff lands one hour later
    in UTC than the per-row TTL. A feature inside the TTL must not be pruned.

    The timezone must be a ZoneInfo, not a tz string: pandas 2.x resolves strings
    to pytz, whose localized datetimes carry a fixed offset and so already
    subtract elapsed time, hiding the bug.

    Entity at noon America/New_York on 2025-03-09 is 16:00 UTC. A 24h TTL keeps
    16:30 UTC on 2025-03-08, but wall-clock subtraction produces 17:00 UTC.
    """
    environment = _make_environment()
    environment.setup()
    store = environment.feature_store
    try:
        feature_table = _create_table(
            environment,
            "dst_features",
            '"driver_id" INT, "event_timestamp" TIMESTAMP_NTZ(9), "value" FLOAT',
        )
        _execute(
            environment,
            f'INSERT INTO "{feature_table}" VALUES '
            "(1, TIMESTAMP '2025-03-08 16:30:00.000000000', 7.0)",
        )

        driver = Entity(name="driver", join_keys=["driver_id"])
        feature_view = _driver_view(
            "dst_fv", _feature_source(feature_table), ttl=timedelta(days=1)
        )
        store.apply([driver, feature_view])

        store.config.coerce_tz_aware = False
        entity_df = pd.DataFrame(
            {
                "driver_id": [1],
                "event_timestamp": pd.Series(
                    [
                        pd.Timestamp(
                            "2025-03-09 12:00:00", tz=ZoneInfo("America/New_York")
                        )
                    ]
                ),
            }
        )

        df = store.get_historical_features(
            entity_df=entity_df,
            features=["dst_fv:value"],
        ).to_df()

        assert df["value"].iloc[0] == 7.0
    finally:
        environment.teardown()
