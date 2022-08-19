import os
from datetime import timedelta

import pytest

from feast.entity import Entity
from feast.feature_view import FeatureView
from tests.data.data_creator import create_basic_driver_dataset
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import (
    construct_test_environment,
)
from tests.integration.feature_repos.universal.data_sources.snowflake import (
    SnowflakeDataSourceCreator,
)
from tests.utils.e2e_test_validation import validate_offline_online_store_consistency

SNOWFLAKE_ENGINE_CONFIG = {
    "type": "snowflake.engine",
    "account": os.getenv("SNOWFLAKE_CI_DEPLOYMENT", ""),
    "user": os.getenv("SNOWFLAKE_CI_USER", ""),
    "password": os.getenv("SNOWFLAKE_CI_PASSWORD", ""),
    "role": os.getenv("SNOWFLAKE_CI_ROLE", ""),
    "warehouse": os.getenv("SNOWFLAKE_CI_WAREHOUSE", ""),
    "database": "FEAST",
    "schema": "MATERIALIZATION",
}

SNOWFLAKE_ONLINE_CONFIG = {
    "type": "snowflake.online",
    "account": os.getenv("SNOWFLAKE_CI_DEPLOYMENT", ""),
    "user": os.getenv("SNOWFLAKE_CI_USER", ""),
    "password": os.getenv("SNOWFLAKE_CI_PASSWORD", ""),
    "role": os.getenv("SNOWFLAKE_CI_ROLE", ""),
    "warehouse": os.getenv("SNOWFLAKE_CI_WAREHOUSE", ""),
    "database": "FEAST",
    "schema": "ONLINE",
}


@pytest.mark.integration
def test_snowflake_materialization_consistency_internal():
    snowflake_config = IntegrationTestRepoConfig(
        online_store=SNOWFLAKE_ONLINE_CONFIG,
        offline_store_creator=SnowflakeDataSourceCreator,
        batch_engine=SNOWFLAKE_ENGINE_CONFIG,
    )
    snowflake_environment = construct_test_environment(snowflake_config, None)

    df = create_basic_driver_dataset()
    ds = snowflake_environment.data_source_creator.create_data_source(
        df,
        snowflake_environment.feature_store.project,
        field_mapping={"ts_1": "ts"},
    )

    fs = snowflake_environment.feature_store
    driver = Entity(
        name="driver_id",
        join_keys=["driver_id"],
    )

    driver_stats_fv = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        ttl=timedelta(weeks=52),
        source=ds,
    )

    try:
        fs.apply([driver, driver_stats_fv])

        # materialization is run in two steps and
        # we use timestamp from generated dataframe as a split point
        split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)

        print(f"Split datetime: {split_dt}")

        validate_offline_online_store_consistency(fs, driver_stats_fv, split_dt)
    finally:
        fs.teardown()
        snowflake_environment.data_source_creator.teardown()


@pytest.mark.integration
def test_snowflake_materialization_consistency_external():
    snowflake_config = IntegrationTestRepoConfig(
        offline_store_creator=SnowflakeDataSourceCreator,
        batch_engine=SNOWFLAKE_ENGINE_CONFIG,
    )
    snowflake_environment = construct_test_environment(snowflake_config, None)

    df = create_basic_driver_dataset()
    ds = snowflake_environment.data_source_creator.create_data_source(
        df,
        snowflake_environment.feature_store.project,
        field_mapping={"ts_1": "ts"},
    )

    fs = snowflake_environment.feature_store
    driver = Entity(
        name="driver_id",
        join_keys=["driver_id"],
    )

    driver_stats_fv = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        ttl=timedelta(weeks=52),
        source=ds,
    )

    try:
        fs.apply([driver, driver_stats_fv])

        # materialization is run in two steps and
        # we use timestamp from generated dataframe as a split point
        split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)

        print(f"Split datetime: {split_dt}")

        validate_offline_online_store_consistency(fs, driver_stats_fv, split_dt)
    finally:
        fs.teardown()
        snowflake_environment.data_source_creator.teardown()
