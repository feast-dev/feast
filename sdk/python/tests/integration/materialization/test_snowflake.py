import os
from datetime import datetime, timedelta, timezone

import pytest

from feast import Field
from feast.entity import Entity
from feast.feature_view import DUMMY_ENTITY_FIELD, FeatureView
from feast.types import Array, Bool, Bytes, Float64, Int32, Int64, String, UnixTimestamp
from feast.utils import _utc_now
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
    "database": os.getenv("SNOWFLAKE_CI_DATABASE", "FEAST"),
    "schema": os.getenv("SNOWFLAKE_CI_SCHEMA_MATERIALIZATION", "MATERIALIZATION"),
}

SNOWFLAKE_ONLINE_CONFIG = {
    "type": "snowflake.online",
    "account": os.getenv("SNOWFLAKE_CI_DEPLOYMENT", ""),
    "user": os.getenv("SNOWFLAKE_CI_USER", ""),
    "password": os.getenv("SNOWFLAKE_CI_PASSWORD", ""),
    "role": os.getenv("SNOWFLAKE_CI_ROLE", ""),
    "warehouse": os.getenv("SNOWFLAKE_CI_WAREHOUSE", ""),
    "database": os.getenv("SNOWFLAKE_CI_DATABASE", "FEAST"),
    "schema": os.getenv("SNOWFLAKE_CI_SCHEMA_ONLINE", "ONLINE"),
}


@pytest.mark.parametrize("online_store", [SNOWFLAKE_ONLINE_CONFIG, "sqlite"])
@pytest.mark.integration
def test_snowflake_materialization_consistency(online_store):
    snowflake_config = IntegrationTestRepoConfig(
        online_store=online_store,
        offline_store_creator=SnowflakeDataSourceCreator,
        batch_engine=SNOWFLAKE_ENGINE_CONFIG,
    )
    snowflake_environment = construct_test_environment(snowflake_config, None)
    snowflake_environment.setup()

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


@pytest.mark.parametrize(
    "feature_dtype, feast_dtype",
    [
        ("string", Array(String)),
        ("bytes", Array(Bytes)),
        ("int32", Array(Int32)),
        ("int64", Array(Int64)),
        ("float", Array(Float64)),
        ("bool", Array(Bool)),
        ("datetime", Array(UnixTimestamp)),
    ],
)
@pytest.mark.parametrize("feature_is_empty_list", [False])
@pytest.mark.parametrize("online_store", [SNOWFLAKE_ONLINE_CONFIG, "sqlite"])
@pytest.mark.integration
def test_snowflake_materialization_consistency_internal_with_lists(
    feature_dtype, feast_dtype, feature_is_empty_list, online_store
):
    snowflake_config = IntegrationTestRepoConfig(
        online_store=online_store,
        offline_store_creator=SnowflakeDataSourceCreator,
        batch_engine=SNOWFLAKE_ENGINE_CONFIG,
    )
    snowflake_environment = construct_test_environment(snowflake_config, None)
    snowflake_environment.setup()

    df = create_basic_driver_dataset(Int32, feature_dtype, True, feature_is_empty_list)
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

    schema = [
        Field(name="driver_id", dtype=Int32),
        Field(name="value", dtype=feast_dtype),
    ]
    driver_stats_fv = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        ttl=timedelta(weeks=52),
        schema=schema,
        source=ds,
    )

    try:
        fs.apply([driver, driver_stats_fv])

        split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)

        print(f"Split datetime: {split_dt}")
        now = _utc_now()

        full_feature_names = True
        start_date = (now - timedelta(hours=5)).replace(tzinfo=timezone.utc)
        end_date = split_dt
        fs.materialize(
            feature_views=[driver_stats_fv.name],
            start_date=start_date,
            end_date=end_date,
        )

        expected_values = {
            "int32": [3] * 2,
            "int64": [3] * 2,
            "float": [3.0] * 2,
            "string": ["3"] * 2,
            "bytes": [b"3"] * 2,
            "bool": [False] * 2,
            "datetime": [datetime(1981, 1, 1, tzinfo=timezone.utc)] * 2,
        }
        expected_value = [] if feature_is_empty_list else expected_values[feature_dtype]

        response_dict = fs.get_online_features(
            [f"{driver_stats_fv.name}:value"],
            [{"driver_id": 1}],
            full_feature_names=full_feature_names,
        ).to_dict()

        actual_value = response_dict[f"{driver_stats_fv.name}__value"][0]
        assert actual_value is not None, f"Response: {response_dict}"
        if feature_dtype == "float":
            for actual_num, expected_num in zip(actual_value, expected_value):
                assert abs(actual_num - expected_num) < 1e-6, (
                    f"Response: {response_dict}, Expected: {expected_value}"
                )
        else:
            assert actual_value == expected_value

    finally:
        fs.teardown()
        snowflake_environment.data_source_creator.teardown()


@pytest.mark.integration
def test_snowflake_materialization_entityless_fv():
    snowflake_config = IntegrationTestRepoConfig(
        online_store=SNOWFLAKE_ONLINE_CONFIG,
        offline_store_creator=SnowflakeDataSourceCreator,
        batch_engine=SNOWFLAKE_ENGINE_CONFIG,
    )
    snowflake_environment = construct_test_environment(snowflake_config, None)
    snowflake_environment.setup()

    df = create_basic_driver_dataset()
    entityless_df = df.drop("driver_id", axis=1)
    ds = snowflake_environment.data_source_creator.create_data_source(
        entityless_df,
        snowflake_environment.feature_store.project,
        field_mapping={"ts_1": "ts"},
    )

    fs = snowflake_environment.feature_store

    # We include the driver entity so we can provide an entity ID when fetching features
    driver = Entity(
        name="driver_id",
        join_keys=["driver_id"],
    )

    overall_stats_fv = FeatureView(
        name="overall_hourly_stats",
        entities=[],
        ttl=timedelta(weeks=52),
        source=ds,
    )
    assert overall_stats_fv.entity_columns == []

    try:
        fs.apply([overall_stats_fv, driver])
        assert overall_stats_fv.entity_columns == [DUMMY_ENTITY_FIELD]

        # materialization is run in two steps and
        # we use timestamp from generated dataframe as a split point
        split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)

        print(f"Split datetime: {split_dt}")

        now = _utc_now()

        start_date = (now - timedelta(hours=5)).replace(tzinfo=timezone.utc)
        end_date = split_dt
        fs.materialize(
            feature_views=[overall_stats_fv.name],
            start_date=start_date,
            end_date=end_date,
        )

        response_dict = fs.get_online_features(
            [f"{overall_stats_fv.name}:value"],
            [{"driver_id": 1}],  # Included because we need an entity
        ).to_dict()
        assert response_dict["value"] == [0.3]

    finally:
        fs.teardown()
        snowflake_environment.data_source_creator.teardown()
