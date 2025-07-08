from datetime import datetime, timedelta

import pandas as pd

from feast import Entity
from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.spark_offline_store.tests.data_source import (
    SparkDataSourceCreator,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import (
    construct_test_environment,
)
from tests.integration.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)

now = datetime.now()
today = datetime.today()

driver = Entity(
    name="driver_id",
    description="driver id",
)


def create_entity_df() -> pd.DataFrame:
    entity_df = pd.DataFrame(
        [
            {"driver_id": 1001, "event_timestamp": today},
            {"driver_id": 1002, "event_timestamp": today},
        ]
    )
    return entity_df


def create_feature_dataset(spark_environment) -> DataSource:
    yesterday = today - timedelta(days=1)
    last_week = today - timedelta(days=7)
    df = pd.DataFrame(
        [
            {
                "driver_id": 1001,
                "event_timestamp": yesterday,
                "created": now - timedelta(hours=2),
                "conv_rate": 0.8,
                "acc_rate": 0.5,
                "avg_daily_trips": 15,
            },
            {
                "driver_id": 1001,
                "event_timestamp": last_week,
                "created": now - timedelta(hours=3),
                "conv_rate": 0.75,
                "acc_rate": 0.9,
                "avg_daily_trips": 14,
            },
            {
                "driver_id": 1002,
                "event_timestamp": yesterday,
                "created": now - timedelta(hours=2),
                "conv_rate": 0.7,
                "acc_rate": 0.4,
                "avg_daily_trips": 12,
            },
            {
                "driver_id": 1002,
                "event_timestamp": yesterday - timedelta(days=1),
                "created": now - timedelta(hours=2),
                "conv_rate": 0.3,
                "acc_rate": 0.6,
                "avg_daily_trips": 12,
            },
        ]
    )
    ds = spark_environment.data_source_creator.create_data_source(
        df,
        spark_environment.feature_store.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    return ds


def create_spark_environment():
    spark_config = IntegrationTestRepoConfig(
        provider="local",
        online_store_creator=RedisOnlineStoreCreator,
        offline_store_creator=SparkDataSourceCreator,
        batch_engine={"type": "spark.engine", "partitions": 10},
    )
    spark_environment = construct_test_environment(
        spark_config, None, entity_key_serialization_version=3
    )
    spark_environment.setup()
    return spark_environment


def _check_online_features(
    fs,
    driver_id,
    feature,
    expected_value,
    full_feature_names: bool = True,
):
    online_response = fs.get_online_features(
        features=[feature],
        entity_rows=[{"driver_id": driver_id}],
        full_feature_names=full_feature_names,
    ).to_dict()

    feature_ref = "__".join(feature.split(":"))

    assert len(online_response["driver_id"]) == 1
    assert online_response["driver_id"][0] == driver_id
    assert abs(online_response[feature_ref][0] - expected_value < 1e-6), (
        "Transformed result"
    )


def _check_offline_features(
    fs,
    feature,
    entity_df,
    size: int = 4,
):
    offline_df = fs.get_historical_features(
        entity_df=entity_df,
        features=[feature],
    ).to_df()
    assert len(offline_df) == size
