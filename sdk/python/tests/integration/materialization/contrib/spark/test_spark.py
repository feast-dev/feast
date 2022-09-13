from datetime import timedelta

import pytest

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.contrib.spark_offline_store.tests.data_source import (
    SparkDataSourceCreator,
)
from feast.types import Float32
from tests.data.data_creator import create_basic_driver_dataset
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import (
    construct_test_environment,
)
from tests.integration.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)
from tests.utils.e2e_test_validation import validate_offline_online_store_consistency


@pytest.mark.integration
def test_spark_materialization_consistency():
    spark_config = IntegrationTestRepoConfig(
        provider="local",
        online_store_creator=RedisOnlineStoreCreator,
        offline_store_creator=SparkDataSourceCreator,
        batch_engine={"type": "spark.engine", "partitions": 10},
    )
    spark_environment = construct_test_environment(
        spark_config, None, entity_key_serialization_version=1
    )

    df = create_basic_driver_dataset()

    ds = spark_environment.data_source_creator.create_data_source(
        df,
        spark_environment.feature_store.project,
        field_mapping={"ts_1": "ts"},
    )

    fs = spark_environment.feature_store
    driver = Entity(
        name="driver_id",
        join_keys=["driver_id"],
    )

    driver_stats_fv = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        ttl=timedelta(weeks=52),
        schema=[Field(name="value", dtype=Float32)],
        source=ds,
    )

    try:

        fs.apply([driver, driver_stats_fv])

        print(df)

        # materialization is run in two steps and
        # we use timestamp from generated dataframe as a split point
        split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)

        print(f"Split datetime: {split_dt}")

        validate_offline_online_store_consistency(fs, driver_stats_fv, split_dt)
    finally:
        fs.teardown()


if __name__ == "__main__":
    test_spark_materialization_consistency()
