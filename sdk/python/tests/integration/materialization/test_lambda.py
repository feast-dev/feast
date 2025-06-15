from datetime import timedelta

import pytest

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.types import Float32
from tests.data.data_creator import create_basic_driver_dataset
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
    RegistryLocation,
)
from tests.integration.feature_repos.repo_configuration import (
    construct_test_environment,
)
from tests.integration.feature_repos.universal.data_sources.redshift import (
    RedshiftDataSourceCreator,
)
from tests.utils.e2e_test_validation import validate_offline_online_store_consistency


@pytest.mark.integration
@pytest.mark.skip(reason="Very flaky test")
def test_lambda_materialization_consistency():
    lambda_config = IntegrationTestRepoConfig(
        provider="aws",
        online_store={
            "type": "dynamodb",
            "region": "us-west-2",
            "consistent_reads": True,
        },
        offline_store_creator=RedshiftDataSourceCreator,
        batch_engine={
            "type": "lambda",
            "materialization_image": "402087665549.dkr.ecr.us-west-2.amazonaws.com/feast-lambda-consumer:v2",
            "lambda_role": "arn:aws:iam::402087665549:role/lambda_execution_role",
        },
        registry_location=RegistryLocation.S3,
    )
    lambda_environment = construct_test_environment(
        lambda_config, None, entity_key_serialization_version=3
    )

    df = create_basic_driver_dataset()
    ds = lambda_environment.data_source_creator.create_data_source(
        df,
        lambda_environment.feature_store.project,
        field_mapping={"ts_1": "ts"},
    )

    fs = lambda_environment.feature_store
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
