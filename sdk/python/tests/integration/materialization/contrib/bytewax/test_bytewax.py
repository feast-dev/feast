from datetime import timedelta

import pytest

from feast import Entity, Feature, FeatureView, ValueType
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
@pytest.mark.skip(reason="Run this test manually after creating an EKS cluster.")
def test_bytewax_materialization():
    bytewax_config = IntegrationTestRepoConfig(
        provider="aws",
        online_store={"type": "dynamodb", "region": "us-west-2"},
        offline_store_creator=RedshiftDataSourceCreator,
        batch_engine={
            "type": "bytewax",
        },
        registry_location=RegistryLocation.S3,
    )
    bytewax_environment = construct_test_environment(bytewax_config, None)

    df = create_basic_driver_dataset()
    ds = bytewax_environment.data_source_creator.create_data_source(
        df,
        bytewax_environment.feature_store.project,
        field_mapping={"ts_1": "ts"},
    )

    fs = bytewax_environment.feature_store
    driver = Entity(
        name="driver_id",
        join_key="driver_id",
        value_type=ValueType.INT64,
    )

    driver_stats_fv = FeatureView(
        name="driver_hourly_stats",
        entities=["driver_id"],
        ttl=timedelta(weeks=52),
        features=[Feature(name="value", dtype=ValueType.FLOAT)],
        batch_source=ds,
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
