from datetime import timedelta

import pytest

from feast import Entity, Feature, FeatureView, ValueType
from tests.data.data_creator import create_basic_driver_dataset
from tests.universal.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
    RegistryLocation,
)
from tests.universal.feature_repos.repo_configuration import (
    construct_test_environment,
)
from tests.universal.feature_repos.universal.data_sources.redshift import (
    RedshiftDataSourceCreator,
)
from tests.utils.e2e_test_validation import validate_offline_online_store_consistency


@pytest.mark.integration
@pytest.mark.skip(
    reason="Run manually against a Kubernetes cluster with the Apache Flink "
    "Kubernetes Operator installed, a feast-flink image on a reachable registry, "
    "and the `flink` service account granted the operator RBAC "
    "(incl. deployments/finalizers). Mirrors test_k8s.py."
)
def test_flink_application_materialization():
    """Full operator-path e2e for the flink_application engine.

    Submits a FlinkDeployment via the Apache Flink Kubernetes Operator and
    verifies online values and registry intervals through
    ``validate_offline_online_store_consistency`` (network-accessible stores
    only, since the engine rejects file-based backends).
    """
    config = IntegrationTestRepoConfig(
        provider="aws",
        online_store={"type": "dynamodb", "region": "us-west-2"},
        offline_store_creator=RedshiftDataSourceCreator,
        batch_engine={
            "type": "flink_application",
            # Image built from sdk/python/feast/infra/compute_engines/flink_application/Dockerfile
            "image": "quay.io/<your-org>/feast-flink:latest",
            "namespace": "feast",
            "service_account": "flink",
            "flink_version": "v2_2",
        },
        registry_location=RegistryLocation.S3,
    )
    env = construct_test_environment(config, None)

    df = create_basic_driver_dataset()
    ds = env.data_source_creator.create_data_source(
        df,
        env.feature_store.project,
        field_mapping={"ts_1": "ts"},
    )

    fs = env.feature_store
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

        split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)

        validate_offline_online_store_consistency(fs, driver_stats_fv, split_dt)
    finally:
        fs.teardown()
