from datetime import timedelta

import pytest

from feast import BigQuerySource, Entity, FeatureView, Field
from feast.feature_service import FeatureService
from feast.types import Float32, String
from tests.integration.feature_repos.universal.entities import driver
from tests.integration.feature_repos.universal.feature_views import driver_feature_view
from tests.utils.basic_read_write_test import basic_rw_test
from tests.utils.cli_repo_creator import CliRunner, get_example_repo
from tests.utils.e2e_test_validation import validate_offline_online_store_consistency


@pytest.mark.integration
@pytest.mark.universal_online_stores
@pytest.mark.parametrize("infer_features", [True, False])
def test_e2e_consistency(environment, e2e_data_sources, infer_features):
    fs = environment.feature_store
    df, data_source = e2e_data_sources
    fv = driver_feature_view(
        name=f"test_consistency_{'with_inference' if infer_features else ''}",
        data_source=data_source,
        infer_features=infer_features,
    )

    entity = driver()
    fs.apply([fv, entity])

    # materialization is run in two steps and
    # we use timestamp from generated dataframe as a split point
    split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)

    validate_offline_online_store_consistency(fs, fv, split_dt)


@pytest.mark.integration
def test_partial() -> None:
    """
    Add another table to existing repo using partial apply API. Make sure both the table
    applied via CLI apply and the new table are passing RW test.
    """

    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "bigquery"
    ) as store:
        driver = Entity(name="driver", join_keys=["test"])

        driver_locations_source = BigQuerySource(
            table="feast-oss.public.drivers",
            timestamp_field="event_timestamp",
            created_timestamp_column="created_timestamp",
        )

        driver_locations_100 = FeatureView(
            name="driver_locations_100",
            entities=[driver],
            ttl=timedelta(days=1),
            schema=[
                Field(name="lat", dtype=Float32),
                Field(name="lon", dtype=String),
                Field(name="name", dtype=String),
                Field(name="test", dtype=String),
            ],
            online=True,
            batch_source=driver_locations_source,
            tags={},
        )

        store.apply([driver_locations_100])

        basic_rw_test(store, view_name="driver_locations")
        basic_rw_test(store, view_name="driver_locations_100")


@pytest.mark.integration
def test_read_pre_applied() -> None:
    """
    Read feature values from the FeatureStore using a FeatureService.
    """
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "bigquery"
    ) as store:

        assert len(store.list_feature_services()) == 1
        fs = store.get_feature_service("driver_locations_service")
        assert len(fs.tags) == 1
        assert fs.tags["release"] == "production"

        fv = store.get_feature_view("driver_locations")

        fs = FeatureService(name="new_feature_service", features=[fv[["lon"]]])

        store.apply([fs])

        assert len(store.list_feature_services()) == 2
        store.get_feature_service("new_feature_service")
