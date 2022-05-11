from datetime import timedelta

import pytest

from feast import BigQuerySource, FeatureView, Field
from feast.types import Float32, String
from tests.utils.cli_utils import CliRunner, get_example_repo
from tests.utils.online_read_write_test import basic_rw_test


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

        driver_locations_source = BigQuerySource(
            table="feast-oss.public.drivers",
            timestamp_field="event_timestamp",
            created_timestamp_column="created_timestamp",
        )

        driver_locations_100 = FeatureView(
            name="driver_locations_100",
            entities=["driver"],
            ttl=timedelta(days=1),
            schema=[
                Field(name="lat", dtype=Float32),
                Field(name="lon", dtype=String),
                Field(name="name", dtype=String),
            ],
            online=True,
            batch_source=driver_locations_source,
            tags={},
        )

        store.apply([driver_locations_100])

        basic_rw_test(store, view_name="driver_locations")
        basic_rw_test(store, view_name="driver_locations_100")
