import pytest
from google.protobuf.duration_pb2 import Duration

from feast import BigQuerySource, Feature, FeatureView, ValueType
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
            table_ref="feast-oss.public.drivers",
            event_timestamp_column="event_timestamp",
            created_timestamp_column="created_timestamp",
        )

        driver_locations_100 = FeatureView(
            name="driver_locations_100",
            entities=["driver"],
            ttl=Duration(seconds=86400 * 1),
            features=[
                Feature(name="lat", dtype=ValueType.FLOAT),
                Feature(name="lon", dtype=ValueType.STRING),
                Feature(name="name", dtype=ValueType.STRING),
            ],
            online=True,
            batch_source=driver_locations_source,
            tags={},
        )

        store.apply([driver_locations_100])

        basic_rw_test(store, view_name="driver_locations")
        basic_rw_test(store, view_name="driver_locations_100")
