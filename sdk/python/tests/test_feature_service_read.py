import pytest
from google.protobuf.duration_pb2 import Duration

from feast import BigQuerySource, Feature, FeatureView, ValueType
from tests.cli_utils import CliRunner, get_example_repo
from tests.online_read_write_test import basic_feature_service_rw_test, basic_rw_test


@pytest.mark.integration
def test_feature_service_read() -> None:
    """
    Read feature values from the FeatureStore using a FeatureService.
    """

    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "bigquery"
    ) as store:

        assert len(store.list_feature_services()) == 2

        basic_feature_service_rw_test(
            store, view_name="driver_locations", feature_service_name="driver_locations"
        )
