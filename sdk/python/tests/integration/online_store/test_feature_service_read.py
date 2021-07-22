import pytest

from tests.utils.cli_utils import CliRunner, get_example_repo
from tests.utils.online_read_write_test import basic_rw_test


@pytest.mark.integration
def test_feature_service_read() -> None:
    """
    Read feature values from the FeatureStore using a FeatureService.
    """

    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "bigquery"
    ) as store:

        basic_rw_test(
            store,
            view_name="driver_locations",
            feature_service_name="driver_locations_service",
        )
