import pytest

<<<<<<< HEAD:sdk/python/tests/unit/local_feast_tests/test_feature_service_read.py
from tests.utils.basic_read_write_test import basic_rw_test
from tests.utils.cli_repo_creator import CliRunner, get_example_repo
=======
from tests.utils.cli_utils import CliRunner, get_example_repo
from tests.utils.online_read_write_test_utils import basic_rw_test
>>>>>>> d02455b88 (Verify tests):sdk/python/tests/integration/online_store/test_feature_service_read.py


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
