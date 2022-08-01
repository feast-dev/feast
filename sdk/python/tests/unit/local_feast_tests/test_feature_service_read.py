from tests.utils.basic_read_write_test import basic_rw_test
from tests.utils.cli_repo_creator import CliRunner, get_example_repo


def test_feature_service_read() -> None:
    """
    Read feature values from the FeatureStore using a FeatureService.
    """
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_with_feature_service.py"), "file"
    ) as store:
        basic_rw_test(
            store,
            view_name="driver_locations",
            feature_service_name="driver_locations_service",
        )
