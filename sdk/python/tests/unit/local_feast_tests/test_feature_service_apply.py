from feast.feature_service import FeatureService
from tests.utils.cli_repo_creator import CliRunner, get_example_repo


def test_read_pre_applied() -> None:
    """
    Read feature values from the FeatureStore using a FeatureService.
    """
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_with_feature_service.py"), "file"
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
