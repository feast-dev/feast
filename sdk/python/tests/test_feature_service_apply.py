import unittest
from typing import Any

from feast import FeatureService, FeatureStore
from tests.cli_utils import CliRunner, get_example_repo


class FeatureServiceTestCase(unittest.TestCase):

    repo: Any
    store: FeatureStore

    @classmethod
    def setUpClass(cls) -> None:
        runner = CliRunner()
        cls.repo = runner.local_repo(
            get_example_repo("example_feature_repo_1.py"), "bigquery"
        )
        cls.store = cls.repo.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.repo.__exit__(None, None, None)

    def test_read_pre_applied(self):
        assert len(self.store.list_feature_services()) == 2
        fs = self.store.get_feature_service("driver_locations")
        assert len(fs.tags) == 1
        assert fs.tags["release"] == "production"

        fv = self.store.get_feature_view("driver_locations")

        fs = FeatureService(name="new_feature_service", features=[fv[["lon"]]])

        self.store.apply([fs])

        assert len(self.store.list_feature_services()) == 3
        self.store.get_feature_service("new_feature_service")
