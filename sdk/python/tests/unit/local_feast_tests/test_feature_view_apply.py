from feast.entity import Entity
from feast.feature_view import FeatureView
from tests.utils.cli_repo_creator import CliRunner, get_example_repo
from tests.utils.data_source_test_creator import prep_file_source


def test_apply_feature_view(simple_dataset_1) -> None:
    """Test that a feature view can be applied correctly."""
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("empty_feature_repo.py"), "file"
    ) as fs, prep_file_source(
        df=simple_dataset_1, timestamp_field="ts_1"
    ) as file_source:
        entity = Entity(name="driver_entity", join_keys=["test_key"])
        driver_fv = FeatureView(
            name="driver_fv",
            entities=[entity],
            source=file_source,
        )

        fs.apply([entity, driver_fv])

        fvs = fs.list_feature_views()
        assert len(fvs) == 1
        assert fvs[0] == driver_fv
