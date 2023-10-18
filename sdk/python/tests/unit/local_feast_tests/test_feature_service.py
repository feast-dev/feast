import os
import tempfile
from datetime import datetime, timedelta

from feast.driver_test_data import (
    create_driver_hourly_stats_df,
    create_global_daily_stats_df,
)
from tests.utils.basic_read_write_test import basic_rw_test
from tests.utils.cli_repo_creator import CliRunner, get_example_repo


def test_apply_without_fv_inference() -> None:
    """
    Tests that feature services based on feature views that do not require inference can be applied correctly.
    """
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_with_feature_service_2.py"), "file"
    ) as store:
        assert len(store.list_feature_services()) == 2

        fs = store.get_feature_service("all_stats")
        assert len(fs.feature_view_projections) == 2
        assert len(fs.feature_view_projections[0].features) == 3
        assert len(fs.feature_view_projections[0].desired_features) == 0
        assert len(fs.feature_view_projections[1].features) == 2
        assert len(fs.feature_view_projections[1].desired_features) == 0
        assert len(fs.tags) == 1
        assert fs.tags["release"] == "production"

        fs = store.get_feature_service("some_stats")
        assert len(fs.feature_view_projections) == 2
        assert len(fs.feature_view_projections[0].features) == 1
        assert len(fs.feature_view_projections[0].desired_features) == 0
        assert len(fs.feature_view_projections[0].features) == 1
        assert len(fs.feature_view_projections[0].desired_features) == 0


def test_apply_with_fv_inference() -> None:
    """
    Tests that feature services based on feature views that require inference can be applied correctly.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as data_dir:
        # Generate test data.
        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=15)

        driver_entities = [1001, 1002, 1003, 1004, 1005]
        driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)
        driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
        driver_df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)

        global_df = create_global_daily_stats_df(start_date, end_date)
        global_stats_path = os.path.join(data_dir, "global_stats.parquet")
        global_df.to_parquet(path=global_stats_path, allow_truncated_timestamps=True)

        with runner.local_repo(
            get_example_repo("example_feature_repo_with_feature_service_3.py")
            .replace("%PARQUET_PATH%", driver_stats_path)
            .replace("%PARQUET_PATH_GLOBAL%", global_stats_path),
            "file",
        ) as store:
            assert len(store.list_feature_services()) == 2

            fs = store.get_feature_service("all_stats")
            assert len(fs.feature_view_projections) == 2
            assert len(fs.feature_view_projections[0].features) == 3
            assert len(fs.feature_view_projections[0].desired_features) == 0
            assert len(fs.feature_view_projections[1].features) == 2
            assert len(fs.feature_view_projections[1].desired_features) == 0
            assert len(fs.tags) == 1
            assert fs.tags["release"] == "production"

            fs = store.get_feature_service("some_stats")
            assert len(fs.feature_view_projections) == 2
            assert len(fs.feature_view_projections[0].features) == 1
            assert len(fs.feature_view_projections[0].desired_features) == 0
            assert len(fs.feature_view_projections[0].features) == 1
            assert len(fs.feature_view_projections[0].desired_features) == 0


def test_read() -> None:
    """
    Test that feature values are correctly read through a feature service.
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
