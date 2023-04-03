import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from feast import Entity, FeatureView, Field, FileSource
from feast.driver_test_data import (
    create_driver_hourly_stats_df,
    create_global_daily_stats_df,
)
from feast.feature_store import FeatureStore
from feast.types import Float32, String
from tests.utils.basic_read_write_test import basic_rw_test
from tests.utils.cli_repo_creator import CliRunner, get_example_repo
from tests.utils.feature_records import validate_online_features


def test_e2e_local() -> None:
    """
    Tests the end-to-end workflow of apply, materialize, and online retrieval.

    This test runs against several types of repos:
    1. A repo with a normal FV and an entity-less FV.
    2. A repo using the SDK from version 0.19.0.
    3. A repo with a FV with a ttl of 0.
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
            get_example_repo("example_feature_repo_2.py")
            .replace("%PARQUET_PATH%", driver_stats_path)
            .replace("%PARQUET_PATH_GLOBAL%", global_stats_path),
            "file",
        ) as store:
            _test_materialize_and_online_retrieval(
                runner, store, start_date, end_date, driver_df
            )

        with runner.local_repo(
            get_example_repo("example_feature_repo_with_bfvs.py")
            .replace("%PARQUET_PATH%", driver_stats_path)
            .replace("%PARQUET_PATH_GLOBAL%", global_stats_path),
            "file",
        ) as store:
            _test_materialize_and_online_retrieval(
                runner, store, start_date, end_date, driver_df
            )

        with runner.local_repo(
            get_example_repo("example_feature_repo_with_ttl_0.py")
            .replace("%PARQUET_PATH%", driver_stats_path)
            .replace("%PARQUET_PATH_GLOBAL%", global_stats_path),
            "file",
        ) as store:
            _test_materialize_and_online_retrieval(
                runner, store, start_date, end_date, driver_df
            )

        # Test a failure case when the parquet file doesn't include a join key
        with runner.local_repo(
            get_example_repo("example_feature_repo_with_entity_join_key.py").replace(
                "%PARQUET_PATH%", driver_stats_path
            ),
            "file",
        ) as store:
            assert store.repo_path is not None

            returncode, output = runner.run_with_output(
                [
                    "materialize",
                    start_date.isoformat(),
                    (end_date - timedelta(days=7)).isoformat(),
                ],
                cwd=Path(store.repo_path),
            )

            assert returncode != 0
            assert "feast.errors.FeastJoinKeysDuringMaterialization" in str(output)


def _test_materialize_and_online_retrieval(
    runner: CliRunner,
    store: FeatureStore,
    start_date: datetime,
    end_date: datetime,
    driver_df: pd.DataFrame,
):
    assert store.repo_path is not None

    # Test `feast materialize` and online retrieval.
    r = runner.run(
        [
            "materialize",
            start_date.isoformat(),
            (end_date - timedelta(days=7)).isoformat(),
        ],
        cwd=Path(store.repo_path),
    )

    assert r.returncode == 0, f"stdout: {r.stdout}\n stderr: {r.stderr}"
    validate_online_features(store, driver_df, end_date - timedelta(days=7))

    # Test `feast materialize-incremental` and online retrieval.
    r = runner.run(
        ["materialize-incremental", end_date.isoformat()],
        cwd=Path(store.repo_path),
    )

    assert r.returncode == 0, f"stdout: {r.stdout}\n stderr: {r.stderr}"
    validate_online_features(store, driver_df, end_date)


def test_partial() -> None:
    """
    Add another table to existing repo using partial apply API. Make sure both the table
    applied via CLI apply and the new table are passing RW test.
    """
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "file"
    ) as store:
        driver = Entity(name="driver", join_keys=["test"])

        driver_locations_source = FileSource(
            path="data/driver_locations.parquet",  # Fake path
            timestamp_field="event_timestamp",
            created_timestamp_column="created_timestamp",
        )

        driver_locations_100 = FeatureView(
            name="driver_locations_100",
            entities=[driver],
            ttl=timedelta(days=1),
            schema=[
                Field(name="lat", dtype=Float32),
                Field(name="lon", dtype=String),
                Field(name="name", dtype=String),
                Field(name="test", dtype=String),
            ],
            online=True,
            source=driver_locations_source,
            tags={},
        )

        store.apply([driver_locations_100])

        basic_rw_test(store, view_name="driver_locations")
        basic_rw_test(store, view_name="driver_locations_100")
