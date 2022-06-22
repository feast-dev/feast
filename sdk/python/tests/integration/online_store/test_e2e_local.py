import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pytz import utc

from feast.driver_test_data import (
    create_driver_hourly_stats_df,
    create_global_daily_stats_df,
)
from feast.feature_store import FeatureStore
from tests.utils.cli_utils import CliRunner, get_example_repo


def _get_last_feature_row(df: pd.DataFrame, driver_id, max_date: datetime):
    """Manually extract last feature value from a dataframe for a given driver_id with up to `max_date` date"""
    filtered = df[
        (df["driver_id"] == driver_id)
        & (df["event_timestamp"] < max_date.replace(tzinfo=utc))
    ]
    max_ts = filtered.loc[filtered["event_timestamp"].idxmax()]["event_timestamp"]
    filtered_by_ts = filtered[filtered["event_timestamp"] == max_ts]
    return filtered_by_ts.loc[filtered_by_ts["created"].idxmax()]


def _assert_online_features(
    store: FeatureStore, driver_df: pd.DataFrame, max_date: datetime
):
    """Assert that features in online store are up to date with `max_date` date."""
    # Read features back
    response = store.get_online_features(
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:avg_daily_trips",
            "global_daily_stats:num_rides",
            "global_daily_stats:avg_ride_length",
        ],
        entity_rows=[{"driver_id": 1001}],
        full_feature_names=True,
    )

    # Float features should still be floats.
    assert (
        response.proto.results[
            list(response.proto.metadata.feature_names.val).index(
                "driver_hourly_stats__conv_rate"
            )
        ]
        .values[0]
        .float_val
        > 0
    )

    result = response.to_dict()
    assert len(result) == 5
    assert "driver_hourly_stats__avg_daily_trips" in result
    assert "driver_hourly_stats__conv_rate" in result
    assert (
        abs(
            result["driver_hourly_stats__conv_rate"][0]
            - _get_last_feature_row(driver_df, 1001, max_date)["conv_rate"]
        )
        < 0.01
    )
    assert "global_daily_stats__num_rides" in result
    assert "global_daily_stats__avg_ride_length" in result

    # Test the ODFV if it exists.
    odfvs = store.list_on_demand_feature_views()
    if odfvs and odfvs[0].name == "conv_rate_plus_100":
        response = store.get_online_features(
            features=[
                "conv_rate_plus_100:conv_rate_plus_100",
                "conv_rate_plus_100:conv_rate_plus_val_to_add",
            ],
            entity_rows=[{"driver_id": 1001, "val_to_add": 100}],
            full_feature_names=True,
        )

        # Check that float64 feature is stored correctly in proto format.
        assert (
            response.proto.results[
                list(response.proto.metadata.feature_names.val).index(
                    "conv_rate_plus_100__conv_rate_plus_100"
                )
            ]
            .values[0]
            .double_val
            > 0
        )

        result = response.to_dict()
        assert len(result) == 3
        assert "conv_rate_plus_100__conv_rate_plus_100" in result
        assert "conv_rate_plus_100__conv_rate_plus_val_to_add" in result
        assert (
            abs(
                result["conv_rate_plus_100__conv_rate_plus_100"][0]
                - (_get_last_feature_row(driver_df, 1001, max_date)["conv_rate"] + 100)
            )
            < 0.01
        )
        assert (
            abs(
                result["conv_rate_plus_100__conv_rate_plus_val_to_add"][0]
                - (_get_last_feature_row(driver_df, 1001, max_date)["conv_rate"] + 100)
            )
            < 0.01
        )


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
    _assert_online_features(store, driver_df, end_date - timedelta(days=7))

    # Test `feast materialize-incremental` and online retrieval.
    r = runner.run(
        ["materialize-incremental", end_date.isoformat()], cwd=Path(store.repo_path),
    )

    assert r.returncode == 0, f"stdout: {r.stdout}\n stderr: {r.stderr}"
    _assert_online_features(store, driver_df, end_date)


def test_e2e_local() -> None:
    """
    Tests the end-to-end workflow of apply, materialize, and online retrieval.

    This test runs against several different types of repos:
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
            get_example_repo("example_feature_repo_version_0_19.py")
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
