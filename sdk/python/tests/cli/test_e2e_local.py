import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd

import tests.driver_test_data as driver_data
from tests.cli.utils import CliRunner, get_example_repo


def _get_last_feature_row(df: pd.DataFrame, driver_id):
    filtered = df[df["driver_id"] == driver_id]
    max_ts = filtered.loc[filtered["datetime"].idxmax()]["datetime"]
    filtered_by_ts = filtered[filtered["datetime"] == max_ts]
    return filtered_by_ts.loc[filtered_by_ts["created"].idxmax()]


class TestLocalEndToEnd:
    def test_basic(self) -> None:
        """
            Add another table to existing repo using partial apply API. Make sure both the table
            applied via CLI apply and the new table are passing RW test.
        """

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as data_dir:
            end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
            start_date = end_date - timedelta(days=15)

            driver_entities = [1001, 1002, 1003, 1004, 1005]
            driver_df = driver_data.create_driver_hourly_stats_df(
                driver_entities, start_date, end_date
            )

            driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
            driver_df.to_parquet(
                path=driver_stats_path, allow_truncated_timestamps=True
            )

            with runner.local_repo(
                get_example_repo("example_feature_repo_2.py").replace(
                    "%PARQUET_PATH%", driver_stats_path
                )
            ) as store:

                repo_path = store.repo_path

                r = runner.run(
                    [
                        "materialize",
                        str(repo_path),
                        start_date.isoformat(),
                        end_date.isoformat(),
                    ],
                    cwd=repo_path,
                )
                assert r.returncode == 0
                result = store.get_online_features(
                    feature_refs=[
                        "driver_hourly_stats:conv_rate",
                        "driver_hourly_stats:avg_daily_trips",
                    ],
                    entity_rows=[{"driver_id": 1001}],
                )

                assert "driver_hourly_stats:avg_daily_trips" in result.to_dict()

                assert "driver_hourly_stats:conv_rate" in result.to_dict()
                assert (
                    abs(
                        result.to_dict()["driver_hourly_stats:conv_rate"][0]
                        - _get_last_feature_row(driver_df, 1001)["conv_rate"]
                    )
                    < 0.01
                )
