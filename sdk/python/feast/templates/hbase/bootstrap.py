from feast.file_utils import replace_str_in_file


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    driver_stats_path = data_path / "driver_stats.parquet"
    driver_df.to_parquet(path=str(driver_stats_path), allow_truncated_timestamps=True)

    example_py_file = repo_path / "example_repo.py"
    replace_str_in_file(
        example_py_file, "%PARQUET_PATH%", str(driver_stats_path.relative_to(repo_path))
    )


if __name__ == "__main__":
    bootstrap()
