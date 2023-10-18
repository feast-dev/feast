def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`
    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import (
        create_customer_daily_profile_df,
        create_driver_hourly_stats_df,
    )

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    driver_entities = [1001, 1002, 1003]
    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)
    driver_stats_df = create_driver_hourly_stats_df(
        driver_entities, start_date, end_date
    )
    driver_stats_df.to_parquet(
        path=str(data_path / "driver_hourly_stats.parquet"),
        allow_truncated_timestamps=True,
    )

    customer_entities = [201, 202, 203]
    customer_profile_df = create_customer_daily_profile_df(
        customer_entities, start_date, end_date
    )
    customer_profile_df.to_parquet(
        path=str(data_path / "customer_daily_profile.parquet"),
        allow_truncated_timestamps=True,
    )


if __name__ == "__main__":
    bootstrap()
