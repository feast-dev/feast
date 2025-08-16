from feast.file_utils import replace_str_in_file


def bootstrap():
    import pathlib
    from datetime import datetime, timedelta

    import numpy as np
    import pandas as pd

    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    project_name = pathlib.Path(__file__).parent.absolute().name
    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    # Generate driver data using Feast's built-in test data generator
    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    if driver_df["event_timestamp"].dt.tz is None:
        driver_df["event_timestamp"] = driver_df["event_timestamp"].dt.tz_localize(
            "UTC"
        )
    if "created" in driver_df.columns and driver_df["created"].dt.tz is None:
        driver_df["created"] = driver_df["created"].dt.tz_localize("UTC")

    driver_stats_path = data_path / "driver_stats.parquet"
    driver_df.to_parquet(path=str(driver_stats_path), allow_truncated_timestamps=True)

    # Generate customer data to demonstrate Ray's multi-source capabilities
    customer_entities = [2001, 2002, 2003, 2004, 2005]

    # Create customer daily profile data
    customer_data = []
    for customer_id in customer_entities:
        for i, single_date in enumerate(
            pd.date_range(start_date, end_date, freq="D", tz="UTC")
        ):
            stable_timestamp = single_date.replace(
                hour=12, minute=0, second=0, microsecond=0
            )
            customer_data.append(
                {
                    "customer_id": customer_id,
                    "event_timestamp": stable_timestamp,
                    "created": stable_timestamp + timedelta(minutes=10),
                    "current_balance": np.random.uniform(10.0, 1000.0),
                    "avg_passenger_count": np.random.uniform(1.0, 4.0),
                    "lifetime_trip_count": np.random.randint(50, 500),
                }
            )

    customer_df = pd.DataFrame(customer_data)

    if customer_df["event_timestamp"].dt.tz is None:
        customer_df["event_timestamp"] = customer_df["event_timestamp"].dt.tz_localize(
            "UTC"
        )
    if customer_df["created"].dt.tz is None:
        customer_df["created"] = customer_df["created"].dt.tz_localize("UTC")

    customer_profile_path = data_path / "customer_daily_profile.parquet"
    customer_df.to_parquet(
        path=str(customer_profile_path), allow_truncated_timestamps=True
    )

    # Update the example_repo.py file with actual paths
    example_py_file = repo_path / "example_repo.py"
    replace_str_in_file(example_py_file, "%PROJECT_NAME%", str(project_name))
    replace_str_in_file(
        example_py_file, "%PARQUET_PATH%", str(driver_stats_path.relative_to(repo_path))
    )
    replace_str_in_file(
        example_py_file, "%LOGGING_PATH%", str(data_path.relative_to(repo_path))
    )

    print("Ray template initialized with sample data:")
    print(f"  - Driver stats: {driver_stats_path}")
    print(f"  - Customer profiles: {customer_profile_path}")
    print(f"  - Ray storage will be created at: {data_path / 'ray_storage'}")
    print("\nTo get started:")
    print(f"  1. cd {project_name}/feature_repo")
    print("  2. feast apply")
    print("  3. python test_workflow.py")


if __name__ == "__main__":
    bootstrap()
