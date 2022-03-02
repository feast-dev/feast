from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from feast.driver_test_data import (
    create_customer_daily_profile_df,
    create_driver_hourly_stats_df,
)

CURRENT_DIR = Path(__file__).parent
DRIVER_ENTITIES = [1001, 1002, 1003]
CUSTOMER_ENTITIES = [201, 202, 203]
START_DATE = datetime.strptime("2022-01-01", "%Y-%m-%d")
END_DATE = START_DATE + timedelta(days=7)


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`
    generate_example_data(
        spark_session=SparkSession.builder.getOrCreate(), base_dir=str(CURRENT_DIR),
    )


def example_data_exists(base_dir: str) -> bool:
    for path in [
        Path(base_dir) / "data" / "driver_hourly_stats",
        Path(base_dir) / "data" / "customer_daily_profile",
    ]:
        if not path.exists():
            return False
    return True


def generate_example_data(spark_session: SparkSession, base_dir: str) -> None:
    spark_session.createDataFrame(
        data=create_driver_hourly_stats_df(DRIVER_ENTITIES, START_DATE, END_DATE)
    ).write.parquet(
        path=str(Path(base_dir) / "data" / "driver_hourly_stats"), mode="overwrite",
    )

    spark_session.createDataFrame(
        data=create_customer_daily_profile_df(CUSTOMER_ENTITIES, START_DATE, END_DATE)
    ).write.parquet(
        path=str(Path(base_dir) / "data" / "customer_daily_profile"), mode="overwrite",
    )


if __name__ == "__main__":
    bootstrap()
