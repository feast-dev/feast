import os
import random
import string
import tempfile
from datetime import datetime, timedelta

import click
import pyarrow as pa
from tqdm import tqdm

from feast.data_source import FileSource
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_store import FeatureStore, _convert_arrow_to_proto
from feast.feature_view import FeatureView
from feast.repo_config import RepoConfig
from feast.value_type import ValueType
from tests.driver_test_data import create_driver_hourly_stats_df


def create_driver_hourly_stats_feature_view(source):
    driver_stats_feature_view = FeatureView(
        name="driver_stats",
        entities=["driver_id"],
        features=[
            Feature(name="conv_rate", dtype=ValueType.FLOAT),
            Feature(name="acc_rate", dtype=ValueType.FLOAT),
            Feature(name="avg_daily_trips", dtype=ValueType.INT32),
        ],
        input=source,
        ttl=timedelta(hours=2),
    )
    return driver_stats_feature_view


def create_driver_hourly_stats_source(parquet_path):
    return FileSource(
        path=parquet_path,
        event_timestamp_column="datetime",
        created_timestamp_column="created",
    )


@click.command(name="run")
def benchmark_writes():
    project_id = "test" + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        store = FeatureStore(
            config=RepoConfig(
                metadata_store=os.path.join(temp_dir, "metadata.db"),
                project=project_id,
                provider="gcp",
            )
        )

        # This is just to set data source to something, we're not reading from parquet source here.
        parquet_path = os.path.join(temp_dir, "data.parquet")

        driver = Entity(name="driver_id", value_type=ValueType.INT64, description="")
        table = create_driver_hourly_stats_feature_view(
            create_driver_hourly_stats_source(parquet_path=parquet_path)
        )
        store.apply([table, driver])

        online_store = store._get_online_store()
        provisioner = store._get_provisioner()

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=14)
        customers = list(range(100))
        data = create_driver_hourly_stats_df(customers, start_date, end_date)

        # Show the data for reference
        print(data)
        proto_data = _convert_arrow_to_proto(pa.Table.from_pandas(data), table)

        # Write it
        with tqdm(total=len(proto_data)) as progress:
            online_store.online_write_batch(
                project=store.project,
                table=table,
                data=proto_data,
                progress=progress.update,
            )

        registry_tables = store._get_registry().list_feature_views(
            project=store.project
        )
        provisioner.teardown_infra(store.project, tables=registry_tables)


if __name__ == "__main__":
    benchmark_writes()
