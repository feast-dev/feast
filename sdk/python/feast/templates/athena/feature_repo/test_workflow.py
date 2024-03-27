import os
from datetime import datetime, timedelta

import pandas as pd

from feast import Entity, FeatureStore, FeatureView, Field
from feast.infra.offline_stores.contrib.athena_offline_store.athena_source import (
    AthenaSource,
)
from feast.types import Float64, Int64


def test_end_to_end():
    try:
        # Before running this test method
        # 1. Upload the driver_stats.parquet file to your S3 bucket.
        # (https://github.com/feast-dev/feast-custom-offline-store-demo/tree/main/feature_repo/data)
        # 2. Using AWS Glue Crawler, create a table in the data catalog. The generated table can be queried through Athena.
        # 3. Specify the S3 bucket name, data source(AwsDataCatalog), database name, Athena's workgroup, etc. in feature_store.yaml

        fs = FeatureStore("./feature_repo")

        # Partition pruning has a significant impact on Athena's query performance and cost.
        # If offline feature dataset is large, it is highly recommended to create partitions using date columns such as ('created','event_timestamp')
        # The date_partition_column must be in form of YYYY-MM-DD(string) as in the beginning of the date column.

        driver_hourly_stats = AthenaSource(
            timestamp_field="event_timestamp",
            table="driver_stats",
            # table="driver_stats_partitioned",
            database="sampledb",
            data_source="AwsDataCatalog",
            created_timestamp_column="created",
            # date_partition_column="std_date"  #YYYY-MM-DD
        )

        driver = Entity(
            name="driver_id",
            description="driver id",
        )

        driver_hourly_stats_view = FeatureView(
            name="driver_hourly_stats",
            entities=[driver],
            ttl=timedelta(days=500),
            schema=[
                Field(name="conv_rate", dtype=Float64),
                Field(name="acc_rate", dtype=Float64),
                Field(name="avg_daily_trips", dtype=Int64),
            ],
            online=True,
            source=driver_hourly_stats,
        )

        # apply repository
        fs.apply([driver_hourly_stats, driver, driver_hourly_stats_view])
        print(fs.list_data_sources())
        print(fs.list_feature_views())

        entity_df = pd.DataFrame(
            {"driver_id": [1001], "event_timestamp": [datetime.now()]}
        )

        # Read features from offline store
        feature_vector = (
            fs.get_historical_features(
                features=["driver_hourly_stats:conv_rate"], entity_df=entity_df
            )
            .to_df()
            .to_dict()
        )
        conv_rate = feature_vector["conv_rate"][0]
        print(conv_rate)
        assert conv_rate > 0

        # load data into online store
        fs.materialize_incremental(end_date=datetime.now())

        online_response = fs.get_online_features(
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",
            ],
            entity_rows=[{"driver_id": 1002}],
        )
        online_response_dict = online_response.to_dict()
        print(online_response_dict)

    except Exception as e:
        print(e)
    finally:
        # tear down feature store
        fs.teardown()


def test_cli():
    os.system("PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c feature_repo apply")
    try:
        os.system("PYTHONPATH=$PYTHONPATH:/$(pwd) ")
        with open("output", "r") as f:
            output = f.read()

        if "Pulling latest features from my offline store" not in output:
            raise Exception(
                'Failed to successfully use provider from CLI. See "output" for more details.'
            )
    finally:
        os.system("PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c feature_repo teardown")


if __name__ == "__main__":
    # pass
    test_end_to_end()
    test_cli()
