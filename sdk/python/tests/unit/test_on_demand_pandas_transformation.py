import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd

from feast import Entity, FeatureStore, FeatureView, FileSource, RepoConfig
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.field import Field
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64


def test_pandas_transformation():
    with tempfile.TemporaryDirectory() as data_dir:
        store = FeatureStore(
            config=RepoConfig(
                project="test_on_demand_python_transformation",
                registry=os.path.join(data_dir, "registry.db"),
                provider="local",
                entity_key_serialization_version=2,
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(data_dir, "online.db")
                ),
            )
        )

        # Generate test data.
        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=15)

        driver_entities = [1001, 1002, 1003, 1004, 1005]
        driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)
        driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
        driver_df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)

        driver = Entity(name="driver", join_keys=["driver_id"])

        driver_stats_source = FileSource(
            name="driver_hourly_stats_source",
            path=driver_stats_path,
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
        )

        driver_stats_fv = FeatureView(
            name="driver_hourly_stats",
            entities=[driver],
            ttl=timedelta(days=0),
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="acc_rate", dtype=Float32),
                Field(name="avg_daily_trips", dtype=Int64),
            ],
            online=True,
            source=driver_stats_source,
        )

        @on_demand_feature_view(
            sources=[driver_stats_fv],
            schema=[Field(name="conv_rate_plus_acc", dtype=Float64)],
            mode="pandas",
        )
        def pandas_view(inputs: pd.DataFrame) -> pd.DataFrame:
            df = pd.DataFrame()
            df["conv_rate_plus_acc"] = inputs["conv_rate"] + inputs["acc_rate"]
            return df

        store.apply([driver, driver_stats_source, driver_stats_fv, pandas_view])

        entity_rows = [
            {
                "driver_id": 1001,
            }
        ]
        store.write_to_online_store(
            feature_view_name="driver_hourly_stats", df=driver_df
        )
        online_response = store.get_online_features(
            entity_rows=entity_rows,
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",
                "pandas_view:conv_rate_plus_acc",
            ],
            full_feature_names=True,
        ).to_df()
        assert online_response["pandas_view__conv_rate_plus_acc"].equals(
            online_response["driver_hourly_stats__conv_rate"]
            + online_response["driver_hourly_stats__acc_rate"]
        )
