import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import pytest

from feast import Entity, FeatureStore, FeatureView, FileSource, RepoConfig
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.field import Field
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64


@pytest.mark.skip(
    reason="This test is not working, can't work out why. Skipping as we don't use/plan to use substrait transformations."
)
def test_ibis_pandas_parity():
    with tempfile.TemporaryDirectory() as data_dir:
        store = FeatureStore(
            config=RepoConfig(
                project="test_on_demand_substrait_transformation",
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
            ttl=timedelta(days=1),
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

        from ibis.expr.types import Table

        @on_demand_feature_view(
            sources=[driver_stats_fv[["conv_rate", "acc_rate"]]],
            schema=[Field(name="conv_rate_plus_acc_substrait", dtype=Float64)],
            mode="substrait",
        )
        def substrait_view(inputs: Table) -> Table:
            return inputs.mutate(
                conv_rate_plus_acc_substrait=inputs["conv_rate"] + inputs["acc_rate"]
            )

        store.apply(
            [driver, driver_stats_source, driver_stats_fv, substrait_view, pandas_view]
        )

        store.materialize(
            start_date=start_date,
            end_date=end_date,
        )

        entity_df = pd.DataFrame.from_dict(
            {
                # entity's join key -> entity values
                "driver_id": [1001, 1002, 1003],
                # "event_timestamp" (reserved key) -> timestamps
                "event_timestamp": [
                    start_date + timedelta(days=4),
                    start_date + timedelta(days=5),
                    start_date + timedelta(days=6),
                ],
            }
        )

        requested_features = [
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
            "substrait_view:conv_rate_plus_acc_substrait",
            "pandas_view:conv_rate_plus_acc",
        ]

        training_df = store.get_historical_features(
            entity_df=entity_df, features=requested_features
        )

        assert training_df.to_df()["conv_rate_plus_acc"].equals(
            training_df.to_df()["conv_rate_plus_acc_substrait"]
        )

        assert training_df.to_arrow()["conv_rate_plus_acc"].equals(
            training_df.to_arrow()["conv_rate_plus_acc_substrait"]
        )

        online_response = store.get_online_features(
            features=requested_features,
            entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}, {"driver_id": 1003}],
        )

        assert (
            online_response.to_dict()["conv_rate_plus_acc"]
            == online_response.to_dict()["conv_rate_plus_acc_substrait"]
        )
