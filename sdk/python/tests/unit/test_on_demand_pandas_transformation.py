import os
import re
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import pytest

from feast import (
    Entity,
    FeatureStore,
    FeatureView,
    FileSource,
    RepoConfig,
    RequestSource,
)
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.field import Field
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import (
    Array,
    Bool,
    Float32,
    Float64,
    Int64,
    String,
)


def test_pandas_transformation():
    with tempfile.TemporaryDirectory() as data_dir:
        store = FeatureStore(
            config=RepoConfig(
                project="test_on_demand_python_transformation",
                registry=os.path.join(data_dir, "registry.db"),
                provider="local",
                entity_key_serialization_version=3,
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
        ).to_df()

        assert online_response["conv_rate_plus_acc"].equals(
            online_response["conv_rate"] + online_response["acc_rate"]
        )


def test_pandas_transformation_returning_all_data_types():
    with tempfile.TemporaryDirectory() as data_dir:
        store = FeatureStore(
            config=RepoConfig(
                project="test_on_demand_python_transformation",
                registry=os.path.join(data_dir, "registry.db"),
                provider="local",
                entity_key_serialization_version=3,
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

        request_source = RequestSource(
            name="request_source",
            schema=[
                Field(name="avg_daily_trip_rank_thresholds", dtype=Array(Int64)),
                Field(name="avg_daily_trip_rank_names", dtype=Array(String)),
            ],
        )

        @on_demand_feature_view(
            sources=[request_source, driver_stats_fv],
            schema=[
                Field(name="highest_achieved_rank", dtype=String),
                Field(name="avg_daily_trips_plus_one", dtype=Int64),
                Field(name="conv_rate_plus_acc", dtype=Float64),
                Field(name="is_highest_rank", dtype=Bool),
                Field(name="achieved_ranks", dtype=Array(String)),
                Field(name="trips_until_next_rank_int", dtype=Array(Int64)),
                Field(name="trips_until_next_rank_float", dtype=Array(Float64)),
                Field(name="achieved_ranks_mask", dtype=Array(Bool)),
            ],
            mode="pandas",
        )
        def pandas_view(inputs: pd.DataFrame) -> pd.DataFrame:
            df = pd.DataFrame()
            df["conv_rate_plus_acc"] = inputs["conv_rate"] + inputs["acc_rate"]
            df["avg_daily_trips_plus_one"] = inputs["avg_daily_trips"] + 1

            df["trips_until_next_rank_int"] = inputs[
                ["avg_daily_trips", "avg_daily_trip_rank_thresholds"]
            ].apply(
                lambda x: [max(threshold - x.iloc[0], 0) for threshold in x.iloc[1]],
                axis=1,
            )
            df["trips_until_next_rank_float"] = df["trips_until_next_rank_int"].map(
                lambda values: [float(value) for value in values]
            )
            df["achieved_ranks_mask"] = df["trips_until_next_rank_int"].map(
                lambda values: [value <= 0 for value in values]
            )

            temp = pd.concat(
                [df[["achieved_ranks_mask"]], inputs[["avg_daily_trip_rank_names"]]],
                axis=1,
            )
            df["achieved_ranks"] = temp.apply(
                lambda x: [
                    rank if achieved else "Locked"
                    for achieved, rank in zip(x.iloc[0], x.iloc[1])
                ],
                axis=1,
            )
            df["highest_achieved_rank"] = (
                df["achieved_ranks"]
                .map(
                    lambda ranks: str(
                        ([rank for rank in ranks if rank != "Locked"][-1:] or ["None"])[
                            0
                        ]
                    )
                )
                .astype("string")
            )
            df["is_highest_rank"] = df["achieved_ranks"].map(
                lambda ranks: ranks[-1] != "Locked"
            )
            return df

        store.apply([driver, driver_stats_source, driver_stats_fv, pandas_view])

        entity_rows = [
            {
                "driver_id": 1001,
                "avg_daily_trip_rank_thresholds": [100, 250, 500, 1000],
                "avg_daily_trip_rank_names": ["Bronze", "Silver", "Gold", "Platinum"],
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
                "pandas_view:avg_daily_trips_plus_one",
                "pandas_view:conv_rate_plus_acc",
                "pandas_view:trips_until_next_rank_int",
                "pandas_view:trips_until_next_rank_float",
                "pandas_view:achieved_ranks_mask",
                "pandas_view:achieved_ranks",
                "pandas_view:highest_achieved_rank",
                "pandas_view:is_highest_rank",
            ],
        ).to_df()
        # We use to_df here to ensure we use the pandas backend, but convert to a dict for comparisons
        result = online_response.to_dict(orient="records")[0]

        # Type assertions
        # Materialized view
        assert type(result["conv_rate"]) == float
        assert type(result["acc_rate"]) == float
        assert type(result["avg_daily_trips"]) == int
        # On-demand view
        assert type(result["avg_daily_trips_plus_one"]) == int
        assert type(result["conv_rate_plus_acc"]) == float
        assert type(result["highest_achieved_rank"]) == str
        assert type(result["is_highest_rank"]) == bool

        assert type(result["trips_until_next_rank_int"]) == list
        assert all([type(e) == int for e in result["trips_until_next_rank_int"]])

        assert type(result["trips_until_next_rank_float"]) == list
        assert all([type(e) == float for e in result["trips_until_next_rank_float"]])

        assert type(result["achieved_ranks"]) == list
        assert all([type(e) == str for e in result["achieved_ranks"]])

        assert type(result["achieved_ranks_mask"]) == list
        assert all([type(e) == bool for e in result["achieved_ranks_mask"]])

        # Value assertions
        expected_trips_until_next_rank = [
            max(threshold - result["avg_daily_trips"], 0)
            for threshold in entity_rows[0]["avg_daily_trip_rank_thresholds"]
        ]
        expected_mask = [value <= 0 for value in expected_trips_until_next_rank]
        expected_ranks = [
            rank if achieved else "Locked"
            for achieved, rank in zip(
                expected_mask, entity_rows[0]["avg_daily_trip_rank_names"]
            )
        ]
        highest_rank = (
            [rank for rank in expected_ranks if rank != "Locked"][-1:] or ["None"]
        )[0]

        assert result["conv_rate_plus_acc"] == result["conv_rate"] + result["acc_rate"]
        assert result["avg_daily_trips_plus_one"] == result["avg_daily_trips"] + 1
        assert result["highest_achieved_rank"] == highest_rank
        assert result["is_highest_rank"] == (expected_ranks[-1] != "Locked")

        assert result["trips_until_next_rank_int"] == expected_trips_until_next_rank
        assert result["trips_until_next_rank_float"] == [
            float(value) for value in expected_trips_until_next_rank
        ]
        assert result["achieved_ranks_mask"] == expected_mask
        assert result["achieved_ranks"] == expected_ranks


def test_invalid_pandas_transformation_raises_type_error_on_apply():
    with tempfile.TemporaryDirectory() as data_dir:
        store = FeatureStore(
            config=RepoConfig(
                project="test_on_demand_python_transformation",
                registry=os.path.join(data_dir, "registry.db"),
                provider="local",
                entity_key_serialization_version=3,
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(data_dir, "online.db")
                ),
            )
        )

        request_source = RequestSource(
            name="request_source",
            schema=[
                Field(name="driver_name", dtype=String),
            ],
        )

        @on_demand_feature_view(
            sources=[request_source],
            schema=[Field(name="driver_name_lower", dtype=String)],
            mode="pandas",
        )
        def pandas_view(inputs: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"driver_name_lower": []})

        with pytest.raises(
            TypeError,
            match=re.escape(
                "Failed to infer type for feature 'driver_name_lower' with value '[]' since no items were returned by the UDF."
            ),
        ):
            store.apply([request_source, pandas_view])
