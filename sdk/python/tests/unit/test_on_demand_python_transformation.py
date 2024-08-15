import os
import re
import tempfile
import unittest
from datetime import datetime, timedelta
from typing import Any

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
from feast.types import Array, Bool, Float32, Float64, Int64, String


class TestOnDemandPythonTransformation(unittest.TestCase):
    def setUp(self):
        with tempfile.TemporaryDirectory() as data_dir:
            self.store = FeatureStore(
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
            driver_df = create_driver_hourly_stats_df(
                driver_entities, start_date, end_date
            )
            driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
            driver_df.to_parquet(
                path=driver_stats_path, allow_truncated_timestamps=True
            )

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
                schema=[Field(name="conv_rate_plus_acc_pandas", dtype=Float64)],
                mode="pandas",
            )
            def pandas_view(inputs: pd.DataFrame) -> pd.DataFrame:
                df = pd.DataFrame()
                df["conv_rate_plus_acc_pandas"] = (
                    inputs["conv_rate"] + inputs["acc_rate"]
                )
                return df

            @on_demand_feature_view(
                sources=[driver_stats_fv[["conv_rate", "acc_rate"]]],
                schema=[Field(name="conv_rate_plus_acc_python", dtype=Float64)],
                mode="python",
            )
            def python_view(inputs: dict[str, Any]) -> dict[str, Any]:
                output: dict[str, Any] = {
                    "conv_rate_plus_acc_python": [
                        conv_rate + acc_rate
                        for conv_rate, acc_rate in zip(
                            inputs["conv_rate"], inputs["acc_rate"]
                        )
                    ]
                }
                return output

            @on_demand_feature_view(
                sources=[driver_stats_fv[["conv_rate", "acc_rate"]]],
                schema=[
                    Field(name="conv_rate_plus_val1_python", dtype=Float64),
                    Field(name="conv_rate_plus_val2_python", dtype=Float64),
                ],
                mode="python",
            )
            def python_demo_view(inputs: dict[str, Any]) -> dict[str, Any]:
                output: dict[str, Any] = {
                    "conv_rate_plus_val1_python": [
                        conv_rate + acc_rate
                        for conv_rate, acc_rate in zip(
                            inputs["conv_rate"], inputs["acc_rate"]
                        )
                    ],
                    "conv_rate_plus_val2_python": [
                        conv_rate + acc_rate
                        for conv_rate, acc_rate in zip(
                            inputs["conv_rate"], inputs["acc_rate"]
                        )
                    ],
                }
                return output

            @on_demand_feature_view(
                sources=[driver_stats_fv[["conv_rate", "acc_rate"]]],
                schema=[
                    Field(name="conv_rate_plus_acc_python_singleton", dtype=Float64)
                ],
                mode="python",
            )
            def python_singleton_view(inputs: dict[str, Any]) -> dict[str, Any]:
                output: dict[str, Any] = dict(conv_rate_plus_acc_python=float("-inf"))
                output["conv_rate_plus_acc_python_singleton"] = (
                    inputs["conv_rate"] + inputs["acc_rate"]
                )
                return output

            with pytest.raises(TypeError):
                # Note the singleton view will fail as the type is
                # expected to be a list which can be confirmed in _infer_features_dict
                self.store.apply(
                    [
                        driver,
                        driver_stats_source,
                        driver_stats_fv,
                        pandas_view,
                        python_view,
                        python_singleton_view,
                    ]
                )

            self.store.apply(
                [
                    driver,
                    driver_stats_source,
                    driver_stats_fv,
                    pandas_view,
                    python_view,
                    python_demo_view,
                ]
            )
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", df=driver_df
            )
            assert len(self.store.list_all_feature_views()) == 4
            assert len(self.store.list_feature_views()) == 1
            assert len(self.store.list_on_demand_feature_views()) == 3
            assert len(self.store.list_stream_feature_views()) == 0

    def test_python_pandas_parity(self):
        entity_rows = [
            {
                "driver_id": 1001,
            }
        ]

        online_python_response = self.store.get_online_features(
            entity_rows=entity_rows,
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "python_view:conv_rate_plus_acc_python",
            ],
        ).to_dict()

        online_pandas_response = self.store.get_online_features(
            entity_rows=entity_rows,
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "pandas_view:conv_rate_plus_acc_pandas",
            ],
        ).to_df()

        assert len(online_python_response) == 4
        assert all(
            key in online_python_response.keys()
            for key in [
                "driver_id",
                "acc_rate",
                "conv_rate",
                "conv_rate_plus_acc_python",
            ]
        )
        assert len(online_python_response["conv_rate_plus_acc_python"]) == 1
        assert (
            online_python_response["conv_rate_plus_acc_python"][0]
            == online_pandas_response["conv_rate_plus_acc_pandas"][0]
            == online_python_response["conv_rate"][0]
            + online_python_response["acc_rate"][0]
        )

    def test_python_docs_demo(self):
        entity_rows = [
            {
                "driver_id": 1001,
            }
        ]

        online_python_response = self.store.get_online_features(
            entity_rows=entity_rows,
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "python_demo_view:conv_rate_plus_val1_python",
                "python_demo_view:conv_rate_plus_val2_python",
            ],
        ).to_dict()

        assert sorted(list(online_python_response.keys())) == sorted(
            [
                "driver_id",
                "acc_rate",
                "conv_rate",
                "conv_rate_plus_val1_python",
                "conv_rate_plus_val2_python",
            ]
        )

        assert (
            online_python_response["conv_rate_plus_val1_python"][0]
            == online_python_response["conv_rate_plus_val2_python"][0]
        )
        assert (
            online_python_response["conv_rate"][0]
            + online_python_response["acc_rate"][0]
            == online_python_response["conv_rate_plus_val1_python"][0]
        )
        assert (
            online_python_response["conv_rate"][0]
            + online_python_response["acc_rate"][0]
            == online_python_response["conv_rate_plus_val2_python"][0]
        )


class TestOnDemandPythonTransformationAllDataTypes(unittest.TestCase):
    def setUp(self):
        with tempfile.TemporaryDirectory() as data_dir:
            self.store = FeatureStore(
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
            driver_df = create_driver_hourly_stats_df(
                driver_entities, start_date, end_date
            )
            driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
            driver_df.to_parquet(
                path=driver_stats_path, allow_truncated_timestamps=True
            )

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
                mode="python",
            )
            def python_view(inputs: dict[str, Any]) -> dict[str, Any]:
                output = {}
                trips_until_next_rank = [
                    [max(threshold - row[1], 0) for threshold in row[0]]
                    for row in zip(
                        inputs["avg_daily_trip_rank_thresholds"],
                        inputs["avg_daily_trips"],
                    )
                ]
                mask = [[value <= 0 for value in row] for row in trips_until_next_rank]
                ranks = [
                    [rank if mask else "Locked" for mask, rank in zip(*row)]
                    for row in zip(mask, inputs["avg_daily_trip_rank_names"])
                ]
                highest_rank = [
                    ([rank for rank in row if rank != "Locked"][-1:] or ["None"])[0]
                    for row in ranks
                ]

                output["conv_rate_plus_acc"] = [
                    sum(row) for row in zip(inputs["conv_rate"], inputs["acc_rate"])
                ]
                output["avg_daily_trips_plus_one"] = [
                    row + 1 for row in inputs["avg_daily_trips"]
                ]
                output["highest_achieved_rank"] = highest_rank
                output["is_highest_rank"] = [row[-1] != "Locked" for row in ranks]

                output["trips_until_next_rank_int"] = trips_until_next_rank
                output["trips_until_next_rank_float"] = [
                    [float(value) for value in row] for row in trips_until_next_rank
                ]
                output["achieved_ranks_mask"] = mask
                output["achieved_ranks"] = ranks
                return output

            self.store.apply(
                [driver, driver_stats_source, driver_stats_fv, python_view]
            )
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", df=driver_df
            )

    def test_python_transformation_returning_all_data_types(self):
        entity_rows = [
            {
                "driver_id": 1001,
                "avg_daily_trip_rank_thresholds": [100, 250, 500, 1000],
                "avg_daily_trip_rank_names": ["Bronze", "Silver", "Gold", "Platinum"],
            }
        ]
        online_response = self.store.get_online_features(
            entity_rows=entity_rows,
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",
                "python_view:avg_daily_trips_plus_one",
                "python_view:conv_rate_plus_acc",
                "python_view:trips_until_next_rank_int",
                "python_view:trips_until_next_rank_float",
                "python_view:achieved_ranks_mask",
                "python_view:achieved_ranks",
                "python_view:highest_achieved_rank",
                "python_view:is_highest_rank",
            ],
        ).to_dict()
        result = {name: value[0] for name, value in online_response.items()}

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


def test_invalid_python_transformation_raises_type_error_on_apply():
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

        request_source = RequestSource(
            name="request_source",
            schema=[
                Field(name="driver_name", dtype=String),
            ],
        )

        @on_demand_feature_view(
            sources=[request_source],
            schema=[Field(name="driver_name_lower", dtype=String)],
            mode="python",
        )
        def python_view(inputs: dict[str, Any]) -> dict[str, Any]:
            return {"driver_name_lower": []}

        with pytest.raises(
            TypeError,
            match=re.escape(
                "Failed to infer type for feature 'driver_name_lower' with value '[]' since no items were returned by the UDF."
            ),
        ):
            store.apply([request_source, python_view])
