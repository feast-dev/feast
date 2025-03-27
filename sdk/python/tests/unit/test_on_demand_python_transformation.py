import os
import platform
import re
import sqlite3
import sys
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
from feast.feature_view import DUMMY_ENTITY_FIELD
from feast.field import Field
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.nlp_test_data import create_document_chunks_df
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import (
    Array,
    Bool,
    Bytes,
    Float32,
    Float64,
    Int64,
    PdfBytes,
    String,
    UnixTimestamp,
    ValueType,
    _utc_now,
    from_value_type,
)

MAC_VER = platform.mac_ver()[0].split(".")[0] if platform.mac_ver() else ""


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

            driver = Entity(
                name="driver", join_keys=["driver_id"], value_type=ValueType.INT64
            )

            driver_stats_source = FileSource(
                name="driver_hourly_stats_source",
                path=driver_stats_path,
                timestamp_field="event_timestamp",
                created_timestamp_column="created",
            )
            input_request_source = RequestSource(
                name="counter_source",
                schema=[
                    Field(name="counter", dtype=Int64),
                    Field(name="input_datetime", dtype=UnixTimestamp),
                ],
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

            driver_stats_entity_less_fv = FeatureView(
                name="driver_hourly_stats_no_entity",
                entities=[],
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
                    "conv_rate_plus_acc_python": conv_rate + acc_rate
                    for conv_rate, acc_rate in zip(
                        inputs["conv_rate"], inputs["acc_rate"]
                    )
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
                    Field(name="conv_rate_plus_acc_python_singleton", dtype=Float64),
                    Field(
                        name="conv_rate_plus_acc_python_singleton_array",
                        dtype=Array(Float64),
                    ),
                ],
                mode="python",
                singleton=True,
            )
            def python_singleton_view(inputs: dict[str, Any]) -> dict[str, Any]:
                output: dict[str, Any] = dict(conv_rate_plus_acc_python=float("-inf"))
                output["conv_rate_plus_acc_python_singleton"] = (
                    inputs["conv_rate"] + inputs["acc_rate"]
                )
                output["conv_rate_plus_acc_python_singleton_array"] = [0.1, 0.2, 0.3]
                return output

            @on_demand_feature_view(
                sources=[
                    driver_stats_fv[["conv_rate", "acc_rate"]],
                    input_request_source,
                ],
                schema=[
                    Field(name="conv_rate_plus_acc", dtype=Float64),
                    Field(name="current_datetime", dtype=UnixTimestamp),
                    Field(name="counter", dtype=Int64),
                    Field(name="input_datetime", dtype=UnixTimestamp),
                ],
                mode="python",
                write_to_online_store=True,
            )
            def python_stored_writes_feature_view(
                inputs: dict[str, Any],
            ) -> dict[str, Any]:
                output: dict[str, Any] = {
                    "conv_rate_plus_acc": [
                        conv_rate + acc_rate
                        for conv_rate, acc_rate in zip(
                            inputs["conv_rate"], inputs["acc_rate"]
                        )
                    ],
                    "current_datetime": [datetime.now() for _ in inputs["conv_rate"]],
                    "counter": [c + 1 for c in inputs["counter"]],
                    "input_datetime": [d for d in inputs["input_datetime"]],
                }
                return output

            self.store.apply(
                [
                    driver,
                    driver_stats_source,
                    driver_stats_fv,
                    pandas_view,
                    python_view,
                    python_singleton_view,
                    python_demo_view,
                    driver_stats_entity_less_fv,
                    python_stored_writes_feature_view,
                ]
            )
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", df=driver_df
            )
            assert driver_stats_fv.entity_columns == [
                Field(name=driver.join_key, dtype=from_value_type(driver.value_type))
            ]
            assert driver_stats_entity_less_fv.entity_columns == [DUMMY_ENTITY_FIELD]

            assert len(self.store.list_all_feature_views()) == 7
            assert len(self.store.list_feature_views()) == 2
            assert len(self.store.list_on_demand_feature_views()) == 5
            assert len(self.store.list_stream_feature_views()) == 0

    def test_setup(self):
        pass

    def test_python_singleton_view(self):
        entity_rows = [
            {
                "driver_id": 1001,
                "acc_rate": 0.25,
                "conv_rate": 0.25,
            }
        ]

        online_python_response = self.store.get_online_features(
            entity_rows=entity_rows,
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "python_singleton_view:conv_rate_plus_acc_python_singleton",
            ],
        ).to_dict()

        assert sorted(list(online_python_response.keys())) == sorted(
            [
                "driver_id",
                "acc_rate",
                "conv_rate",
                "conv_rate_plus_acc_python_singleton",
            ]
        )

        assert online_python_response["conv_rate_plus_acc_python_singleton"][0] == (
            online_python_response["conv_rate"][0]
            + online_python_response["acc_rate"][0]
        )

    def test_python_pandas_parity(self):
        entity_rows = [
            {
                "driver_id": 1001,
                "counter": 0,
                "input_datetime": _utc_now(),
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

    def test_stored_writes(self):
        # Note that here we shouldn't have to pass the request source features for reading
        # because they should have already been written to the online store
        current_datetime = _utc_now()
        entity_rows_to_read = [
            {
                "driver_id": 1001,
                "counter": 0,
                "input_datetime": current_datetime,
            }
        ]

        online_python_response = self.store.get_online_features(
            entity_rows=entity_rows_to_read,
            features=[
                "python_stored_writes_feature_view:conv_rate_plus_acc",
                "python_stored_writes_feature_view:current_datetime",
                "python_stored_writes_feature_view:counter",
                "python_stored_writes_feature_view:input_datetime",
            ],
        ).to_dict()

        assert sorted(list(online_python_response.keys())) == sorted(
            [
                "driver_id",
                "conv_rate_plus_acc",
                "counter",
                "current_datetime",
                "input_datetime",
            ]
        )
        print(online_python_response)
        # Now this is where we need to test the stored writes, this should return the same output as the previous


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
            assert driver_stats_fv.entities == [driver.name]
            assert driver_stats_fv.entity_columns == []

            request_source = RequestSource(
                name="request_source",
                schema=[
                    Field(name="avg_daily_trip_rank_thresholds", dtype=Array(Int64)),
                    Field(name="avg_daily_trip_rank_names", dtype=Array(String)),
                ],
            )
            input_request = RequestSource(
                name="vals_to_add",
                schema=[
                    Field(name="val_to_add", dtype=Int64),
                    Field(name="val_to_add_2", dtype=Int64),
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

            @on_demand_feature_view(
                sources=[
                    driver_stats_fv,
                    input_request,
                ],
                schema=[
                    Field(name="conv_rate_plus_val1", dtype=Float64),
                    Field(name="conv_rate_plus_val2", dtype=Float64),
                ],
                mode="pandas",
            )
            def pandas_view(features_df: pd.DataFrame) -> pd.DataFrame:
                df = pd.DataFrame()
                df["conv_rate_plus_val1"] = (
                    features_df["conv_rate"] + features_df["val_to_add"]
                )
                df["conv_rate_plus_val2"] = (
                    features_df["conv_rate"] + features_df["val_to_add_2"]
                )
                return df

            self.store.apply(
                [
                    driver,
                    driver_stats_source,
                    driver_stats_fv,
                    python_view,
                    pandas_view,
                    input_request,
                    request_source,
                ]
            )
            fv_applied = self.store.get_feature_view("driver_hourly_stats")
            assert fv_applied.entities == [driver.name]
            # Note here that after apply() is called, the entity_columns are populated with the join_key
            assert fv_applied.entity_columns[0].name == driver.join_key

            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", df=driver_df
            )

            batch_sample = pd.DataFrame(driver_entities, columns=["driver_id"])
            batch_sample["val_to_add"] = 0
            batch_sample["val_to_add_2"] = 1
            batch_sample["event_timestamp"] = start_date
            batch_sample["created"] = start_date
            fv_only_cols = ["driver_id", "event_timestamp", "created"]

            resp_base_fv = self.store.get_historical_features(
                entity_df=batch_sample[fv_only_cols],
                features=[
                    "driver_hourly_stats:conv_rate",
                    "driver_hourly_stats:acc_rate",
                    "driver_hourly_stats:avg_daily_trips",
                ],
            ).to_df()
            assert resp_base_fv is not None
            assert sorted(resp_base_fv.columns) == [
                "acc_rate",
                "avg_daily_trips",
                "conv_rate",
                "created__",
                "driver_id",
                "event_timestamp",
            ]
            resp = self.store.get_historical_features(
                entity_df=batch_sample,
                features=[
                    "driver_hourly_stats:conv_rate",
                    "driver_hourly_stats:acc_rate",
                    "driver_hourly_stats:avg_daily_trips",
                    "pandas_view:conv_rate_plus_val1",
                    "pandas_view:conv_rate_plus_val2",
                ],
            ).to_df()
            assert resp is not None
            assert resp["conv_rate_plus_val1"].isnull().sum() == 0

            batch_sample["avg_daily_trip_rank_thresholds"] = [
                [100, 250, 500, 1000]
            ] * batch_sample.shape[0]
            batch_sample["avg_daily_trip_rank_names"] = [
                ["Bronze", "Silver", "Gold", "Platinum"]
            ] * batch_sample.shape[0]
            resp_python = self.store.get_historical_features(
                entity_df=batch_sample,
                features=[
                    "driver_hourly_stats:conv_rate",
                    "driver_hourly_stats:acc_rate",
                    "driver_hourly_stats:avg_daily_trips",
                    "python_view:conv_rate_plus_acc",
                ],
            ).to_df()
            assert resp_python is not None
            assert resp_python["conv_rate_plus_acc"].isnull().sum() == 0

            # Now testing feature retrieval for driver ids not in the dataset
            missing_batch_sample = pd.DataFrame([1234567890], columns=["driver_id"])
            missing_batch_sample["val_to_add"] = 0
            missing_batch_sample["val_to_add_2"] = 1
            missing_batch_sample["event_timestamp"] = start_date
            missing_batch_sample["created"] = start_date
            resp_offline = self.store.get_historical_features(
                entity_df=missing_batch_sample,
                features=[
                    "driver_hourly_stats:conv_rate",
                    "driver_hourly_stats:acc_rate",
                    "driver_hourly_stats:avg_daily_trips",
                    "pandas_view:conv_rate_plus_val1",
                    "pandas_view:conv_rate_plus_val2",
                ],
            ).to_df()
            assert resp_offline is not None
            assert resp_offline["conv_rate_plus_val1"].isnull().sum() == 1
            assert sorted(resp_offline.columns) == [
                "acc_rate",
                "avg_daily_trips",
                "conv_rate",
                "conv_rate_plus_val1",
                "conv_rate_plus_val2",
                "created__",
                "driver_id",
                "event_timestamp",
                "val_to_add",
                "val_to_add_2",
            ]
            resp_online_missing_entity = self.store.get_online_features(
                entity_rows=[
                    {"driver_id": 1234567890, "val_to_add": 0, "val_to_add_2": 1}
                ],
                features=[
                    "driver_hourly_stats:conv_rate",
                    "driver_hourly_stats:acc_rate",
                    "driver_hourly_stats:avg_daily_trips",
                    "pandas_view:conv_rate_plus_val1",
                    "pandas_view:conv_rate_plus_val2",
                ],
            )
            assert resp_online_missing_entity is not None
            resp_online = self.store.get_online_features(
                entity_rows=[{"driver_id": 1001, "val_to_add": 0, "val_to_add_2": 1}],
                features=[
                    "driver_hourly_stats:conv_rate",
                    "driver_hourly_stats:acc_rate",
                    "driver_hourly_stats:avg_daily_trips",
                    "pandas_view:conv_rate_plus_val1",
                    "pandas_view:conv_rate_plus_val2",
                ],
            ).to_df()
            assert resp_online is not None
            assert sorted(resp_online.columns) == [
                "acc_rate",
                "avg_daily_trips",
                "conv_rate",
                "conv_rate_plus_val1",
                "conv_rate_plus_val2",
                "driver_id",
                # It does not have the items below
                # "created__",
                # "event_timestamp",
                # "val_to_add",
                # "val_to_add_2",
            ]
            # Note online and offline columns will not match because:
            # you want to be space efficient online when considering the impact of network latency so you want to send
            # and receive the minimally required set of data, which means after transformation you only need to send the
            # output in the response.
            # Offline, you will probably prioritize reproducibility and being able to iterate, which means you will want
            # the underlying inputs into your transformation, so the extra data is tolerable.
            assert sorted(resp_online.columns) != sorted(resp_offline.columns)

    def test_setup(self):
        pass

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


@pytest.mark.skipif(
    not (
        sys.version_info[0:2] in [(3, 10), (3, 11)]
        and platform.system() == "Darwin"
        and MAC_VER != "14"
    ),
    reason="Only works on Python 3.10 and 3.11 on macOS",
)
class TestOnDemandTransformationsWithWrites(unittest.TestCase):
    def test_stored_writes(self):
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
            input_request_source = RequestSource(
                name="counter_source",
                schema=[
                    Field(name="counter", dtype=Int64),
                    Field(name="input_datetime", dtype=UnixTimestamp),
                ],
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
            assert driver_stats_fv.entities == [driver.name]
            assert driver_stats_fv.entity_columns == []

            ODFV_STRING_CONSTANT = "guaranteed constant"
            ODFV_OTHER_STRING_CONSTANT = "somethign else"

            @on_demand_feature_view(
                entities=[driver],
                sources=[
                    driver_stats_fv[["conv_rate", "acc_rate"]],
                    input_request_source,
                ],
                schema=[
                    Field(name="conv_rate_plus_acc", dtype=Float64),
                    Field(name="current_datetime", dtype=UnixTimestamp),
                    Field(name="counter", dtype=Int64),
                    Field(name="input_datetime", dtype=UnixTimestamp),
                    Field(name="string_constant", dtype=String),
                ],
                mode="python",
                write_to_online_store=True,
            )
            def python_stored_writes_feature_view(
                inputs: dict[str, Any],
            ) -> dict[str, Any]:
                output: dict[str, Any] = {
                    "conv_rate_plus_acc": [
                        conv_rate + acc_rate
                        for conv_rate, acc_rate in zip(
                            inputs["conv_rate"], inputs["acc_rate"]
                        )
                    ],
                    "current_datetime": [datetime.now() for _ in inputs["conv_rate"]],
                    "counter": [c + 1 for c in inputs["counter"]],
                    "input_datetime": [d for d in inputs["input_datetime"]],
                    "string_constant": [ODFV_STRING_CONSTANT],
                }
                return output

            assert python_stored_writes_feature_view.entities == [driver.name]
            assert python_stored_writes_feature_view.entity_columns == []

            self.store.apply(
                [
                    driver,
                    driver_stats_source,
                    driver_stats_fv,
                    python_stored_writes_feature_view,
                ]
            )
            fv_applied = self.store.get_feature_view("driver_hourly_stats")
            odfv_applied = self.store.get_on_demand_feature_view(
                "python_stored_writes_feature_view"
            )

            assert fv_applied.entities == [driver.name]
            assert odfv_applied.entities == [driver.name]

            # Note here that after apply() is called, the entity_columns are populated with the join_key
            # assert fv_applied.entity_columns[0].name == driver.join_key
            assert fv_applied.entity_columns[0].name == driver.join_key
            assert odfv_applied.entity_columns[0].name == driver.join_key

            assert len(self.store.list_all_feature_views()) == 2
            assert len(self.store.list_feature_views()) == 1
            assert len(self.store.list_on_demand_feature_views()) == 1
            assert len(self.store.list_stream_feature_views()) == 0
            assert (
                driver_stats_fv.entity_columns
                == self.store.get_feature_view("driver_hourly_stats").entity_columns
            )
            assert (
                python_stored_writes_feature_view.entity_columns
                == self.store.get_on_demand_feature_view(
                    "python_stored_writes_feature_view"
                ).entity_columns
            )

            current_datetime = _utc_now()
            fv_entity_rows_to_write = [
                {
                    "driver_id": 1001,
                    "conv_rate": 0.25,
                    "acc_rate": 0.25,
                    "avg_daily_trips": 2,
                    "event_timestamp": current_datetime,
                    "created": current_datetime,
                }
            ]
            fv_entity_rows_to_read = [
                {
                    "driver_id": 1001,
                }
            ]
            print("")
            print("storing FV features")
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats",
                df=fv_entity_rows_to_write,
            )
            print("reading fv features")
            online_python_response = self.store.get_online_features(
                entity_rows=fv_entity_rows_to_read,
                features=[
                    "driver_hourly_stats:conv_rate",
                    "driver_hourly_stats:acc_rate",
                    "driver_hourly_stats:avg_daily_trips",
                ],
            ).to_dict()

            assert online_python_response == {
                "driver_id": [1001],
                "conv_rate": [0.25],
                "avg_daily_trips": [2],
                "acc_rate": [0.25],
            }

            # Note that here we shouldn't have to pass the request source features for reading
            # because they should have already been written to the online store
            odfv_entity_rows_to_write = [
                {
                    "driver_id": 1002,
                    "counter": 0,
                    "conv_rate": 0.25,
                    "acc_rate": 0.50,
                    "input_datetime": current_datetime,
                    "string_constant": ODFV_OTHER_STRING_CONSTANT,
                }
            ]
            odfv_entity_rows_to_read = [
                {
                    "driver_id": 1002,
                    "conv_rate_plus_acc": 7,  # note how this is not the correct value and would be calculate on demand
                    "conv_rate": 0.25,
                    "acc_rate": 0.50,
                    "counter": 0,
                    "input_datetime": current_datetime,
                    "string_constant": ODFV_STRING_CONSTANT,
                }
            ]
            print("storing ODFV features")
            self.store.write_to_online_store(
                feature_view_name="python_stored_writes_feature_view",
                df=odfv_entity_rows_to_write,
            )
            _conn = sqlite3.connect(self.store.config.online_store.path)
            _table_name = (
                self.store.project
                + "_"
                + self.store.get_on_demand_feature_view(
                    "python_stored_writes_feature_view"
                ).name
            )
            sample = pd.read_sql(
                f"""
            select
                feature_name,
                value
            from {_table_name}
            """,
                _conn,
            )
            assert (
                sample[sample["feature_name"] == "string_constant"]["value"]
                .astype(str)
                .str.contains("guaranteed constant")
                .values[0]
            )

            print("reading odfv features")
            online_odfv_python_response = self.store.get_online_features(
                entity_rows=odfv_entity_rows_to_read,
                features=[
                    "python_stored_writes_feature_view:conv_rate_plus_acc",
                    "python_stored_writes_feature_view:current_datetime",
                    "python_stored_writes_feature_view:counter",
                    "python_stored_writes_feature_view:input_datetime",
                    "python_stored_writes_feature_view:string_constant",
                ],
            ).to_dict()
            print(online_odfv_python_response)
            assert sorted(list(online_odfv_python_response.keys())) == sorted(
                [
                    "driver_id",
                    "conv_rate_plus_acc",
                    "counter",
                    "current_datetime",
                    "input_datetime",
                    "string_constant",
                ]
            )
            # This should be 1 because we write the value of 0 and during the write, the counter is incremented
            assert online_odfv_python_response["counter"] == [1]
            assert online_odfv_python_response["string_constant"] == [
                ODFV_STRING_CONSTANT
            ]
            assert online_odfv_python_response["string_constant"] != [
                ODFV_OTHER_STRING_CONSTANT
            ]

    def test_stored_writes_with_explode(self):
        with tempfile.TemporaryDirectory() as data_dir:
            self.store = FeatureStore(
                config=RepoConfig(
                    project="test_on_demand_python_transformation_explode",
                    registry=os.path.join(data_dir, "registry.db"),
                    provider="local",
                    entity_key_serialization_version=3,
                    online_store=SqliteOnlineStoreConfig(
                        path=os.path.join(data_dir, "online.db"),
                        vector_enabled=True,
                        vector_len=5,
                    ),
                )
            )

            documents = {
                "doc_1": "Hello world. How are you?",
                "doc_2": "This is a test. Document chunking example.",
            }
            start_date = datetime.now() - timedelta(days=15)
            end_date = datetime.now()

            documents_df = create_document_chunks_df(
                documents,
                start_date,
                end_date,
                embedding_size=60,
            )
            corpus_path = os.path.join(data_dir, "documents.parquet")
            documents_df.to_parquet(path=corpus_path, allow_truncated_timestamps=True)

            chunk = Entity(
                name="chunk", join_keys=["chunk_id"], value_type=ValueType.STRING
            )
            document = Entity(
                name="document", join_keys=["document_id"], value_type=ValueType.STRING
            )

            input_explode_request_source = RequestSource(
                name="counter_source",
                schema=[
                    Field(name="document_id", dtype=String),
                    Field(name="document_text", dtype=String),
                    Field(name="document_bytes", dtype=Bytes),
                ],
            )

            @on_demand_feature_view(
                entities=[chunk, document],
                sources=[
                    input_explode_request_source,
                ],
                schema=[
                    Field(name="document_id", dtype=String),
                    Field(name="chunk_id", dtype=String),
                    Field(name="chunk_text", dtype=String),
                    Field(
                        name="vector",
                        dtype=Array(Float32),
                        vector_index=True,
                        vector_search_metric="L2",
                    ),
                ],
                mode="python",
                write_to_online_store=True,
            )
            def python_stored_writes_feature_view_explode_singleton(
                inputs: dict[str, Any],
            ):
                output: dict[str, Any] = {
                    "document_id": ["doc_1", "doc_1", "doc_2", "doc_2"],
                    "chunk_id": ["chunk-1", "chunk-2", "chunk-1", "chunk-2"],
                    "chunk_text": [
                        "hello friends",
                        "how are you?",
                        "This is a test.",
                        "Document chunking example.",
                    ],
                    "vector": [
                        [0.1] * 5,
                        [0.2] * 5,
                        [0.3] * 5,
                        [0.4] * 5,
                    ],
                }
                return output

            assert python_stored_writes_feature_view_explode_singleton.entities == [
                chunk.name,
                document.name,
            ]
            assert (
                python_stored_writes_feature_view_explode_singleton.entity_columns[
                    0
                ].name
                == document.join_key
            )
            assert (
                python_stored_writes_feature_view_explode_singleton.entity_columns[
                    1
                ].name
                == chunk.join_key
            )

            self.store.apply(
                [
                    chunk,
                    document,
                    input_explode_request_source,
                    python_stored_writes_feature_view_explode_singleton,
                ]
            )
            odfv_applied = self.store.get_on_demand_feature_view(
                "python_stored_writes_feature_view_explode_singleton"
            )

            assert odfv_applied.features[1].vector_index

            assert odfv_applied.entities == [chunk.name, document.name]

            # Note here that after apply() is called, the entity_columns are populated with the join_key
            assert odfv_applied.entity_columns[1].name == chunk.join_key
            assert odfv_applied.entity_columns[0].name == document.join_key

            assert len(self.store.list_all_feature_views()) == 1
            assert len(self.store.list_feature_views()) == 0
            assert len(self.store.list_on_demand_feature_views()) == 1
            assert len(self.store.list_stream_feature_views()) == 0
            assert (
                python_stored_writes_feature_view_explode_singleton.entity_columns
                == self.store.get_on_demand_feature_view(
                    "python_stored_writes_feature_view_explode_singleton"
                ).entity_columns
            )

            odfv_entity_rows_to_write = [
                {
                    "document_id": "document_1",
                    "document_text": "Hello world. How are you?",
                },
                {
                    "document_id": "document_2",
                    "document_text": "This is a test. Document chunking example.",
                },
            ]
            fv_entity_rows_to_read = [
                {
                    "document_id": "doc_1",
                    "chunk_id": "chunk-2",
                },
                {
                    "document_id": "doc_2",
                    "chunk_id": "chunk-1",
                },
            ]

            self.store.write_to_online_store(
                feature_view_name="python_stored_writes_feature_view_explode_singleton",
                df=odfv_entity_rows_to_write,
            )
            _table_name = (
                self.store.project
                + "_"
                + self.store.get_on_demand_feature_view(
                    "python_stored_writes_feature_view_explode_singleton"
                ).name
            )
            _conn = sqlite3.connect(self.store.config.online_store.path)
            sample = pd.read_sql(
                f"""
            select
                entity_key,
                feature_name,
                value
            from {_table_name}
            """,
                _conn,
            )
            print(f"\nsample from {_table_name}:\n{sample}")

            # verifying we retrieve doc_1 chunk-2
            filt = (sample["feature_name"] == "chunk_text") & (
                sample["value"]
                .apply(lambda x: x.decode("latin1"))
                .str.contains("how are")
            )
            assert (
                sample[filt]["entity_key"].astype(str).str.contains("doc_1")
                & sample[filt]["entity_key"].astype(str).str.contains("chunk-2")
            ).values[0]

            print("reading fv features")
            online_python_response = self.store.get_online_features(
                entity_rows=fv_entity_rows_to_read,
                features=[
                    "python_stored_writes_feature_view_explode_singleton:document_id",
                    "python_stored_writes_feature_view_explode_singleton:chunk_id",
                    "python_stored_writes_feature_view_explode_singleton:chunk_text",
                ],
            ).to_dict()
            assert sorted(list(online_python_response.keys())) == sorted(
                [
                    "chunk_id",
                    "chunk_text",
                    "document_id",
                ]
            )
            assert online_python_response == {
                "document_id": ["doc_1", "doc_2"],
                "chunk_id": ["chunk-2", "chunk-1"],
                "chunk_text": ["how are you?", "This is a test."],
            }

            if sys.version_info[0:2] == (3, 10):
                query_embedding = [0.05] * 5
                online_python_vec_response = self.store.retrieve_online_documents_v2(
                    features=[
                        "python_stored_writes_feature_view_explode_singleton:document_id",
                        "python_stored_writes_feature_view_explode_singleton:chunk_id",
                        "python_stored_writes_feature_view_explode_singleton:chunk_text",
                    ],
                    query=query_embedding,
                    top_k=2,
                ).to_dict()

                assert online_python_vec_response is not None
                assert online_python_vec_response == {
                    "document_id": ["doc_1", "doc_1"],
                    "chunk_id": ["chunk-1", "chunk-2"],
                    "chunk_text": ["hello friends", "how are you?"],
                    "distance": [0.11180340498685837, 0.3354102075099945],
                }

    def test_docling_transform(self):
        import io

        from docling.chunking import HybridChunker
        from docling.datamodel.base_models import DocumentStream
        from docling.document_converter import DocumentConverter
        from transformers import AutoTokenizer

        EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
        VECTOR_LEN = 10
        MAX_TOKENS = 64  # Small token limit for demonstration
        tokenizer = AutoTokenizer.from_pretrained(EMBED_MODEL_ID)
        chunker = HybridChunker(
            tokenizer=tokenizer, max_tokens=MAX_TOKENS, merge_peers=True
        )

        with tempfile.TemporaryDirectory() as data_dir:
            self.store = FeatureStore(
                config=RepoConfig(
                    project="test_on_demand_python_transformation_explode",
                    registry=os.path.join(data_dir, "registry.db"),
                    provider="local",
                    entity_key_serialization_version=3,
                    online_store=SqliteOnlineStoreConfig(
                        path=os.path.join(data_dir, "online.db"),
                        vector_enabled=True,
                        vector_len=VECTOR_LEN,
                    ),
                )
            )

            chunk = Entity(
                name="chunk_id",
                description="Chunk ID",
                value_type=ValueType.STRING,
                join_keys=["chunk_id"],
            )

            document = Entity(
                name="document_id",
                description="Document ID",
                value_type=ValueType.STRING,
                join_keys=["document_id"],
            )

            def embed_chunk(input_string) -> dict[str, list[float]]:
                output = {"query_embedding": [0.5] * VECTOR_LEN}
                return output

            input_request_pdf = RequestSource(
                name="pdf_request_source",
                schema=[
                    Field(name="pdf_bytes", dtype=PdfBytes),
                    Field(name="file_name", dtype=String),
                    Field(name="document_id", dtype=String),
                ],
            )
            self.store.apply([chunk, document, input_request_pdf])

            @on_demand_feature_view(
                entities=[chunk, document],
                sources=[input_request_pdf],
                schema=[
                    Field(name="document_id", dtype=String),
                    Field(name="chunk_id", dtype=String),
                    Field(name="chunk_text", dtype=String),
                    Field(
                        name="vector",
                        dtype=Array(Float32),
                        vector_index=True,
                        vector_search_metric="L2",
                    ),
                ],
                mode="python",
                write_to_online_store=True,
                singleton=True,
            )
            def docling_transform_docs(inputs: dict[str, Any]):
                document_ids, chunks, embeddings, chunk_ids = [], [], [], []
                buf = io.BytesIO(
                    inputs["pdf_bytes"],
                )
                source = DocumentStream(name=inputs["file_name"], stream=buf)
                converter = DocumentConverter()
                result = converter.convert(source)
                for i, chunk in enumerate(chunker.chunk(dl_doc=result.document)):
                    raw_chunk = chunker.serialize(chunk=chunk)
                    embedding = embed_chunk(raw_chunk).get("query_embedding", [])
                    chunk_id = f"chunk-{i}"
                    document_ids.append(inputs["document_id"])
                    chunks.append(raw_chunk)
                    chunk_ids.append(chunk_id)
                    embeddings.append(embedding)
                return {
                    "document_id": document_ids,
                    "chunk_id": chunk_ids,
                    "vector": embeddings,
                    "chunk_text": chunks,
                }

            sample_pdf = b"%PDF-1.3\n3 0 obj\n<</Type /Page\n/Parent 1 0 R\n/Resources 2 0 R\n/Contents 4 0 R>>\nendobj\n4 0 obj\n<</Filter /FlateDecode /Length 115>>\nstream\nx\x9c\x15\xcc1\x0e\x820\x18@\xe1\x9dS\xbcM]jk$\xd5\xd5(\x83!\x86\xa1\x17\xf8\xa3\xa5`LIh+\xd7W\xc6\xf7\r\xef\xc0\xbd\xd2\xaa\xb6,\xd5\xc5\xb1o\x0c\xa6VZ\xe3znn%\xf3o\xab\xb1\xe7\xa3:Y\xdc\x8bm\xeb\xf3&1\xc8\xd7\xd3\x97\xc82\xe6\x81\x87\xe42\xcb\x87Vb(\x12<\xdd<=}Jc\x0cL\x91\xee\xda$\xb5\xc3\xbd\xd7\xe9\x0f\x8d\x97 $\nendstream\nendobj\n1 0 obj\n<</Type /Pages\n/Kids [3 0 R ]\n/Count 1\n/MediaBox [0 0 595.28 841.89]\n>>\nendobj\n5 0 obj\n<</Type /Font\n/BaseFont /Helvetica\n/Subtype /Type1\n/Encoding /WinAnsiEncoding\n>>\nendobj\n2 0 obj\n<<\n/ProcSet [/PDF /Text /ImageB /ImageC /ImageI]\n/Font <<\n/F1 5 0 R\n>>\n/XObject <<\n>>\n>>\nendobj\n6 0 obj\n<<\n/Producer (PyFPDF 1.7.2 http://pyfpdf.googlecode.com/)\n/Title (This is a sample title.)\n/Author (Francisco Javier Arceo)\n/CreationDate (D:20250312165548)\n>>\nendobj\n7 0 obj\n<<\n/Type /Catalog\n/Pages 1 0 R\n/OpenAction [3 0 R /FitH null]\n/PageLayout /OneColumn\n>>\nendobj\nxref\n0 8\n0000000000 65535 f \n0000000272 00000 n \n0000000455 00000 n \n0000000009 00000 n \n0000000087 00000 n \n0000000359 00000 n \n0000000559 00000 n \n0000000734 00000 n \ntrailer\n<<\n/Size 8\n/Root 7 0 R\n/Info 6 0 R\n>>\nstartxref\n837\n%%EOF\n"
            sample_pdf_2 = b"%PDF-1.3\n3 0 obj\n<</Type /Page\n/Parent 1 0 R\n/Resources 2 0 R\n/Contents 4 0 R>>\nendobj\n4 0 obj\n<</Filter /FlateDecode /Length 115>>\nstream\nx\x9c\x15\xcc1\x0e\x820\x18@\xe1\x9dS\xbcM]jk$\xd5\xd5(\x83!\x86\xa1\x17\xf8\xa3\xa5`LIh+\xd7W\xc6\xf7\r\xef\xc0\xbd\xd2\xaa\xb6,\xd5\xc5\xb1o\x0c\xa6VZ\xe3znn%\xf3o\xab\xb1\xe7\xa3:Y\xdc\x8bm\xeb\xf3&1\xc8\xd7\xd3\x97\xc82\xe6\x81\x87\xe42\xcb\x87Vb(\x12<\xdd<=}Jc\x0cL\x91\xee\xda$\xb5\xc3\xbd\xd7\xe9\x0f\x8d\x97 $\nendstream\nendobj\n1 0 obj\n<</Type /Pages\n/Kids [3 0 R ]\n/Count 1\n/MediaBox [0 0 595.28 841.89]\n>>\nendobj\n5 0 obj\n<</Type /Font\n/BaseFont /Helvetica\n/Subtype /Type1\n/Encoding /WinAnsiEncoding\n>>\nendobj\n2 0 obj\n<<\n/ProcSet [/PDF /Text /ImageB /ImageC /ImageI]\n/Font <<\n/F1 5 0 R\n>>\n/XObject <<\n>>\n>>\nendobj\n6 0 obj\n<<\n/Producer (PyFPDF 1.7.2 http://pyfpdf.googlecode.com/)\n/Title (This is another sample title.)\n/Author (Frank John Broceo)\n/CreationDate (D:20250312165548)\n>>\nendobj\n7 0 obj\n<<\n/Type /Catalog\n/Pages 1 0 R\n/OpenAction [3 0 R /FitH null]\n/PageLayout /OneColumn\n>>\nendobj\nxref\n0 8\n0000000000 65535 f \n0000000272 00000 n \n0000000455 00000 n \n0000000009 00000 n \n0000000087 00000 n \n0000000359 00000 n \n0000000559 00000 n \n0000000734 00000 n \ntrailer\n<<\n/Size 8\n/Root 7 0 R\n/Info 6 0 R\n>>\nstartxref\n837\n%%EOF\n"

            sample_input = {
                "pdf_bytes": sample_pdf,
                "file_name": "sample_pdf",
                "document_id": "doc_1",
            }
            sample_input_2 = {
                "pdf_bytes": sample_pdf_2,
                "file_name": "sample_pdf_2",
                "document_id": "doc_2",
            }
            docling_output = docling_transform_docs.feature_transformation.udf(
                sample_input
            )

            self.store.apply([docling_transform_docs])

            assert docling_output == {
                "document_id": ["doc_1"],
                "chunk_id": ["chunk-0"],
                "vector": [[0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]],
                "chunk_text": [
                    "Let's have fun with Natural Language Processing on PDFs."
                ],
            }

            input_df = pd.DataFrame([sample_input])
            self.store.write_to_online_store("docling_transform_docs", input_df)

            conn = self.store._provider._online_store._conn
            document_table = self.store._provider._online_store._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' and name like '%docling%';"
            ).fetchall()[0][0]
            written_data = pd.read_sql_query(f"select * from {document_table}", conn)
            assert (
                written_data[written_data["feature_name"] == "document_id"][
                    "vector_value"
                ].values[0]
                == "doc_1"
            )
            assert (
                written_data[written_data["feature_name"] == "chunk_id"][
                    "vector_value"
                ].values[0]
                == "chunk-0"
            )

            online_features = self.store.get_online_features(
                features=[
                    "docling_transform_docs:document_id",
                    "docling_transform_docs:chunk_id",
                    "docling_transform_docs:chunk_text",
                ],
                entity_rows=[{"document_id": "doc_1", "chunk_id": "chunk-0"}],
            ).to_dict()
            online_features == {
                "document_id": ["doc_1"],
                "chunk_id": ["chunk-0"],
                "chunk_text": [
                    "Let's have fun with Natural Language Processing on PDFs."
                ],
            }

            multiple_inputs_df = pd.DataFrame([sample_input, sample_input_2])
            # note this test needs to be updated with writing to the online store to verify this behavior works
            assert multiple_inputs_df is not None
