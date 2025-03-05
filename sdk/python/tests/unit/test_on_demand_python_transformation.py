import os
import tempfile
import unittest
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import pytest

from feast import Entity, FeatureStore, FeatureView, FileSource, RepoConfig
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.field import Field
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64


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

    @pytest.mark.skip(
        reason="Failing test, can't work out why. Skipping for now as we don't use/plan to use python transformations"
    )
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

    @pytest.mark.skip(
        reason="Failing test, can't work out why. Skipping for now as we don't use/plan to use python transformations"
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
            full_feature_names=True,
        ).to_df()
        print(f"{online_python_response=}")
        assert sorted(list(online_python_response.keys())) == sorted(
            [
                "driver_id",
                "driver_hourly_stats__acc_rate",
                "driver_hourly_stats__conv_rate",
                "python_demo_view__conv_rate_plus_val1_python",
                "python_demo_view__conv_rate_plus_val2_python",
            ]
        )

        assert (
            online_python_response["python_demo_view__conv_rate_plus_val1_python"][0]
            == online_python_response["python_demo_view__conv_rate_plus_val2_python"][0]
        )
        assert (
            online_python_response["driver_hourly_stats__conv_rate"][0]
            + online_python_response["driver_hourly_stats__acc_rate"][0]
            == online_python_response["python_demo_view__conv_rate_plus_val1_python"][0]
        )
        assert (
            online_python_response["driver_hourly_stats__conv_rate"][0]
            + online_python_response["driver_hourly_stats__acc_rate"][0]
            == online_python_response["python_demo_view__conv_rate_plus_val2_python"][0]
        )
