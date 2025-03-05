# Copyright 2022 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import tempfile
import unittest
from datetime import datetime, timedelta
from typing import Any

from feast import Entity, FeatureStore, FeatureView, FileSource, RepoConfig
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.field import Field
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64


class TestOnlineWrites(unittest.TestCase):
    def setUp(self):
        with tempfile.TemporaryDirectory() as data_dir:
            self.store = FeatureStore(
                config=RepoConfig(
                    project="test_write_to_online_store",
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

            # TODO: This view is not used as python transformations don't work in this version of feast
            @on_demand_feature_view(
                sources=[driver_stats_fv[["conv_rate", "acc_rate"]]],
                schema=[Field(name="conv_rate_plus_acc", dtype=Float64)],
                mode="python",
            )
            def test_view(inputs: dict[str, Any]) -> dict[str, Any]:
                output: dict[str, Any] = {
                    "conv_rate_plus_acc": [
                        conv_rate + acc_rate
                        for conv_rate, acc_rate in zip(
                            inputs["conv_rate"], inputs["acc_rate"]
                        )
                    ]
                }
                return output

            self.store.apply(
                [
                    driver,
                    driver_stats_source,
                    driver_stats_fv,
                    test_view,
                ]
            )
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", df=driver_df
            )
            # This will give the intuitive structure of the data as:
            # {"driver_id": [..], "conv_rate": [..], "acc_rate": [..], "avg_daily_trips": [..]}
            driver_dict = driver_df.to_dict(orient="list")
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats",
                inputs=driver_dict,
            )

    def test_online_retrieval(self):
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
            ],
        ).to_dict()

        assert len(online_python_response) == 3
        assert all(
            key in online_python_response.keys()
            for key in [
                "driver_id",
                "acc_rate",
                "conv_rate",
            ]
        )
