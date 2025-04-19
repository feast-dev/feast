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

import pandas as pd

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
from feast.types import Array, Float32, Float64, Int64, PdfBytes, String, ValueType


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
            # Before apply() join_keys is empty
            assert driver_stats_fv.join_keys == []
            assert driver_stats_fv.entity_columns == []

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
            # after apply() join_keys is [driver]
            assert driver_stats_fv.join_keys == [driver.join_key]
            assert driver_stats_fv.entity_columns[0].name == driver.join_key

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
                "test_view:conv_rate_plus_acc",
            ],
        ).to_dict()

        assert len(online_python_response) == 4
        assert all(
            key in online_python_response.keys()
            for key in [
                "driver_id",
                "acc_rate",
                "conv_rate",
                "conv_rate_plus_acc",
            ]
        )


class TestOnlineWritesWithTransform(unittest.TestCase):
    def test_transform_on_write_pdf(self):
        with tempfile.TemporaryDirectory() as data_dir:
            self.store = FeatureStore(
                config=RepoConfig(
                    project="test_write_to_online_store_with_transform",
                    registry=os.path.join(data_dir, "registry.db"),
                    provider="local",
                    entity_key_serialization_version=2,
                    online_store=SqliteOnlineStoreConfig(
                        path=os.path.join(data_dir, "online.db")
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

            input_request_pdf = RequestSource(
                name="pdf_request_source",
                schema=[
                    Field(name="document_id", dtype=String),
                    Field(name="pdf_bytes", dtype=PdfBytes),
                    Field(name="file_name", dtype=String),
                ],
            )

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
            def transform_pdf_on_write_view(inputs: dict[str, Any]) -> dict[str, Any]:
                k = 10
                return {
                    "document_id": ["doc_1", "doc_2"],
                    "chunk_id": ["chunk-1", "chunk-2"],
                    "vector": [[0.5] * k, [0.4] * k],
                    "chunk_text": ["chunk text 1", "chunk text 2"],
                }

            self.store.apply([chunk, document, transform_pdf_on_write_view])

            sample_pdf = b"%PDF-1.3\n3 0 obj\n<</Type /Page\n/Parent 1 0 R\n/Resources 2 0 R\n/Contents 4 0 R>>\nendobj\n4 0 obj\n<</Filter /FlateDecode /Length 115>>\nstream\nx\x9c\x15\xcc1\x0e\x820\x18@\xe1\x9dS\xbcM]jk$\xd5\xd5(\x83!\x86\xa1\x17\xf8\xa3\xa5`LIh+\xd7W\xc6\xf7\r\xef\xc0\xbd\xd2\xaa\xb6,\xd5\xc5\xb1o\x0c\xa6VZ\xe3znn%\xf3o\xab\xb1\xe7\xa3:Y\xdc\x8bm\xeb\xf3&1\xc8\xd7\xd3\x97\xc82\xe6\x81\x87\xe42\xcb\x87Vb(\x12<\xdd<=}Jc\x0cL\x91\xee\xda$\xb5\xc3\xbd\xd7\xe9\x0f\x8d\x97 $\nendstream\nendobj\n1 0 obj\n<</Type /Pages\n/Kids [3 0 R ]\n/Count 1\n/MediaBox [0 0 595.28 841.89]\n>>\nendobj\n5 0 obj\n<</Type /Font\n/BaseFont /Helvetica\n/Subtype /Type1\n/Encoding /WinAnsiEncoding\n>>\nendobj\n2 0 obj\n<<\n/ProcSet [/PDF /Text /ImageB /ImageC /ImageI]\n/Font <<\n/F1 5 0 R\n>>\n/XObject <<\n>>\n>>\nendobj\n6 0 obj\n<<\n/Producer (PyFPDF 1.7.2 http://pyfpdf.googlecode.com/)\n/Title (This is a sample title. And this is another sentence. Finally, this is the third sentence.)\n/Author (Francisco Javier Arceo)\n/CreationDate (D:20250312165548)\n>>\nendobj\n7 0 obj\n<<\n/Type /Catalog\n/Pages 1 0 R\n/OpenAction [3 0 R /FitH null]\n/PageLayout /OneColumn\n>>\nendobj\nxref\n0 8\n0000000000 65535 f \n0000000272 00000 n \n0000000455 00000 n \n0000000009 00000 n \n0000000087 00000 n \n0000000359 00000 n \n0000000559 00000 n \n0000000734 00000 n \ntrailer\n<<\n/Size 8\n/Root 7 0 R\n/Info 6 0 R\n>>\nstartxref\n837\n%%EOF\n"
            sample_input = {
                "pdf_bytes": sample_pdf,
                "file_name": "sample_pdf",
                "document_id": "doc_1",
            }
            input_df = pd.DataFrame([sample_input])

            self.store.write_to_online_store(
                feature_view_name="transform_pdf_on_write_view",
                df=input_df,
            )

        assert 1 == 2
