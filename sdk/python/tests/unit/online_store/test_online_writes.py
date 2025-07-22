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

import asyncio
import os
import shutil
import tempfile
import unittest
import warnings
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
from feast.types import Array, Float32, Float64, Int64, PdfBytes, String, ValueType


class TestOnlineWrites(unittest.TestCase):
    def setUp(self):
        with tempfile.TemporaryDirectory() as data_dir:
            self.store = FeatureStore(
                config=RepoConfig(
                    project="test_write_to_online_store",
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


class TestEmptyDataFrameValidation(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.mkdtemp()
        self.store = FeatureStore(
            config=RepoConfig(
                project="test_empty_df_validation",
                registry=os.path.join(self.data_dir, "registry.db"),
                provider="local",
                entity_key_serialization_version=3,
                online_store=SqliteOnlineStoreConfig(
                    path=os.path.join(self.data_dir, "online.db")
                ),
            )
        )

        # Generate test data for schema creation
        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=1)

        driver_entities = [1001]
        driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)
        driver_stats_path = os.path.join(self.data_dir, "driver_stats.parquet")
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

        self.store.apply([driver, driver_stats_source, driver_stats_fv])

    def tearDown(self):
        shutil.rmtree(self.data_dir)

    def test_empty_dataframe_warns(self):
        """Test that completely empty dataframe warns"""
        empty_df = pd.DataFrame()

        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", df=empty_df
            )

        # Check that our specific warning message is present
        warning_messages = [str(w.message) for w in warning_list]
        self.assertTrue(
            any(
                "Cannot write empty dataframe to online store" in msg
                for msg in warning_messages
            ),
            f"Expected warning not found. Actual warnings: {warning_messages}",
        )

    def test_empty_dataframe_async_warns(self):
        """Test that completely empty dataframe warns instead of raising error in async version"""

        async def test_async_empty():
            empty_df = pd.DataFrame()

            with warnings.catch_warnings(record=True) as warning_list:
                warnings.simplefilter("always")
                await self.store.write_to_online_store_async(
                    feature_view_name="driver_hourly_stats", df=empty_df
                )

            # Check that our specific warning message is present
            warning_messages = [str(w.message) for w in warning_list]
            self.assertTrue(
                any(
                    "Cannot write empty dataframe to online store" in msg
                    for msg in warning_messages
                ),
                f"Expected warning not found. Actual warnings: {warning_messages}",
            )

        asyncio.run(test_async_empty())

    def test_dataframe_with_empty_feature_columns_warns(self):
        """Test that dataframe with entity data but empty feature columns warns instead of raising error"""
        current_time = pd.Timestamp.now()
        df_with_entity_only = pd.DataFrame(
            {
                "driver_id": [1001, 1002, 1003],
                "event_timestamp": [current_time] * 3,
                "created": [current_time] * 3,
                "conv_rate": [None, None, None],  # All nulls
                "acc_rate": [None, None, None],  # All nulls
                "avg_daily_trips": [None, None, None],  # All nulls
            }
        )

        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", df=df_with_entity_only
            )

        # Check that our specific warning message is present
        warning_messages = [str(w.message) for w in warning_list]
        self.assertTrue(
            any(
                "Cannot write dataframe with empty feature columns to online store"
                in msg
                for msg in warning_messages
            ),
            f"Expected warning not found. Actual warnings: {warning_messages}",
        )

    def test_dataframe_with_empty_feature_columns_async_warns(self):
        """Test that dataframe with entity data but empty feature columns warns instead of raising error in async version"""

        async def test_async_empty_features():
            current_time = pd.Timestamp.now()
            df_with_entity_only = pd.DataFrame(
                {
                    "driver_id": [1001, 1002, 1003],
                    "event_timestamp": [current_time] * 3,
                    "created": [current_time] * 3,
                    "conv_rate": [None, None, None],
                    "acc_rate": [None, None, None],
                    "avg_daily_trips": [None, None, None],
                }
            )

            with warnings.catch_warnings(record=True) as warning_list:
                warnings.simplefilter("always")
                await self.store.write_to_online_store_async(
                    feature_view_name="driver_hourly_stats", df=df_with_entity_only
                )

            # Check that our specific warning message is present
            warning_messages = [str(w.message) for w in warning_list]
            self.assertTrue(
                any(
                    "Cannot write dataframe with empty feature columns to online store"
                    in msg
                    for msg in warning_messages
                ),
                f"Expected warning not found. Actual warnings: {warning_messages}",
            )

        asyncio.run(test_async_empty_features())

    def test_valid_dataframe(self):
        """Test that valid dataframe with feature data succeeds"""
        current_time = pd.Timestamp.now()
        valid_df = pd.DataFrame(
            {
                "driver_id": [1001, 1002],
                "event_timestamp": [current_time] * 2,
                "created": [current_time] * 2,
                "conv_rate": [0.5, 0.7],
                "acc_rate": [0.8, 0.9],
                "avg_daily_trips": [10, 12],
            }
        )

        # This should not raise an exception or warnings
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", df=valid_df
            )

        # Validate that our specific empty dataframe warnings are NOT raised for valid data
        empty_df_warning_messages = [
            "Cannot write empty dataframe to online store",
            "Cannot write dataframe with empty feature columns to online store"
        ]
        
        for warning in warning_list:
            warning_message = str(warning.message)
            for empty_df_msg in empty_df_warning_messages:
                self.assertNotIn(
                    empty_df_msg, 
                    warning_message,
                    f"Valid dataframe should not generate empty dataframe warnings. Found: '{warning_message}'"
                )

    def test_valid_dataframe_async(self):
        """Test that valid dataframe with feature data succeeds in async version"""
        pytest.skip("Feature not implemented yet")

        async def test_async_valid():
            current_time = pd.Timestamp.now()
            valid_df = pd.DataFrame(
                {
                    "driver_id": [1001, 1002],
                    "event_timestamp": [current_time] * 2,
                    "created": [current_time] * 2,
                    "conv_rate": [0.5, 0.7],
                    "acc_rate": [0.8, 0.9],
                    "avg_daily_trips": [10, 12],
                }
            )

            # This should not raise an exception or warnings
            with warnings.catch_warnings(record=True) as warning_list:
                warnings.simplefilter("always")
                await self.store.write_to_online_store_async(
                    feature_view_name="driver_hourly_stats", df=valid_df
                )

            # Validate that our specific empty dataframe warnings are NOT raised for valid data
            empty_df_warning_messages = [
                "Cannot write empty dataframe to online store",
                "Cannot write dataframe with empty feature columns to online store"
            ]
            
            for warning in warning_list:
                warning_message = str(warning.message)
                for empty_df_msg in empty_df_warning_messages:
                    self.assertNotIn(
                        empty_df_msg, 
                        warning_message,
                        f"Valid dataframe should not generate empty dataframe warnings in async. Found: '{warning_message}'"
                    )

        asyncio.run(test_async_valid())

    def test_mixed_dataframe_with_some_valid_features(self):
        """Test that dataframe with some valid feature values succeeds"""
        current_time = pd.Timestamp.now()
        mixed_df = pd.DataFrame(
            {
                "driver_id": [1001, 1002, 1003],
                "event_timestamp": [current_time] * 3,
                "created": [current_time] * 3,
                "conv_rate": [0.5, None, 0.7],  # Mixed values
                "acc_rate": [0.8, 0.9, None],  # Mixed values
                "avg_daily_trips": [10, 12, 15],  # All valid
            }
        )

        # This should not raise an exception because not all feature values are null
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", df=mixed_df
            )

        # Validate that our specific empty dataframe warnings are NOT raised for mixed valid data
        empty_df_warning_messages = [
            "Cannot write empty dataframe to online store",
            "Cannot write dataframe with empty feature columns to online store"
        ]
        
        for warning in warning_list:
            warning_message = str(warning.message)
            for empty_df_msg in empty_df_warning_messages:
                self.assertNotIn(
                    empty_df_msg, 
                    warning_message,
                    f"Mixed dataframe with valid features should not generate empty dataframe warnings. Found: '{warning_message}'"
                )

    def test_empty_inputs_dict_warns(self):
        """Test that empty inputs dict warns instead of raising error"""
        empty_inputs = {
            "driver_id": [],
            "conv_rate": [],
            "acc_rate": [],
            "avg_daily_trips": [],
        }

        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", inputs=empty_inputs
            )

            # Check that our specific warning message is present
            warning_messages = [str(w.message) for w in warning_list]
            self.assertTrue(
                any(
                    "Cannot write empty dataframe to online store" in msg
                    for msg in warning_messages
                ),
                f"Expected warning not found. Actual warnings: {warning_messages}",
            )

    def test_inputs_dict_with_empty_features_warns(self):
        """Test that inputs dict with empty feature values warns instead of raising error"""
        current_time = pd.Timestamp.now()
        empty_feature_inputs = {
            "driver_id": [1001, 1002, 1003],
            "event_timestamp": [current_time] * 3,
            "created": [current_time] * 3,
            "conv_rate": [None, None, None],
            "acc_rate": [None, None, None],
            "avg_daily_trips": [None, None, None],
        }

        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            self.store.write_to_online_store(
                feature_view_name="driver_hourly_stats", inputs=empty_feature_inputs
            )

        # Check that our specific warning message is present
        warning_messages = [str(w.message) for w in warning_list]
        self.assertTrue(
            any(
                "Cannot write dataframe with empty feature columns to online store"
                in msg
                for msg in warning_messages
            ),
            f"Expected warning not found. Actual warnings: {warning_messages}",
        )

    def test_multiple_feature_views_materialization_with_empty_data(self):
        """Test materializing multiple feature views where one has empty data - should not break materialization"""

        with tempfile.TemporaryDirectory() as data_dir:
            # Create a new store for this test
            test_store = FeatureStore(
                config=RepoConfig(
                    project="test_multiple_fv_materialization",
                    registry=os.path.join(data_dir, "registry.db"),
                    provider="local",
                    entity_key_serialization_version=3,
                    online_store=SqliteOnlineStoreConfig(
                        path=os.path.join(data_dir, "online.db")
                    ),
                )
            )

            # Create entities
            driver = Entity(name="driver", join_keys=["driver_id"])
            customer = Entity(name="customer", join_keys=["customer_id"])

            # Create 5 feature views with data
            current_time = pd.Timestamp.now().replace(microsecond=0)
            start_date = current_time - timedelta(hours=2)
            end_date = current_time - timedelta(minutes=10)
            feature_views = []
            dataframes = []
            offline_paths = []

            for i in range(5):
                # Create file path for offline data
                offline_path = os.path.join(data_dir, f"feature_view_{i + 1}.parquet")
                offline_paths.append(offline_path)

                # Create feature view with real file source
                fv = FeatureView(
                    name=f"feature_view_{i + 1}",
                    entities=[driver if i % 2 == 0 else customer],
                    ttl=timedelta(days=1),
                    schema=[
                        Field(name=f"feature_{i + 1}_rate", dtype=Float32),
                        Field(name=f"feature_{i + 1}_count", dtype=Int64),
                    ],
                    online=True,
                    source=FileSource(
                        name=f"source_{i + 1}",
                        path=offline_path,
                        timestamp_field="event_timestamp",
                        created_timestamp_column="created",
                    ),
                )
                feature_views.append(fv)

                # Create data - make 2nd feature view (index 1) empty
                if i == 1:  # 2nd feature view gets empty data
                    df = pd.DataFrame()  # Empty dataframe
                else:
                    # Create valid data for other feature views
                    entity_key = "driver_id" if i % 2 == 0 else "customer_id"
                    df = pd.DataFrame(
                        {
                            entity_key: [1000 + j for j in range(3)],
                            "event_timestamp": [
                                start_date + timedelta(minutes=j * 10) for j in range(3)
                            ],
                            "created": [current_time] * 3,
                            f"feature_{i + 1}_rate": [0.5 + j * 0.1 for j in range(3)],
                            f"feature_{i + 1}_count": [10 + j for j in range(3)],
                        }
                    )

                # Write data to offline store (parquet files) - offline store allows empty dataframes
                if len(df) > 0:
                    df.to_parquet(offline_path, allow_truncated_timestamps=True)
                else:
                    # Create empty parquet file with correct schema (timezone-aware timestamps)
                    entity_key = "driver_id" if i % 2 == 0 else "customer_id"
                    empty_schema_df = pd.DataFrame(
                        {
                            entity_key: pd.Series([], dtype="int64"),
                            "event_timestamp": pd.Series(
                                [], dtype="datetime64[ns, UTC]"
                            ),  # Timezone-aware
                            "created": pd.Series(
                                [], dtype="datetime64[ns, UTC]"
                            ),  # Timezone-aware
                            f"feature_{i + 1}_rate": pd.Series([], dtype="float32"),
                            f"feature_{i + 1}_count": pd.Series([], dtype="int64"),
                        }
                    )
                    empty_schema_df.to_parquet(
                        offline_path, allow_truncated_timestamps=True
                    )

                dataframes.append(df)

            # Apply entities and feature views
            test_store.apply([driver, customer] + feature_views)

            # Test: Use materialize() to move data from offline to online store
            # This should NOT raise any exceptions even with empty data
            try:
                with warnings.catch_warnings(record=True) as warning_list:
                    warnings.simplefilter("always")
                    test_store.materialize(
                        start_date=start_date,
                        end_date=end_date,
                        feature_views=[fv.name for fv in feature_views],
                    )
            except Exception as e:
                self.fail(f"Materialization raised an unexpected exception: {e}")

            # Validate that our specific empty dataframe warnings are NOT raised during materialization
            empty_df_warning_messages = [
                "Cannot write empty dataframe to online store",
                "Cannot write dataframe with empty feature columns to online store"
            ]
            
            for warning in warning_list:
                warning_message = str(warning.message)
                for empty_df_msg in empty_df_warning_messages:
                    self.assertNotIn(
                        empty_df_msg, 
                        warning_message,
                        f"Materialization should not generate empty dataframe warnings. Found: '{warning_message}'"
                    )

            # Verify that the operation was successful by checking that non-empty feature views have data
            successful_materializations = 0
            for i, fv in enumerate(feature_views):
                if i != 1:  # Skip the empty one (2nd feature view)
                    entity_key = "driver_id" if i % 2 == 0 else "customer_id"
                    entity_value = 1000  # First entity from our test data

                    # Try to retrieve features to verify they were written successfully
                    online_response = test_store.get_online_features(
                        entity_rows=[{entity_key: entity_value}],
                        features=[
                            f"{fv.name}:feature_{i + 1}_rate",
                            f"{fv.name}:feature_{i + 1}_count",
                        ],
                    ).to_dict()

                    # Verify we got some data back (not None/null)
                    rate_value = online_response.get(f"feature_{i + 1}_rate")
                    count_value = online_response.get(f"feature_{i + 1}_count")

                    if rate_value is not None and count_value is not None:
                        successful_materializations += 1

                    self.assertIsNotNone(rate_value)
                    self.assertIsNotNone(count_value)

            # Verify that 4 out of 4 non-empty feature views were successfully materialized
            self.assertEqual(successful_materializations, 4)


class TestOnlineWritesWithTransform(unittest.TestCase):
    def test_transform_on_write_pdf(self):
        with tempfile.TemporaryDirectory() as data_dir:
            self.store = FeatureStore(
                config=RepoConfig(
                    project="test_write_to_online_store_with_transform",
                    registry=os.path.join(data_dir, "registry.db"),
                    provider="local",
                    entity_key_serialization_version=3,
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
