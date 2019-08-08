# Copyright 2018 The Feast Authors
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

import pandas as pd
import pytest
import ntpath

from feast.sdk.client import Client
from feast.sdk.resources.feature import Feature, ValueType
from feast.sdk.importer import _create_feature, Importer
from feast.sdk.utils.gs_utils import is_gs_path
from datetime import datetime
import pytz
from unittest.mock import MagicMock
import numpy as np


class TestImporter(object):
    def test_from_csv(self):
        csv_path = "tests/data/driver_features.csv"
        entity_name = "driver"
        owner = "owner@feast.com"
        staging_location = "gs://test-bucket"
        id_column = "driver_id"
        feature_columns = ["avg_distance_completed", "avg_customer_distance_completed"]
        timestamp_column = "ts"

        importer = Importer.from_csv(
            path=csv_path,
            entity=entity_name,
            owner=owner,
            staging_location=staging_location,
            id_column=id_column,
            feature_columns=feature_columns,
            timestamp_column=timestamp_column,
        )

        self._validate_csv_importer(
            importer,
            csv_path,
            entity_name,
            owner,
            staging_location,
            id_column,
            feature_columns,
            timestamp_column,
        )

    def test_from_csv_id_column_not_specified(self):
        with pytest.raises(ValueError, match="Column with name driver is not found"):
            feature_columns = [
                "avg_distance_completed",
                "avg_customer_distance_completed",
            ]
            csv_path = "tests/data/driver_features.csv"
            Importer.from_csv(
                path=csv_path,
                entity="driver",
                owner="owner@feast.com",
                staging_location="gs://test-bucket",
                feature_columns=feature_columns,
                timestamp_column="ts",
            )

    def test_from_csv_timestamp_column_not_specified(self):
        feature_columns = [
            "avg_distance_completed",
            "avg_customer_distance_completed",
            "avg_distance_cancelled",
        ]
        csv_path = "tests/data/driver_features.csv"
        entity_name = "driver"
        owner = "owner@feast.com"
        staging_location = "gs://test-bucket"
        id_column = "driver_id"
        importer = Importer.from_csv(
            path=csv_path,
            entity=entity_name,
            owner=owner,
            staging_location=staging_location,
            id_column=id_column,
            feature_columns=feature_columns,
        )

        self._validate_csv_importer(
            importer,
            csv_path,
            entity_name,
            owner,
            staging_location=staging_location,
            id_column=id_column,
            feature_columns=feature_columns,
        )

    def test_from_csv_feature_columns_not_specified(self):
        csv_path = "tests/data/driver_features.csv"
        entity_name = "driver"
        owner = "owner@feast.com"
        staging_location = "gs://test-bucket"
        id_column = "driver_id"
        timestamp_column = "ts"
        importer = Importer.from_csv(
            path=csv_path,
            entity=entity_name,
            owner=owner,
            staging_location=staging_location,
            id_column=id_column,
            timestamp_column=timestamp_column,
        )

        self._validate_csv_importer(
            importer,
            csv_path,
            entity_name,
            owner,
            staging_location=staging_location,
            id_column=id_column,
            timestamp_column=timestamp_column,
        )

    def test_from_csv_staging_location_not_valid(self):
        with pytest.raises(
            ValueError, match="Staging location must be in GCS"
        ) as e_info:
            feature_columns = [
                "avg_distance_completed",
                "avg_customer_distance_completed",
            ]
            csv_path = "tests/data/driver_features.csv"
            Importer.from_csv(
                path=csv_path,
                entity="driver",
                owner="owner@feast.com",
                staging_location="/home",
                feature_columns=feature_columns,
                timestamp_column="ts",
            )

    def test_from_df_with_timestamp_and_timezone_utc(self):
        df = pd.DataFrame(
            {
                "entity_key": [1, 3, 4],
                "feature": [5, 12, 1],
                "timestamp": [
                    datetime(2019, 5, 12, 14, 30, 0).replace(tzinfo=pytz.utc)
                    for _ in range(3)
                ],
            }
        )
        importer = Importer.from_df(
            df, entity="test_entity", owner="test_owner", id_column="entity_key"
        )
        assert str(importer.df["timestamp"].dtype) == "datetime64[ns]"
        np.testing.assert_array_equal(importer.df["entity_key"].values, [1, 3, 4])
        np.testing.assert_array_equal(importer.df["feature"].values, [5, 12, 1])
        assert importer.df["timestamp"].astype("int64")[0] == 1557671400000000000

    def test_from_df_with_timestamp_and_timezone_non_utc(self):
        df = pd.DataFrame(
            {
                "entity_key": [1, 3, 4],
                "feature": [5, 12, 1],
                "timestamp": [
                    pytz.timezone("Asia/Jakarta").localize(
                        datetime(2019, 5, 12, 14, 30, 0)
                    )
                    for _ in range(3)
                ],
            }
        )
        importer = Importer.from_df(
            df, entity="test_entity", owner="test_owner", id_column="entity_key"
        )
        assert str(importer.df["timestamp"].dtype) == "datetime64[ns]"
        assert importer.df["timestamp"].astype("int64")[0] == 1557646200000000000

    def test_from_df_with_timestamp_and_no_timezone(self):
        df = pd.DataFrame(
            {
                "entity_key": [1, 3, 4],
                "feature": [5, 12, 1],
                "timestamp": [datetime(2019, 5, 12, 14, 30, 0) for _ in range(3)],
            }
        )
        importer = Importer.from_df(
            df, entity="test_entity", owner="test_owner", id_column="entity_key"
        )
        assert str(importer.df["timestamp"].dtype) == "datetime64[ns]"
        assert importer.df["timestamp"].astype("int64")[0] == 1557671400000000000

    def test_from_df(self):
        csv_path = "tests/data/driver_features.csv"
        df = pd.read_csv(csv_path)
        staging_location = "gs://test-bucket"
        entity = "driver"

        importer = Importer.from_df(
            df=df,
            entity=entity,
            owner="owner@feast.com",
            staging_location=staging_location,
            id_column="driver_id",
            timestamp_column="ts",
        )

        assert importer.require_staging == True
        assert "{}/tmp_{}".format(staging_location, entity) in importer.remote_path
        for feature in importer.features.values():
            assert feature.name in df.columns
            assert feature.id == "driver." + feature.name

        import_spec = importer.spec
        assert import_spec.type == "file.csv"
        assert import_spec.sourceOptions == {"path": importer.remote_path}
        assert import_spec.entities == ["driver"]

        schema = import_spec.schema
        assert schema.entityIdColumn == "driver_id"
        assert schema.timestampValue is not None
        feature_columns = [
            "completed",
            "avg_distance_completed",
            "avg_customer_distance_completed",
            "avg_distance_cancelled",
        ]
        for col, field in zip(df.columns.values, schema.fields):
            assert col == field.name
            if col in feature_columns:
                assert field.featureId == "driver." + col

    def test_stage_df_without_timestamp(self, mocker):
        mocker.patch("feast.sdk.importer.df_to_gcs", return_value=True)
        feature_columns = [
            "avg_distance_completed",
            "avg_customer_distance_completed",
            "avg_distance_cancelled",
        ]
        csv_path = "tests/data/driver_features.csv"
        entity_name = "driver"
        owner = "owner@feast.com"
        staging_location = "gs://test-bucket"
        id_column = "driver_id"
        importer = Importer.from_csv(
            path=csv_path,
            entity=entity_name,
            owner=owner,
            staging_location=staging_location,
            id_column=id_column,
            feature_columns=feature_columns,
        )
        importer.stage(None)

    def _validate_csv_importer(
        self,
        importer,
        csv_path,
        entity_name,
        owner,
        staging_location=None,
        id_column=None,
        feature_columns=None,
        timestamp_column=None,
        timestamp_value=None,
    ):
        df = pd.read_csv(csv_path)
        assert not importer.require_staging == is_gs_path(csv_path)
        if importer.require_staging:
            assert importer.remote_path == "{}/{}".format(
                staging_location, ntpath.basename(csv_path)
            )

        # check features created
        for feature in importer.features.values():
            assert feature.name in df.columns
            assert feature.id == "{}.{}".format(entity_name, feature.name)

        import_spec = importer.spec
        assert import_spec.type == "file.csv"
        path = importer.remote_path if importer.require_staging else csv_path
        assert import_spec.sourceOptions == {"path": path}
        assert import_spec.entities == [entity_name]

        schema = import_spec.schema
        assert (
            schema.entityIdColumn == id_column if id_column is not None else entity_name
        )
        if timestamp_column is not None:
            assert schema.timestampColumn == timestamp_column
        elif timestamp_value is not None:
            assert schema.timestampValue == timestamp_value

        if feature_columns is None:
            feature_columns = list(df.columns.values)
            feature_columns.remove(id_column)
            feature_columns.remove(timestamp_column)

        # check schema's field
        for col, field in zip(df.columns.values, schema.fields):
            assert col == field.name
            if col in feature_columns:
                assert field.featureId == "{}.{}".format(entity_name, col).lower()


class TestHelpers:
    def test_create_feature(self):
        col = pd.Series([1] * 3, dtype="int32", name="test")
        expected = Feature(
            name="test", entity="test", owner="person", value_type=ValueType.INT32
        )
        actual = _create_feature(col, "test", "person")
        assert actual.id == expected.id
        assert actual.value_type == expected.value_type
        assert actual.owner == expected.owner

    def test_create_feature_with_stores(self):
        col = pd.Series([1] * 3, dtype="int32", name="test")
        expected = Feature(
            name="test", entity="test", owner="person", value_type=ValueType.INT32
        )
        actual = _create_feature(col, "test", "person")
        assert actual.id == expected.id
        assert actual.value_type == expected.value_type
        assert actual.owner == expected.owner
