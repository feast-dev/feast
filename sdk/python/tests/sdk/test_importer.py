import pandas as pd
import pytest
from feast.sdk.resources.feature import Feature
from feast.types.Granularity_pb2 import Granularity
from feast.types.Value_pb2 import ValueType
from feast.sdk.importer import _create_feature, Importer

class TestImporter(object):
    def test_from_csv(self):
        feature_columns = ["avg_distance_completed", "avg_customer_distance_completed"]
        csv_path = "tests/data/driver_features.csv"
        importer = Importer.from_csv(path = csv_path, 
            entity = "driver", 
            granularity = Granularity.DAY, 
            owner = "owner@feast.com", 
            staging_location="gs://test-bucket", 
            id_column = "driver_id",
            feature_columns=feature_columns,
            timestamp_column="ts")
        
        df = pd.read_csv(csv_path)
        assert importer.require_staging == True
        assert importer.remote_path == "gs://test-bucket/driver_features.csv"
        for feature in importer.features:
            assert feature.name in df.columns
            assert feature.id == "driver.day." + feature.name

        import_spec = importer.spec
        assert import_spec.type == "file"
        assert import_spec.options == {"format" : "csv", "url" : "gs://test-bucket/driver_features.csv"}
        assert import_spec.entities == ["driver"]

        schema = import_spec.schema
        assert schema.entityIdColumn == "driver_id"
        assert schema.timestampColumn == "ts"
        for col, field in zip(df.columns.values, schema.fields):
            assert col == field.name
            if col in feature_columns:
                assert field.featureId == "driver.day." + col            


    def test_from_csv_id_column_not_specified(self):
        with pytest.raises(ValueError, match="Column with name driver is not found") as e_info:
            feature_columns = ["avg_distance_completed", "avg_customer_distance_completed"]
            csv_path = "tests/data/driver_features.csv"
            importer = Importer.from_csv(path = csv_path, 
                entity = "driver", 
                granularity = Granularity.DAY, 
                owner = "owner@feast.com", 
                staging_location="gs://test-bucket", 
                feature_columns=feature_columns,
                timestamp_column="ts")

    def test_from_csv_timestamp_column_not_specified(self):
        feature_columns = ["avg_distance_completed", "avg_customer_distance_completed", "avg_distance_cancelled"]
        csv_path = "tests/data/driver_features.csv"
        importer = Importer.from_csv(path = csv_path, 
            entity = "driver", 
            granularity = Granularity.DAY, 
            owner = "owner@feast.com", 
            staging_location="gs://test-bucket", 
            id_column = "driver_id",
            feature_columns=feature_columns,
            timestamp_column="ts")
        
        df = pd.read_csv(csv_path)
        assert importer.require_staging == True
        assert importer.remote_path == "gs://test-bucket/driver_features.csv"
        for feature in importer.features:
            assert feature.name in df.columns
            assert feature.id == "driver.day." + feature.name

        import_spec = importer.spec
        assert import_spec.type == "file"
        assert import_spec.options == {"format" : "csv", "url" : "gs://test-bucket/driver_features.csv"}
        assert import_spec.entities == ["driver"]

        schema = import_spec.schema
        assert schema.entityIdColumn == "driver_id"
        assert schema.timestampValue is not None
        for col, field in zip(df.columns.values, schema.fields):
            assert col == field.name
            if col in feature_columns:
                assert field.featureId == "driver.day." + col
    
    def test_from_csv_feature_columns_not_specified(self):
        csv_path = "tests/data/driver_features.csv"
        importer = Importer.from_csv(path = csv_path, 
            entity = "driver", 
            granularity = Granularity.DAY, 
            owner = "owner@feast.com", 
            staging_location="gs://test-bucket", 
            id_column = "driver_id",
            timestamp_column="ts")
        
        df = pd.read_csv(csv_path)
        assert importer.require_staging == True
        assert importer.remote_path == "gs://test-bucket/driver_features.csv"
        for feature in importer.features:
            assert feature.name in df.columns
            assert feature.id == "driver.day." + feature.name

        import_spec = importer.spec
        assert import_spec.type == "file"
        assert import_spec.options == {"format" : "csv", "url" : "gs://test-bucket/driver_features.csv"}
        assert import_spec.entities == ["driver"]

        schema = import_spec.schema
        assert schema.entityIdColumn == "driver_id"
        assert schema.timestampValue is not None
        feature_columns = ["completed", "avg_distance_completed", "avg_customer_distance_completed", "avg_distance_cancelled"]
        for col, field in zip(df.columns.values, schema.fields):
            assert col == field.name
            if col in feature_columns:
                assert field.featureId == "driver.day." + col

    def test_from_csv_staging_location_not_specified(self):
        with pytest.raises(ValueError, match="Specify staging_location for importing local file/dataframe") as e_info:
            feature_columns = ["avg_distance_completed", "avg_customer_distance_completed"]
            csv_path = "tests/data/driver_features.csv"
            importer = Importer.from_csv(path = csv_path, 
                entity = "driver", 
                granularity = Granularity.DAY, 
                owner = "owner@feast.com", 
                feature_columns=feature_columns,
                timestamp_column="ts")
        
        with pytest.raises(ValueError, match="Staging location must be in GCS") as e_info:
            feature_columns = ["avg_distance_completed", "avg_customer_distance_completed"]
            csv_path = "tests/data/driver_features.csv"
            importer = Importer.from_csv(path = csv_path, 
                entity = "driver", 
                granularity = Granularity.DAY, 
                owner = "owner@feast.com", 
                staging_location = "/home",
                feature_columns=feature_columns,
                timestamp_column="ts")


class TestHelpers:
    def test_create_feature(self):
        col = pd.Series([1]*3,dtype='int32',name="test")
        expected = Feature(name="test", 
                    entity="test",
                    granularity=Granularity.NONE,
                    owner="person",
                    value_type=ValueType.INT32)
        actual = _create_feature(col, "test", Granularity.NONE, "person")
        assert  actual.id == expected.id
        assert  actual.value_type == expected.value_type
        assert  actual.owner == expected.owner

