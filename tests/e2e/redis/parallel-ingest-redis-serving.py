import os
import uuid
from datetime import datetime

import pytest
from google.protobuf.duration_pb2 import Duration

from feast.client import Client
from feast.data_source import DataSource, FileOptions, SourceType
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_table import FeatureTable
from feast.value_type import ValueType

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
PROJECT_NAME = "basic_" + uuid.uuid4().hex.upper()[0:6]


@pytest.fixture(scope="module")
def client(pytestconfig):
    core_url = pytestconfig.getoption("core_url")
    serving_url = pytestconfig.getoption("serving_url")

    client = Client(core_url=core_url, serving_url=serving_url,)

    client.set_project(PROJECT_NAME)

    return client


@pytest.mark.incremental
class TestBasicIngestionRetrieval:
    def setup_class(cls):
        prefix = "basic_ingestion"
        suffix = str(int(datetime.now().timestamp()))
        cls.customer_ft_name = f"{prefix}_customer_{suffix}"
        cls.driver_ft_name = f"{prefix}_driver_{suffix}"

        cls.customer_entity = Entity(
            name="customer_id",
            description="Customer entity for rides",
            value_type=ValueType.STRING,
            labels={"team": "customer_service", "common_key": "common_val"},
        )

        cls.driver_entity = Entity(
            name="driver_id",
            description="Driver entity for car rides",
            value_type=ValueType.STRING,
            labels={"team": "matchmaking", "common_key": "common_val"},
        )

        cls.basic_ft_spec = FeatureTable.from_yaml(
            f"{DIR_PATH}/specifications/dev_ft.yaml"
        )

    def test_discovery(self, client):

        # ApplyEntity
        client.apply_entity(self.customer_entity)
        client.apply_entity(self.driver_entity)

        # GetEntity Check
        assert client.get_entity(name="customer_id") == self.customer_entity
        assert client.get_entity(name="driver_id") == self.driver_entity

        # ListEntities Check
        common_filtering_labels = {"common_key": "common_val"}
        matchmaking_filtering_labels = {"team": "matchmaking"}

        actual_common_entities = client.list_entities(labels=common_filtering_labels)
        actual_matchmaking_entities = client.list_entities(
            labels=matchmaking_filtering_labels
        )
        assert len(actual_common_entities) == 2
        assert len(actual_matchmaking_entities) == 1

        # ApplyFeatureTable
        client.apply_feature_table(self.basic_ft_spec, PROJECT_NAME)

        # GetFeatureTable Check
        actual_get_feature_table = client.get_feature_table(name="dev_featuretable")
        assert actual_get_feature_table.name == self.basic_ft_spec.name
        assert actual_get_feature_table.entities == self.basic_ft_spec.entities
        assert actual_get_feature_table.features == self.basic_ft_spec.features

        # ListFeatureTables Check
        actual_list_feature_table = client.list_feature_tables()[0]
        assert actual_list_feature_table.name == self.basic_ft_spec.name
        assert actual_list_feature_table.entities == self.basic_ft_spec.entities
        assert actual_list_feature_table.features == self.basic_ft_spec.features

    def test_basic_retrieval(self, client):
        # TODO: Add ingest and retrieval check
        pass


@pytest.mark.incremental
class TestAllTypesIngestionRetrieval:
    def setup_class(cls):
        prefix = "alltypes_ingestion"
        suffix = str(int(datetime.now().timestamp()))
        batch_source = DataSource(
            type=SourceType(1).name,
            field_mapping={
                "ride_distance": "ride_distance",
                "ride_duration": "ride_duration",
            },
            options=FileOptions(file_format="parquet", file_url="file://feast/*"),
            timestamp_column="ts_col",
            date_partition_column="date_partition_col",
        )

        cls.alltypes_entity = Entity(
            name="alltypes_id",
            description="Driver entity for car rides",
            value_type=ValueType.STRING,
            labels={"cat": "alltypes"},
        )

        cls.alltypes_ft_name = f"{prefix}_alltypes_{suffix}"
        cls.alltypes_ft_spec = FeatureTable(
            name="alltypes",
            entities=["alltypes_id"],
            features=[
                Feature(name="float_feature", dtype=ValueType.FLOAT).to_proto(),
                Feature(name="int64_feature", dtype=ValueType.INT64).to_proto(),
                Feature(name="int32_feature", dtype=ValueType.INT32).to_proto(),
                Feature(name="string_feature", dtype=ValueType.STRING).to_proto(),
                Feature(name="bytes_feature", dtype=ValueType.BYTES).to_proto(),
                Feature(name="bool_feature", dtype=ValueType.BOOL).to_proto(),
                Feature(name="double_feature", dtype=ValueType.DOUBLE).to_proto(),
                Feature(
                    name="double_list_feature", dtype=ValueType.DOUBLE_LIST
                ).to_proto(),
                Feature(
                    name="float_list_feature", dtype=ValueType.FLOAT_LIST
                ).to_proto(),
                Feature(
                    name="int64_list_feature", dtype=ValueType.INT64_LIST
                ).to_proto(),
                Feature(
                    name="int32_list_feature", dtype=ValueType.INT32_LIST
                ).to_proto(),
                Feature(
                    name="string_list_feature", dtype=ValueType.STRING_LIST
                ).to_proto(),
                Feature(
                    name="bytes_list_feature", dtype=ValueType.BYTES_LIST
                ).to_proto(),
                Feature(name="bool_list_feature", dtype=ValueType.BOOL_LIST).to_proto(),
            ],
            max_age=Duration(seconds=3600),
            batch_source=batch_source.to_proto(),
            labels={"cat": "alltypes"},
        )

    def test_discovery(self, client):
        # ApplyEntity
        client.apply_entity(self.alltypes_entity)

        # GetEntity Check
        assert client.get_entity(name="alltypes_id") == self.alltypes_entity

        # ListEntities Check
        alltypes_filtering_labels = {"cat": "alltypes"}
        actual_alltypes_entities = client.list_entities(
            labels=alltypes_filtering_labels
        )
        assert len(actual_alltypes_entities) == 1

        # ApplyFeatureTable
        client.apply_feature_table(self.alltypes_ft_spec, PROJECT_NAME)

        # GetFeatureTable Check
        actual_get_feature_table = client.get_feature_table(name="alltypes")
        assert actual_get_feature_table.name == self.alltypes_ft_spec.name
        assert actual_get_feature_table.entities == self.alltypes_ft_spec.entities
        assert actual_get_feature_table.features == self.alltypes_ft_spec.features

        # ListFeatureTables Check
        actual_list_feature_table = client.list_feature_tables()[0]
        assert actual_list_feature_table.name == self.alltypes_ft_spec.name
        assert actual_list_feature_table.entities == self.alltypes_ft_spec.entities
        assert actual_list_feature_table.features == self.alltypes_ft_spec.features

    def test_alltypes_retrieval(self, client):
        # TODO: Add ingest and retrieval check
        pass
