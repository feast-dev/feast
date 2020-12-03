from datetime import datetime

import numpy as np
import pandas as pd
import pytest
import pytz
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.protobuf.duration_pb2 import Duration
from pandas.testing import assert_frame_equal

from feast.client import Client
from feast.data_format import ParquetFormat, ProtoFormat
from feast.data_source import BigQuerySource, FileSource, KafkaSource
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_table import FeatureTable
from feast.value_type import ValueType
from feast.wait import wait_retry_backoff


@pytest.fixture
def customer_entity():
    return Entity(
        name="customer_id",
        description="Customer entity for rides",
        value_type=ValueType.STRING,
        labels={"team": "customer_service", "common_key": "common_val"},
    )


@pytest.fixture
def driver_entity():
    return Entity(
        name="driver_id",
        description="Driver entity for car rides",
        value_type=ValueType.STRING,
        labels={"team": "matchmaking", "common_key": "common_val"},
    )


@pytest.fixture
def basic_featuretable():
    batch_source = FileSource(
        field_mapping={
            "dev_entity": "dev_entity_field",
            "dev_feature_float": "dev_feature_float_field",
            "dev_feature_string": "dev_feature_string_field",
        },
        file_format=ParquetFormat(),
        file_url="gs://example/feast/*",
        event_timestamp_column="datetime_col",
        created_timestamp_column="timestamp",
        date_partition_column="datetime",
    )
    stream_source = KafkaSource(
        field_mapping={
            "dev_entity": "dev_entity_field",
            "dev_feature_float": "dev_feature_float_field",
            "dev_feature_string": "dev_feature_string_field",
        },
        bootstrap_servers="localhost:9094",
        message_format=ProtoFormat(class_path="class.path"),
        topic="test_topic",
        event_timestamp_column="datetime_col",
    )
    return FeatureTable(
        name="basic_featuretable",
        entities=["driver_id", "customer_id"],
        features=[
            Feature(name="dev_feature_float", dtype=ValueType.FLOAT),
            Feature(name="dev_feature_string", dtype=ValueType.STRING),
        ],
        max_age=Duration(seconds=3600),
        batch_source=batch_source,
        stream_source=stream_source,
        labels={"key1": "val1", "key2": "val2"},
    )


@pytest.fixture
def bq_dataframe():
    N_ROWS = 100
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    return pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "dev_feature_float": [np.float(row) for row in range(N_ROWS)],
            "dev_feature_string": ["feat_" + str(row) for row in range(N_ROWS)],
        }
    )


@pytest.fixture
def alltypes_entity():
    return Entity(
        name="alltypes_id",
        description="Driver entity for car rides",
        value_type=ValueType.STRING,
        labels={"cat": "alltypes"},
    )


@pytest.fixture
def alltypes_featuretable():
    batch_source = FileSource(
        file_format=ParquetFormat(),
        file_url="file://feast/*",
        event_timestamp_column="ts_col",
        created_timestamp_column="timestamp",
        date_partition_column="date_partition_col",
    )
    return FeatureTable(
        name="alltypes",
        entities=["alltypes_id"],
        features=[
            Feature(name="float_feature", dtype=ValueType.FLOAT),
            Feature(name="int64_feature", dtype=ValueType.INT64),
            Feature(name="int32_feature", dtype=ValueType.INT32),
            Feature(name="string_feature", dtype=ValueType.STRING),
            Feature(name="bytes_feature", dtype=ValueType.BYTES),
            Feature(name="bool_feature", dtype=ValueType.BOOL),
            Feature(name="double_feature", dtype=ValueType.DOUBLE),
            Feature(name="double_list_feature", dtype=ValueType.DOUBLE_LIST),
            Feature(name="float_list_feature", dtype=ValueType.FLOAT_LIST),
            Feature(name="int64_list_feature", dtype=ValueType.INT64_LIST),
            Feature(name="int32_list_feature", dtype=ValueType.INT32_LIST),
            Feature(name="string_list_feature", dtype=ValueType.STRING_LIST),
            Feature(name="bytes_list_feature", dtype=ValueType.BYTES_LIST),
            Feature(name="bool_list_feature", dtype=ValueType.BOOL_LIST),
        ],
        max_age=Duration(seconds=3600),
        batch_source=batch_source,
        labels={"cat": "alltypes"},
    )


def test_get_list_basic(
    feast_client: Client,
    customer_entity: Entity,
    driver_entity: Entity,
    basic_featuretable: FeatureTable,
):

    # ApplyEntity
    feast_client.apply(customer_entity)
    feast_client.apply(driver_entity)

    # GetEntity Check
    assert feast_client.get_entity(name="customer_id") == customer_entity
    assert feast_client.get_entity(name="driver_id") == driver_entity

    # ListEntities Check
    common_filtering_labels = {"common_key": "common_val"}
    matchmaking_filtering_labels = {"team": "matchmaking"}

    actual_common_entities = feast_client.list_entities(labels=common_filtering_labels)
    actual_matchmaking_entities = feast_client.list_entities(
        labels=matchmaking_filtering_labels
    )
    assert len(actual_common_entities) == 2
    assert len(actual_matchmaking_entities) == 1

    # ApplyFeatureTable
    feast_client.apply(basic_featuretable)

    # GetFeatureTable Check
    actual_get_feature_table = feast_client.get_feature_table(name="basic_featuretable")
    assert actual_get_feature_table == basic_featuretable

    # ListFeatureTables Check
    actual_list_feature_table = [
        ft
        for ft in feast_client.list_feature_tables()
        if ft.name == "basic_featuretable"
    ][0]
    assert actual_list_feature_table == basic_featuretable


def test_get_list_alltypes(
    feast_client: Client, alltypes_entity: Entity, alltypes_featuretable: FeatureTable
):
    # ApplyEntity
    feast_client.apply(alltypes_entity)

    # GetEntity Check
    assert feast_client.get_entity(name="alltypes_id") == alltypes_entity

    # ListEntities Check
    alltypes_filtering_labels = {"cat": "alltypes"}
    actual_alltypes_entities = feast_client.list_entities(
        labels=alltypes_filtering_labels
    )
    assert len(actual_alltypes_entities) == 1

    # ApplyFeatureTable
    feast_client.apply(alltypes_featuretable)

    # GetFeatureTable Check
    actual_get_feature_table = feast_client.get_feature_table(name="alltypes")
    assert actual_get_feature_table == alltypes_featuretable

    # ListFeatureTables Check
    actual_list_feature_table = [
        ft for ft in feast_client.list_feature_tables() if ft.name == "alltypes"
    ][0]
    assert actual_list_feature_table == alltypes_featuretable


@pytest.mark.env("gcloud")
def test_ingest_into_bq(
    feast_client: Client,
    customer_entity: Entity,
    driver_entity: Entity,
    bq_dataframe: pd.DataFrame,
    bq_dataset: str,
    pytestconfig,
):
    bq_project = pytestconfig.getoption("bq_project")
    bq_table_id = f"bq_staging_{datetime.now():%Y%m%d%H%M%s}"
    ft = FeatureTable(
        name="basic_featuretable",
        entities=["driver_id", "customer_id"],
        features=[
            Feature(name="dev_feature_float", dtype=ValueType.FLOAT),
            Feature(name="dev_feature_string", dtype=ValueType.STRING),
        ],
        max_age=Duration(seconds=3600),
        batch_source=BigQuerySource(
            table_ref=f"{bq_project}:{bq_dataset}.{bq_table_id}",
            event_timestamp_column="datetime",
            created_timestamp_column="timestamp",
        ),
    )

    # ApplyEntity
    feast_client.apply(customer_entity)
    feast_client.apply(driver_entity)

    # ApplyFeatureTable
    feast_client.apply(ft)
    feast_client.ingest(ft, bq_dataframe, timeout=120)

    bq_client = bigquery.Client(project=bq_project)

    # Poll BQ for table until the table has been created
    def try_get_table():
        try:
            table = bq_client.get_table(
                bigquery.TableReference(
                    bigquery.DatasetReference(bq_project, bq_dataset), bq_table_id
                )
            )
        except NotFound:
            return None, False
        else:
            return table, True

    wait_retry_backoff(
        retry_fn=try_get_table,
        timeout_secs=30,
        timeout_msg="Timed out trying to get bigquery table",
    )

    query_string = f"SELECT * FROM `{bq_project}.{bq_dataset}.{bq_table_id}`"

    job = bq_client.query(query_string)
    query_df = job.to_dataframe()

    assert_frame_equal(query_df, bq_dataframe)
