import os
import shutil
import socket
import tempfile
from concurrent import futures
from contextlib import closing
from datetime import datetime
from typing import List, Tuple

import grpc
import pytest
from google.protobuf.duration_pb2 import Duration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from feast import Client, Entity, Feature, FeatureTable, FileSource, ValueType
from feast.core import CoreService_pb2_grpc as Core
from tests.feast_core_server import CoreServicer


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


free_port = find_free_port()


def assert_dataframe_equal(left: DataFrame, right: DataFrame):
    is_column_equal = set(left.columns) == set(right.columns)

    if not is_column_equal:
        print(f"Column not equal. Left: {left.columns}, Right: {right.columns}")
    assert is_column_equal

    is_content_equal = (
        left.exceptAll(right).count() == 0 and right.exceptAll(left).count() == 0
    )
    if not is_content_equal:
        print("Rows are different.")
        print("Left:")
        left.show()
        print("Right:")
        right.show()

    assert is_content_equal


@pytest.yield_fixture(scope="module")
def spark():
    spark_session = (
        SparkSession.builder.appName("Historical Feature Retrieval Test")
        .master("local")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="module")
def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
    server.add_insecure_port(f"[::]:{free_port}")
    server.start()
    yield server
    server.stop(0)


@pytest.fixture(scope="module")
def client(server):
    return Client(core_url=f"localhost:{free_port}")


@pytest.fixture(scope="module")
def driver_entity(client):
    return client.apply_entity(Entity("driver_id", "description", ValueType.INT32))


@pytest.fixture(scope="module")
def customer_entity(client):
    return client.apply_entity(Entity("customer_id", "description", ValueType.INT32))


def create_temp_parquet_file(
    spark: SparkSession, filename, schema: StructType, data: List[Tuple]
) -> Tuple[str, str]:
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, filename)
    df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.write.parquet(file_path)
    return temp_dir, f"file://{file_path}"


@pytest.fixture(scope="module")
def transactions_feature_table(spark, client):
    schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("total_transactions", DoubleType()),
        ]
    )
    df_data = [
        (
            1001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            50.0,
        ),
        (
            1001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=2),
            100.0,
        ),
        (
            2001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            400.0,
        ),
        (
            1001,
            datetime(year=2020, month=9, day=2),
            datetime(year=2020, month=9, day=1),
            200.0,
        ),
        (
            1001,
            datetime(year=2020, month=9, day=4),
            datetime(year=2020, month=9, day=1),
            300.0,
        ),
    ]
    temp_dir, file_uri = create_temp_parquet_file(
        spark, "transactions", schema, df_data
    )
    file_source = FileSource(
        "event_timestamp", "created_timestamp", "parquet", file_uri
    )
    features = [Feature("total_transactions", ValueType.DOUBLE)]
    feature_table = FeatureTable(
        "transactions", ["customer_id"], features, batch_source=file_source
    )
    yield client.apply_feature_table(feature_table)
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="module")
def bookings_feature_table(spark, client):
    schema = StructType(
        [
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("total_completed_bookings", IntegerType()),
        ]
    )
    df_data = [
        (
            8001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            100,
        ),
        (
            8001,
            datetime(year=2020, month=9, day=2),
            datetime(year=2020, month=9, day=2),
            150,
        ),
        (
            8002,
            datetime(year=2020, month=9, day=2),
            datetime(year=2020, month=9, day=2),
            200,
        ),
    ]
    temp_dir, file_uri = create_temp_parquet_file(spark, "bookings", schema, df_data)

    file_source = FileSource(
        "event_timestamp", "created_timestamp", "parquet", file_uri
    )
    features = [Feature("total_completed_bookings", ValueType.INT32)]
    max_age = Duration()
    max_age.FromSeconds(86400)
    feature_table = FeatureTable(
        "bookings", ["driver_id"], features, batch_source=file_source, max_age=max_age
    )
    yield client.apply_feature_table(feature_table)
    shutil.rmtree(temp_dir)


def test_historical_feature_retrieval_from_local_spark_session(
    spark,
    client,
    driver_entity,
    customer_entity,
    bookings_feature_table,
    transactions_feature_table,
):
    schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
        ]
    )
    df_data = [
        (1001, 8001, datetime(year=2020, month=9, day=1),),
        (2001, 8001, datetime(year=2020, month=9, day=2),),
        (2001, 8002, datetime(year=2020, month=9, day=1),),
        (1001, 8001, datetime(year=2020, month=9, day=2),),
        (1001, 8001, datetime(year=2020, month=9, day=3),),
        (1001, 8001, datetime(year=2020, month=9, day=4),),
    ]
    temp_dir, file_uri = create_temp_parquet_file(
        spark, "customer_driver_pair", schema, df_data
    )
    entity_source = FileSource(
        "event_timestamp", "created_timestamp", "parquet", file_uri
    )
    joined_df = client.get_historical_features_df(
        ["transactions:total_transactions", "bookings:total_completed_bookings"],
        entity_source,
    )
    expected_joined_df_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("transactions__total_transactions", DoubleType()),
            StructField("bookings__total_completed_bookings", IntegerType()),
        ]
    )
    expected_joined_df_data = [
        (1001, 8001, datetime(year=2020, month=9, day=1), 100.0, 100),
        (2001, 8001, datetime(year=2020, month=9, day=2), 400.0, 150),
        (2001, 8002, datetime(year=2020, month=9, day=1), 400.0, None),
        (1001, 8001, datetime(year=2020, month=9, day=2), 200.0, 150),
        (1001, 8001, datetime(year=2020, month=9, day=3), 200.0, 150),
        (1001, 8001, datetime(year=2020, month=9, day=4), 300.0, None),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_df_data),
        expected_joined_df_schema,
    )
    assert_dataframe_equal(joined_df, expected_joined_df)
    shutil.rmtree(temp_dir)
