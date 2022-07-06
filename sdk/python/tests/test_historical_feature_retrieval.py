import os
import shutil
import socket
import tempfile
from concurrent import futures
from contextlib import closing
from datetime import datetime
from typing import List, Tuple
from urllib.parse import urlparse

import grpc
import numpy as np
import pandas as pd
import pytest
from google.protobuf.duration_pb2 import Duration
from pandas.util.testing import assert_frame_equal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)
from pytz import utc

from feast import Client, Entity, Feature, FeatureTable, FileSource, ValueType
from feast.core import CoreService_pb2_grpc as Core
from feast.data_format import ParquetFormat
from feast.pyspark.abc import SparkJobStatus
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


@pytest.fixture()
def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
    server.add_insecure_port(f"[::]:{free_port}")
    server.start()
    yield server
    server.stop(0)


@pytest.fixture()
def client(server):
    return Client(core_url=f"localhost:{free_port}")


@pytest.yield_fixture()
def client_with_local_spark(tmpdir):
    import pyspark

    spark_staging_location = f"file://{os.path.join(tmpdir, 'staging')}"
    historical_feature_output_location = (
        f"file://{os.path.join(tmpdir, 'historical_feature_retrieval_output')}"
    )

    return Client(
        core_url=f"localhost:{free_port}",
        spark_launcher="standalone",
        spark_standalone_master="local",
        spark_home=os.path.dirname(pyspark.__file__),
        spark_staging_location=spark_staging_location,
        historical_feature_output_location=historical_feature_output_location,
        historical_feature_output_format="parquet",
    )


@pytest.fixture()
def client_with_tfrecord_output(tmpdir):
    import pyspark

    spark_staging_location = f"file://{os.path.join(tmpdir, 'staging')}"
    historical_feature_output_location = (
        f"file://{os.path.join(tmpdir, 'historical_feature_retrieval_tfrecord_output')}"
    )

    return Client(
        core_url=f"localhost:{free_port}",
        spark_launcher="standalone",
        spark_standalone_master="local",
        spark_home=os.path.dirname(pyspark.__file__),
        spark_staging_location=spark_staging_location,
        historical_feature_output_location=historical_feature_output_location,
        historical_feature_output_format="tfrecord",
    )


@pytest.fixture()
def driver_entity(client):
    return client.apply(Entity("driver_id", "description", ValueType.INT32))


@pytest.fixture()
def customer_entity(client):
    return client.apply(Entity("customer_id", "description", ValueType.INT32))


def create_temp_parquet_file(
    spark: SparkSession, filename, schema: StructType, data: List[Tuple]
) -> Tuple[str, str]:
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, filename)
    df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.write.parquet(file_path)
    return temp_dir, f"file://{file_path}"


@pytest.fixture()
def transactions_feature_table(spark, client):
    schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("total_transactions", DoubleType()),
            StructField("is_vip", BooleanType()),
        ]
    )
    df_data = [
        (
            1001,
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            50.0,
            True,
        ),
        (
            1001,
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            datetime(year=2020, month=9, day=2, tzinfo=utc),
            100.0,
            True,
        ),
        (
            2001,
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            400.0,
            False,
        ),
        (
            1001,
            datetime(year=2020, month=9, day=2, tzinfo=utc),
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            200.0,
            False,
        ),
        (
            1001,
            datetime(year=2020, month=9, day=4, tzinfo=utc),
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            300.0,
            False,
        ),
    ]
    temp_dir, file_uri = create_temp_parquet_file(
        spark, "transactions", schema, df_data
    )
    file_source = FileSource(
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created_timestamp",
        file_format=ParquetFormat(),
        file_url=file_uri,
    )
    features = [
        Feature("total_transactions", ValueType.DOUBLE),
        Feature("is_vip", ValueType.BOOL),
    ]
    feature_table = FeatureTable(
        "transactions", ["customer_id"], features, batch_source=file_source
    )
    yield client.apply(feature_table)
    shutil.rmtree(temp_dir)


@pytest.fixture()
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
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            100,
        ),
        (
            8001,
            datetime(year=2020, month=9, day=2, tzinfo=utc),
            datetime(year=2020, month=9, day=2, tzinfo=utc),
            150,
        ),
        (
            8002,
            datetime(year=2020, month=9, day=2, tzinfo=utc),
            datetime(year=2020, month=9, day=2, tzinfo=utc),
            200,
        ),
    ]
    temp_dir, file_uri = create_temp_parquet_file(spark, "bookings", schema, df_data)

    file_source = FileSource(
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created_timestamp",
        file_format=ParquetFormat(),
        file_url=file_uri,
    )
    features = [Feature("total_completed_bookings", ValueType.INT32)]
    max_age = Duration()
    max_age.FromSeconds(86400)
    feature_table = FeatureTable(
        "bookings", ["driver_id"], features, batch_source=file_source, max_age=max_age
    )
    yield client.apply(feature_table)
    shutil.rmtree(temp_dir)


@pytest.fixture()
def bookings_feature_table_with_mapping(spark, client):
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("datetime", TimestampType()),
            StructField("created_datetime", TimestampType()),
            StructField("total_completed_bookings", IntegerType()),
        ]
    )
    df_data = [
        (
            8001,
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            datetime(year=2020, month=9, day=1, tzinfo=utc),
            100,
        ),
        (
            8001,
            datetime(year=2020, month=9, day=2, tzinfo=utc),
            datetime(year=2020, month=9, day=2, tzinfo=utc),
            150,
        ),
        (
            8002,
            datetime(year=2020, month=9, day=2, tzinfo=utc),
            datetime(year=2020, month=9, day=2, tzinfo=utc),
            200,
        ),
    ]
    temp_dir, file_uri = create_temp_parquet_file(spark, "bookings", schema, df_data)

    file_source = FileSource(
        event_timestamp_column="datetime",
        created_timestamp_column="created_datetime",
        file_format=ParquetFormat(),
        file_url=file_uri,
        field_mapping={"id": "driver_id"},
    )
    features = [Feature("total_completed_bookings", ValueType.INT32)]
    max_age = Duration()
    max_age.FromSeconds(86400)
    feature_table = FeatureTable(
        "bookings", ["driver_id"], features, batch_source=file_source, max_age=max_age
    )
    yield client.apply(feature_table)
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
        (1001, 8001, datetime(year=2020, month=9, day=1, tzinfo=utc)),
        (2001, 8001, datetime(year=2020, month=9, day=2, tzinfo=utc)),
        (2001, 8002, datetime(year=2020, month=9, day=1, tzinfo=utc)),
        (1001, 8001, datetime(year=2020, month=9, day=2, tzinfo=utc)),
        (1001, 8001, datetime(year=2020, month=9, day=3, tzinfo=utc)),
        (1001, 8001, datetime(year=2020, month=9, day=4, tzinfo=utc)),
    ]
    temp_dir, file_uri = create_temp_parquet_file(
        spark, "customer_driver_pair", schema, df_data
    )
    customer_driver_pairs_source = FileSource(
        event_timestamp_column="event_timestamp",
        file_format=ParquetFormat(),
        file_url=file_uri,
    )
    joined_df = client.get_historical_features_df(
        ["transactions:total_transactions", "bookings:total_completed_bookings"],
        customer_driver_pairs_source,
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
        (1001, 8001, datetime(year=2020, month=9, day=1, tzinfo=utc), 100.0, 100),
        (2001, 8001, datetime(year=2020, month=9, day=2, tzinfo=utc), 400.0, 150),
        (2001, 8002, datetime(year=2020, month=9, day=1, tzinfo=utc), 400.0, None),
        (1001, 8001, datetime(year=2020, month=9, day=2, tzinfo=utc), 200.0, 150),
        (1001, 8001, datetime(year=2020, month=9, day=3, tzinfo=utc), 200.0, 150),
        (1001, 8001, datetime(year=2020, month=9, day=4, tzinfo=utc), 300.0, None),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_df_data),
        expected_joined_df_schema,
    )
    assert_dataframe_equal(joined_df, expected_joined_df)
    shutil.rmtree(temp_dir)


def test_historical_feature_retrieval_with_field_mappings_from_local_spark_session(
    spark, client, driver_entity, bookings_feature_table_with_mapping,
):
    schema = StructType(
        [
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
        ]
    )
    df_data = [
        (8001, datetime(year=2020, month=9, day=1, tzinfo=utc)),
        (8001, datetime(year=2020, month=9, day=2, tzinfo=utc)),
        (8002, datetime(year=2020, month=9, day=1, tzinfo=utc)),
    ]
    temp_dir, file_uri = create_temp_parquet_file(spark, "drivers", schema, df_data)
    entity_source = FileSource(
        event_timestamp_column="event_timestamp",
        file_format=ParquetFormat(),
        file_url=file_uri,
    )
    joined_df = client.get_historical_features_df(
        ["bookings:total_completed_bookings"], entity_source,
    )
    expected_joined_df_schema = StructType(
        [
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("bookings__total_completed_bookings", IntegerType()),
        ]
    )
    expected_joined_df_data = [
        (8001, datetime(year=2020, month=9, day=1, tzinfo=utc), 100),
        (8001, datetime(year=2020, month=9, day=2, tzinfo=utc), 150),
        (8002, datetime(year=2020, month=9, day=1, tzinfo=utc), None),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_df_data),
        expected_joined_df_schema,
    )
    assert_dataframe_equal(joined_df, expected_joined_df)
    shutil.rmtree(temp_dir)


@pytest.mark.usefixtures(
    "driver_entity",
    "customer_entity",
    "bookings_feature_table",
    "transactions_feature_table",
)
def test_historical_feature_retrieval_with_pandas_dataframe_input(
    client_with_local_spark,
):

    customer_driver_pairs_pandas_df = pd.DataFrame(
        np.array(
            [
                [1001, 8001, datetime(year=2020, month=9, day=1, tzinfo=utc)],
                [2001, 8001, datetime(year=2020, month=9, day=2, tzinfo=utc)],
                [2001, 8002, datetime(year=2020, month=9, day=1, tzinfo=utc)],
                [1001, 8001, datetime(year=2020, month=9, day=2, tzinfo=utc)],
                [1001, 8001, datetime(year=2020, month=9, day=3, tzinfo=utc)],
                [1001, 8001, datetime(year=2020, month=9, day=4, tzinfo=utc)],
            ]
        ),
        columns=["customer_id", "driver_id", "event_timestamp"],
    )
    customer_driver_pairs_pandas_df = customer_driver_pairs_pandas_df.astype(
        {"customer_id": "int32", "driver_id": "int32"}
    )

    job_output = client_with_local_spark.get_historical_features(
        ["transactions:total_transactions", "bookings:total_completed_bookings"],
        customer_driver_pairs_pandas_df,
    )

    output_dir = job_output.get_output_file_uri()
    joined_df = pd.read_parquet(urlparse(output_dir).path)

    expected_joined_df = pd.DataFrame(
        np.array(
            [
                [1001, 8001, datetime(year=2020, month=9, day=1), 100.0, 100],
                [2001, 8001, datetime(year=2020, month=9, day=2), 400.0, 150],
                [2001, 8002, datetime(year=2020, month=9, day=1), 400.0, None],
                [1001, 8001, datetime(year=2020, month=9, day=2), 200.0, 150],
                [1001, 8001, datetime(year=2020, month=9, day=3), 200.0, 150],
                [1001, 8001, datetime(year=2020, month=9, day=4), 300.0, None],
            ]
        ),
        columns=[
            "customer_id",
            "driver_id",
            "event_timestamp",
            "transactions__total_transactions",
            "bookings__total_completed_bookings",
        ],
    )
    expected_joined_df = expected_joined_df.astype(
        {
            "customer_id": "int32",
            "driver_id": "int32",
            "transactions__total_transactions": "float64",
            "bookings__total_completed_bookings": "float64",
        }
    )

    assert_frame_equal(
        joined_df.sort_values(
            by=["customer_id", "driver_id", "event_timestamp"]
        ).reset_index(drop=True),
        expected_joined_df.sort_values(
            by=["customer_id", "driver_id", "event_timestamp"]
        ).reset_index(drop=True),
    )


@pytest.mark.usefixtures(
    "driver_entity",
    "customer_entity",
    "bookings_feature_table",
    "transactions_feature_table",
)
def test_historical_feature_retrieval_with_tfrecord_output(
    client_with_tfrecord_output,
):

    customer_driver_pairs_pandas_df = pd.DataFrame(
        np.array(
            [
                [1001, 8001, datetime(year=2020, month=9, day=1, tzinfo=utc)],
                [2001, 8001, datetime(year=2020, month=9, day=2, tzinfo=utc)],
                [2001, 8002, datetime(year=2020, month=9, day=1, tzinfo=utc)],
                [1001, 8001, datetime(year=2020, month=9, day=2, tzinfo=utc)],
                [1001, 8001, datetime(year=2020, month=9, day=3, tzinfo=utc)],
                [1001, 8001, datetime(year=2020, month=9, day=4, tzinfo=utc)],
            ]
        ),
        columns=["customer_id", "driver_id", "event_timestamp"],
    )
    customer_driver_pairs_pandas_df = customer_driver_pairs_pandas_df.astype(
        {"customer_id": "int32", "driver_id": "int32"}
    )

    job_output = client_with_tfrecord_output.get_historical_features(
        ["transactions:total_transactions", "bookings:total_completed_bookings"],
        customer_driver_pairs_pandas_df,
    )

    job_output.get_output_file_uri()
    assert job_output.get_status() == SparkJobStatus.COMPLETED
