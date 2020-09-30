from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from feast.pyspark.batch_retrieval_job import as_of_join, join_entity_to_feature_tables


@pytest.yield_fixture(scope="module")
def spark(pytestconfig):
    spark_session = (
        SparkSession.builder.appName("Batch Retrieval Test")
        .master("local")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture
def single_entity_schema():
    return StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
        ]
    )


@pytest.fixture
def composite_entity_schema():
    return StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
        ]
    )


@pytest.fixture
def customer_feature_schema():
    return StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("daily_transactions", FloatType()),
        ]
    )


@pytest.fixture
def driver_feature_schema():
    return StructType(
        [
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("completed_bookings", IntegerType()),
        ]
    )


@pytest.fixture
def rating_feature_schema():
    return StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("customer_rating", FloatType()),
            StructField("driver_rating", FloatType()),
        ]
    )


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


def test_join_without_max_age(
    spark: SparkSession,
    single_entity_schema: StructType,
    customer_feature_schema: StructType,
):
    entity_data = [
        (1001, datetime(year=2020, month=8, day=31)),
        (1001, datetime(year=2020, month=9, day=1)),
        (1001, datetime(year=2020, month=9, day=2)),
        (1001, datetime(year=2020, month=9, day=3)),
        (2001, datetime(year=2020, month=9, day=2)),
        (3001, datetime(year=2020, month=9, day=1)),
    ]
    entity_df = spark.createDataFrame(
        spark.sparkContext.parallelize(entity_data), single_entity_schema
    )

    feature_table_data = [
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
    feature_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(feature_table_data), customer_feature_schema
    )

    joined_df = as_of_join(
        entity_df,
        ["customer_id"],
        feature_table_df,
        ["daily_transactions"],
        feature_prefix="transactions__",
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("transactions__daily_transactions", FloatType()),
        ]
    )
    expected_joined_data = [
        (1001, datetime(year=2020, month=8, day=31), None),
        (1001, datetime(year=2020, month=9, day=1), 100.0,),
        (1001, datetime(year=2020, month=9, day=2), 200.0,),
        (1001, datetime(year=2020, month=9, day=3), 200.0,),
        (2001, datetime(year=2020, month=9, day=2), 400.0,),
        (3001, datetime(year=2020, month=9, day=1), None),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)


def test_join_with_max_age(
    spark: SparkSession,
    single_entity_schema: StructType,
    customer_feature_schema: StructType,
):
    entity_data = [
        (1001, datetime(year=2020, month=9, day=1)),
        (1001, datetime(year=2020, month=9, day=3)),
        (2001, datetime(year=2020, month=9, day=2)),
    ]
    entity_df = spark.createDataFrame(
        spark.sparkContext.parallelize(entity_data), single_entity_schema
    )

    feature_table_data = [
        (
            1001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            100.0,
        ),
        (
            2001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            200.0,
        ),
    ]
    feature_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(feature_table_data), customer_feature_schema
    )

    joined_df = as_of_join(
        entity_df,
        ["customer_id"],
        feature_table_df,
        ["daily_transactions"],
        feature_prefix="transactions__",
        max_age="1 day",
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("transactions__daily_transactions", FloatType()),
        ]
    )
    expected_joined_data = [
        (1001, datetime(year=2020, month=9, day=1), 100.0,),
        (1001, datetime(year=2020, month=9, day=3), None),
        (2001, datetime(year=2020, month=9, day=2), 200.0,),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)


def test_join_with_composite_entity(
    spark: SparkSession,
    composite_entity_schema: StructType,
    rating_feature_schema: StructType,
):
    entity_data = [
        (1001, 8001, datetime(year=2020, month=9, day=1)),
        (1001, 8002, datetime(year=2020, month=9, day=3)),
        (1001, 8003, datetime(year=2020, month=9, day=1)),
        (2001, 8001, datetime(year=2020, month=9, day=2)),
    ]
    entity_df = spark.createDataFrame(
        spark.sparkContext.parallelize(entity_data), composite_entity_schema
    )

    feature_table_data = [
        (
            1001,
            8001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            3.0,
            5.0,
        ),
        (
            1001,
            8002,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            4.0,
            3.0,
        ),
        (
            2001,
            8001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            4.0,
            4.5,
        ),
    ]
    feature_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(feature_table_data), rating_feature_schema,
    )

    joined_df = as_of_join(
        entity_df,
        ["customer_id", "driver_id"],
        feature_table_df,
        ["customer_rating", "driver_rating"],
        feature_prefix="ratings__",
        max_age="1 day",
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("ratings__customer_rating", FloatType()),
            StructField("ratings__driver_rating", FloatType()),
        ]
    )
    expected_joined_data = [
        (1001, 8001, datetime(year=2020, month=9, day=1), 3.0, 5.0,),
        (1001, 8002, datetime(year=2020, month=9, day=3), None, None),
        (1001, 8003, datetime(year=2020, month=9, day=1), None, None),
        (2001, 8001, datetime(year=2020, month=9, day=2), 4.0, 4.5,),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)


def test_entity_filter(
    spark: SparkSession,
    composite_entity_schema: StructType,
    customer_feature_schema: StructType,
):
    entity_data = [
        (1001, 8001, datetime(year=2020, month=9, day=2)),
        (2001, 8002, datetime(year=2020, month=9, day=2)),
    ]
    entity_df = spark.createDataFrame(
        spark.sparkContext.parallelize(entity_data), composite_entity_schema
    )

    feature_table_data = [
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
    ]
    feature_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(feature_table_data), customer_feature_schema
    )

    joined_df = as_of_join(
        entity_df,
        ["customer_id"],
        feature_table_df,
        ["daily_transactions"],
        feature_prefix="transactions__",
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("transactions__daily_transactions", FloatType()),
        ]
    )
    expected_joined_data = [
        (1001, 8001, datetime(year=2020, month=9, day=2), 100.0,),
        (2001, 8002, datetime(year=2020, month=9, day=2), 400.0,),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)


def test_multiple_join(
    spark: SparkSession,
    composite_entity_schema: StructType,
    customer_feature_schema: StructType,
    driver_feature_schema: StructType,
):
    query_conf = [
        {
            "table": "transactions",
            "features": ["daily_transactions"],
            "join": ["customer_id"],
            "max_age": "1 day",
        },
        {
            "table": "bookings",
            "features": ["completed_bookings"],
            "join": ["driver_id"],
        },
    ]

    entity_data = [
        (1001, 8001, datetime(year=2020, month=9, day=2)),
        (1001, 8002, datetime(year=2020, month=9, day=2)),
        (2001, 8002, datetime(year=2020, month=9, day=3)),
    ]
    entity_df = spark.createDataFrame(
        spark.sparkContext.parallelize(entity_data), composite_entity_schema
    )

    customer_table_data = [
        (
            1001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            100.0,
        ),
        (
            2001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            200.0,
        ),
    ]
    customer_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(customer_table_data), customer_feature_schema
    )

    driver_table_data = [
        (
            8001,
            datetime(year=2020, month=8, day=31),
            datetime(year=2020, month=8, day=31),
            200,
        ),
        (
            8001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            300,
        ),
        (
            8002,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            600,
        ),
        (
            8002,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=2),
            500,
        ),
    ]
    driver_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(driver_table_data), driver_feature_schema
    )

    tables = {"transactions": customer_table_df, "bookings": driver_table_df}

    joined_df = join_entity_to_feature_tables(query_conf, entity_df, tables)

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("transactions__daily_transactions", FloatType()),
            StructField("bookings__completed_bookings", IntegerType()),
        ]
    )

    expected_joined_data = [
        (1001, 8001, datetime(year=2020, month=9, day=2), 100.0, 300,),
        (1001, 8002, datetime(year=2020, month=9, day=2), 100.0, 500,),
        (2001, 8002, datetime(year=2020, month=9, day=3), None, 500,),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)
