import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from unittest.mock import patch
from pyspark.testing.utils import assertDataFrameEqual

from feast.transformation.spark_transformation import SparkTransformation
from feast.transformation.mode import TransformationMode
from feast.transformation.base import Transformation


def get_sample_df(spark):
    sample_data = [{"name": "John    D.", "age": 30},
                   {"name": "Alice   G.", "age": 25},
                   {"name": "Bob  T.", "age": 35},
                   {"name": "Eve   A.", "age": 28}]
    df = spark.createDataFrame(sample_data)
    return df


def get_expected_df(spark):
    expected_data = [{"name": "John D.", "age": 30},
                     {"name": "Alice G.", "age": 25},
                     {"name": "Bob T.", "age": 35},
                     {"name": "Eve A.", "age": 28}]

    expected_df = spark.createDataFrame(expected_data)
    return expected_df


def remove_extra_spaces(df,
                        column_name):
    df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))
    return df_transformed


def remove_extra_spaces_sql(df,
                            column_name):
    sql = f"""
    SELECT
        age,
        regexp_replace({column_name}, '\\s+', ' ') as {column_name}
    FROM {df}
    """
    return sql


@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark


@patch(
    "feast.infra.compute_engines.spark.utils.get_or_create_new_spark_session"
)
def test_spark_transformation(spark_fixture):
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    df = get_sample_df(spark)

    spark_transformation = Transformation(
        mode=TransformationMode.SPARK,
        udf=remove_extra_spaces,
        udf_string="remove extra spaces",
    )

    transformed_df = spark_transformation.transform(df, "name")
    expected_df = get_expected_df(spark)
    assertDataFrameEqual(transformed_df, expected_df)


@patch(
    "feast.infra.compute_engines.spark.utils.get_or_create_new_spark_session"
)
def test_spark_transformation_init_transformation(spark_fixture):
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    df = get_sample_df(spark)

    spark_transformation = SparkTransformation(
        mode=TransformationMode.SPARK,
        udf=remove_extra_spaces,
        udf_string="remove extra spaces",
    )

    transformed_df = spark_transformation.transform(df, "name")
    expected_df = get_expected_df(spark)
    assertDataFrameEqual(transformed_df, expected_df)


@patch(
    "feast.infra.compute_engines.spark.utils.get_or_create_new_spark_session"
)
def test_spark_transformation_sql(spark_fixture):
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    df = get_sample_df(spark)

    spark_transformation = SparkTransformation(
        mode=TransformationMode.SPARK_SQL,
        udf=remove_extra_spaces_sql,
        udf_string="remove extra spaces",
    )

    transformed_df = spark_transformation.transform(df, "name")
    expected_df = get_expected_df(spark)
    assertDataFrameEqual(transformed_df, expected_df)
