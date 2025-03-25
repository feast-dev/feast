from typing import Optional, Dict

from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_or_create_new_spark_session(
        spark_config: Optional[Dict[str, str]] = None
) -> SparkSession:
    spark_session = SparkSession.getActiveSession()
    if not spark_session:
        spark_builder = SparkSession.builder
        if spark_config:
            spark_builder = spark_builder.config(
                conf=SparkConf().setAll([(k, v) for k, v in spark_config.items()])
            )

        spark_session = spark_builder.getOrCreate()
    return spark_session
