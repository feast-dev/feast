import logging
from typing import Dict, Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_databricks_connect_session(
    host: Optional[str] = None,
    token: Optional[str] = None,
    cluster_id: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    spark_conf: Optional[Dict[str, str]] = None,
) -> SparkSession:
    spark_session = SparkSession.getActiveSession()
    if not spark_session:
        if host:
            if host.startswith("https://"):
                host = host[8:]
            elif host.startswith("http://"):
                host = host[7:]

        if host and cluster_id:
            conn_str = f"sc://{host}:443/"
            params = []
            if token:
                params.append(f"token={token}")
            params.append(f"x-databricks-cluster-id={cluster_id}")
            if params:
                conn_str = f"{conn_str};{';'.join(params)}"

            try:
                from databricks.connect import DatabricksSession

                builder = DatabricksSession.builder.remote(conn_str)
            except ImportError:
                builder = SparkSession.builder.remote(conn_str)
        else:
            try:
                from databricks.connect import DatabricksSession

                builder = DatabricksSession.builder
            except ImportError:
                builder = SparkSession.builder

        if spark_conf:
            builder = builder.config(
                conf=SparkConf().setAll([(k, v) for k, v in spark_conf.items()])
            )

        spark_session = builder.getOrCreate()

    assert spark_session is not None

    spark_session.conf.set("spark.sql.parser.quotedRegexColumnNames", "true")

    if catalog:
        spark_session.sql(f"USE CATALOG `{catalog}`")
    if schema:
        spark_session.sql(f"USE SCHEMA `{schema}`")

    return spark_session
