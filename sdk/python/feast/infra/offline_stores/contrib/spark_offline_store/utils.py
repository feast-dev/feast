import logging
from typing import Dict, Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _normalise_host(host: str) -> str:
    if host.startswith("https://"):
        return host[8:]
    if host.startswith("http://"):
        return host[7:]
    return host


def _expected_connect_uri(host: str) -> str:
    return f"sc://{_normalise_host(host)}:443/"


def get_databricks_connect_session(
    host: Optional[str] = None,
    token: Optional[str] = None,
    cluster_id: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    spark_conf: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """Create (or reuse) a Databricks Connect Spark session.

    Args:
        host: Databricks workspace host (e.g. ``adb-xxxx.azuredatabricks.net``).
        token: Databricks PAT.
        cluster_id: Databricks cluster ID for the remote session.
        catalog: Default Unity Catalog to use after connecting.
        schema: Default schema to use after connecting.
        spark_conf: Additional Spark configuration.

    Returns:
        A Spark session connected to the Databricks workspace.
    """
    spark_session = SparkSession.getActiveSession()
    if spark_session and host:
        try:
            current_uri = spark_session.conf.get("spark.connect.client.uri", "")
            expected_uri = _expected_connect_uri(host)
            if expected_uri not in current_uri:
                logger.info(
                    "Active Spark session URI %r does not match requested host %s "
                    "(expected %r). Stopping and recreating.",
                    current_uri,
                    host,
                    expected_uri,
                )
                spark_session.stop()
                spark_session = None
        except Exception:
            spark_session = None

    if not spark_session:
        if host and cluster_id:
            conn_str = _expected_connect_uri(host)
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
