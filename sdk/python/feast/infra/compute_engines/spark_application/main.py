import logging
import sys

from feast.infra.compute_engines.operator.launcher import (
    load_config,
    run_materialization,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("feast.spark_application.driver")


def _bind_spark_session_to_thread(spark):
    """Register *spark* as the active session for this OS thread.

    spark-submit creates the SparkSession on the main thread.  PySpark's
    ``getActiveSession()`` is thread-local, so worker threads see ``None``.
    Constructing a ``SparkSession`` from the same SparkContext + JVM session
    calls Java ``setActiveSession`` for *this* thread — no classmethod
    monkey-patching required.
    """
    from pyspark.sql import SparkSession

    SparkSession(spark.sparkContext, spark._jsparkSession)


def main():
    feast_config, mat_config = load_config()

    from pyspark.sql import SparkSession

    # Get (or create) the SparkSession that spark-submit started.
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    logger.info(f"SparkSession: {spark.sparkContext.applicationId}")

    run_materialization(
        feast_config,
        mat_config,
        on_thread_start=lambda: _bind_spark_session_to_thread(spark),
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Driver failed")
        sys.exit(1)
