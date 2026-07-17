import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("feast.spark_application.driver")


def _load_config_from_configmap():
    """Load feast and materialization config from a Kubernetes ConfigMap."""
    from kubernetes import client
    from kubernetes import config as k8s_config

    k8s_config.load_incluster_config()
    v1 = client.CoreV1Api()
    cm = v1.read_namespaced_config_map(
        name=os.environ["FEAST_CONFIGMAP_NAME"],
        namespace=os.environ["FEAST_CONFIGMAP_NAMESPACE"],
    )
    feast_config = yaml.safe_load(cm.data["feature_store.yaml"])
    mat_config = yaml.safe_load(cm.data["materialization_config.yaml"])
    return feast_config, mat_config


def _load_config_from_files():
    """Load feast and materialization config from mounted files."""
    with open("/var/feast/feature_store.yaml") as f:
        feast_config = yaml.safe_load(f)
    with open("/var/feast/materialization_config.yaml") as f:
        mat_config = yaml.safe_load(f)
    return feast_config, mat_config


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


def _materialize_one_fv(spark_session, config, task_info):
    """Materialize a single feature view in a worker thread.

    Each thread gets its own FeatureStore instance to avoid race conditions
    in Feast's usage.py call_stack (not thread-safe).
    """
    from datetime import datetime, timezone

    from tqdm import tqdm

    from feast import FeatureStore, RepoConfig

    _bind_spark_session_to_thread(spark_session)

    fv_name = task_info["feature_view"]
    logger.info(f"Thread started: {fv_name}")

    # Per-thread FeatureStore avoids race in feast/usage.py call_stack
    thread_config = RepoConfig(**config)
    thread_store = FeatureStore(config=thread_config)
    fv = thread_store.get_feature_view(fv_name)
    provider = thread_store.provider

    start = datetime.fromisoformat(task_info["start_time"])
    end = datetime.fromisoformat(task_info["end_time"])
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    t0 = time.time()
    provider.materialize_single_feature_view(
        config=thread_config,
        feature_view=fv,
        start_date=start,
        end_date=end,
        registry=thread_store.registry,
        project=thread_store.project,
        tqdm_builder=lambda length: tqdm(total=length, ncols=100),
    )

    thread_store.registry.apply_materialization(fv, thread_store.project, start, end)
    logger.info(f"Applied materialization metadata for {fv_name}")

    elapsed = time.time() - t0
    return fv_name, elapsed


def main():
    if os.environ.get("FEAST_CONFIGMAP_NAME"):
        logger.info("Loading config from ConfigMap via K8s API")
        feast_config, mat_config = _load_config_from_configmap()
    elif os.path.exists("/var/feast/feature_store.yaml"):
        logger.info("Loading config from mounted files")
        feast_config, mat_config = _load_config_from_files()
    else:
        raise RuntimeError(
            "No config source found. Set FEAST_CONFIGMAP_NAME env var "
            "or mount config at /var/feast/"
        )

    from pyspark.sql import SparkSession

    from feast import RepoConfig

    RepoConfig(**feast_config)  # validate config eagerly before any Spark work
    operation = mat_config["operation"]

    if operation == "materialize":
        tasks = mat_config.get("tasks", [])
        if not tasks:
            tasks = [
                {
                    "feature_view": mat_config["feature_view"],
                    "start_time": mat_config["start_time"],
                    "end_time": mat_config["end_time"],
                }
            ]

        concurrency = mat_config.get("concurrency", 1)
        total = len(tasks)
        total_start = time.time()

        logger.info(
            f"Starting materialization: {total} feature views, "
            f"concurrency={concurrency}"
        )

        # Get the active SparkSession (created by spark-submit)
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        logger.info(f"SparkSession: {spark.sparkContext.applicationId}")

        succeeded, failed = 0, 0
        if concurrency <= 1:
            for i, task in enumerate(tasks, 1):
                fv_name = task["feature_view"]
                logger.info(f"[{i}/{total}] Materializing: {fv_name}")
                try:
                    name, elapsed = _materialize_one_fv(spark, feast_config, task)
                    succeeded += 1
                    logger.info(f"[{i}/{total}] Completed: {name} ({elapsed:.1f}s)")
                except Exception:
                    failed += 1
                    logger.exception(f"[{i}/{total}] Failed: {fv_name}")
        else:
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                future_to_fv = {
                    executor.submit(
                        _materialize_one_fv, spark, feast_config, task
                    ): task["feature_view"]
                    for task in tasks
                }
                for future in as_completed(future_to_fv):
                    fv_name = future_to_fv[future]
                    try:
                        name, elapsed = future.result()
                        succeeded += 1
                        logger.info(f"Completed: {name} ({elapsed:.1f}s)")
                    except Exception:
                        failed += 1
                        logger.exception(f"Failed: {fv_name}")

        total_elapsed = time.time() - total_start
        level = logging.ERROR if failed > 0 else logging.INFO
        logger.log(
            level,
            f"Materialization batch complete: "
            f"{succeeded} succeeded, {failed} failed, {total} total, "
            f"elapsed={total_elapsed:.1f}s",
        )
        if failed > 0:
            sys.exit(1)
    else:
        raise ValueError(f"Unknown operation: {operation}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Driver failed")
        sys.exit(1)
