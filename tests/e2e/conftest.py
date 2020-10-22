import os
from pathlib import Path

import pyspark
import pytest

from feast import Client


def pytest_addoption(parser):
    parser.addoption("--core_url", action="store", default="localhost:6565")
    parser.addoption("--serving_url", action="store", default="localhost:6566")
    parser.addoption("--allow_dirty", action="store", default="False")
    parser.addoption(
        "--gcs_path", action="store", default="gs://feast-templocation-kf-feast/"
    )
    parser.addoption("--enable_auth", action="store", default="False")
    parser.addoption("--kafka_brokers", action="store", default="localhost:9092")

    parser.addoption("--env", action="store", help="local|aws|gcloud", default="local")
    parser.addoption(
        "--staging-path", action="store", default="gs://feast-templocation-kf-feast/"
    )
    parser.addoption("--dataproc-cluster-name", action="store")
    parser.addoption("--dataproc-region", action="store")
    parser.addoption("--dataproc-project", action="store")
    parser.addoption("--ingestion-jar", action="store")
    parser.addoption("--redis-url", action="store", default="localhost:6379")


def pytest_runtest_makereport(item, call):
    if "incremental" in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            parent._previousfailed = item


def pytest_runtest_setup(item):
    if "incremental" in item.keywords:
        previousfailed = getattr(item.parent, "_previousfailed", None)
        if previousfailed is not None:
            pytest.xfail("previous test failed (%s)" % previousfailed.name)


@pytest.fixture(scope="session")
def feast_version():
    return "0.8-SNAPSHOT"


@pytest.fixture(scope="session")
def ingestion_job_jar(pytestconfig, feast_version):
    default_path = (
        Path(__file__).parent.parent.parent
        / "spark"
        / "ingestion"
        / "target"
        / f"feast-ingestion-spark-{feast_version}.jar"
    )

    return pytestconfig.getoption("ingestion_jar") or f"file://{default_path}"


@pytest.fixture(scope="session")
def feast_client(pytestconfig, ingestion_job_jar):
    redis_host, redis_port = pytestconfig.getoption("redis_url").split(":")

    if pytestconfig.getoption("env") == "local":
        return Client(
            core_url=pytestconfig.getoption("core_url"),
            serving_url=pytestconfig.getoption("serving_url"),
            spark_launcher="standalone",
            spark_standalone_master="local",
            spark_home=os.getenv("SPARK_HOME") or os.path.dirname(pyspark.__file__),
            spark_ingestion_jar=ingestion_job_jar,
            redis_host=redis_host,
            redis_port=redis_port,
        )

    if pytestconfig.getoption("env") == "gcloud":
        return Client(
            core_url=pytestconfig.getoption("core_url"),
            serving_url=pytestconfig.getoption("serving_url"),
            spark_launcher="dataproc",
            dataproc_cluster_name=pytestconfig.getoption("dataproc_cluster_name"),
            dataproc_project=pytestconfig.getoption("dataproc_project"),
            dataproc_region=pytestconfig.getoption("dataproc_region"),
            dataproc_staging_location=os.path.join(
                pytestconfig.getoption("staging_path"), "dataproc"
            ),
            spark_ingestion_jar=ingestion_job_jar,
        )
