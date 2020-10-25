import os
import tempfile
import uuid
from typing import Tuple

import pyspark
import pytest
from pytest_redis.executor import RedisExecutor

from feast import Client


@pytest.fixture
def feast_client(
    pytestconfig,
    ingestion_job_jar,
    redis_server: RedisExecutor,
    feast_core: Tuple[str, int],
    feast_serving: Tuple[str, int],
    local_staging_path,
):
    if pytestconfig.getoption("env") == "local":
        return Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            serving_url=f"{feast_serving[0]}:{feast_serving[1]}",
            spark_launcher="standalone",
            spark_standalone_master="local",
            spark_home=os.getenv("SPARK_HOME") or os.path.dirname(pyspark.__file__),
            spark_ingestion_jar=ingestion_job_jar,
            redis_host=redis_server.host,
            redis_port=redis_server.port,
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            ),
        )

    if pytestconfig.getoption("env") == "gcloud":
        return Client(
            core_url=f"{feast_core[0]}:{feast_core[1]}",
            serving_url=f"{feast_serving[0]}:{feast_serving[1]}",
            spark_launcher="dataproc",
            dataproc_cluster_name=pytestconfig.getoption("dataproc_cluster_name"),
            dataproc_project=pytestconfig.getoption("dataproc_project"),
            dataproc_region=pytestconfig.getoption("dataproc_region"),
            dataproc_staging_location=os.path.join(
                local_staging_path, "dataproc"
            ),
            spark_ingestion_jar=ingestion_job_jar,
            redis_host=pytestconfig.getoption("redis_url").split(":")[0],
            redis_port=pytestconfig.getoption("redis_url").split(":")[1],
            historical_feature_output_location=os.path.join(
                local_staging_path, "historical_output"
            )
        )


@pytest.fixture(scope="session")
def global_staging_path(pytestconfig):
    if pytestconfig.getoption("env") == "local":
        tmp_path = tempfile.mkdtemp()
        return f"file://{tmp_path}"

    staging_path = pytestconfig.getoption("staging_path")
    return os.path.join(staging_path, str(uuid.uuid4()))


@pytest.fixture(scope="function")
def local_staging_path(global_staging_path):
    return os.path.join(global_staging_path, str(uuid.uuid4()))


@pytest.fixture(scope="session")
def ingestion_job_jar(pytestconfig, project_root, project_version):
    default_path = (
        project_root
        / "spark"
        / "ingestion"
        / "target"
        / f"feast-ingestion-spark-{project_version}.jar"
    )

    return pytestconfig.getoption("ingestion_jar") or f"file://{default_path}"
