import os
import shutil
import socket
import subprocess
import tempfile
import time
from typing import Any, Dict, Tuple

import pyspark
import pytest
import yaml
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_redis.executor import RedisExecutor

__all__ = (
    "feast_core",
    "feast_serving",
    "enable_auth",
    "feast_jobservice",
)


def _start_jar(jar, options=None) -> subprocess.Popen:
    if not os.path.isfile(jar):
        raise ValueError(f"{jar} doesn't exist")

    cmd = [shutil.which("java"), "-jar", jar]
    if options:
        cmd.extend(options)

    return subprocess.Popen(cmd)  # type: ignore


def _wait_port_open(port, max_wait=60):
    print(f"Waiting for port {port}")
    start = time.time()

    while True:
        try:
            socket.create_connection(("localhost", port), timeout=1)
        except OSError:
            if time.time() - start > max_wait:
                raise

            time.sleep(1)
        else:
            return


@pytest.fixture(
    scope="session", params=[False],
)
def enable_auth(request):
    return request.param


@pytest.fixture(scope="session")
def feast_core(
    project_root, project_version, enable_auth, postgres_server: PostgreSQLExecutor
):
    jar = str(
        project_root / "core" / "target" / f"feast-core-{project_version}-exec.jar"
    )
    config = dict(
        feast=dict(
            security=dict(
                enabled=enable_auth,
                provider="jwt",
                options=dict(
                    jwkEndpointURI="https://www.googleapis.com/oauth2/v3/certs"
                ),
            )
        ),
        spring=dict(
            datasource=dict(
                url=f"jdbc:postgresql://{postgres_server.host}:{postgres_server.port}/postgres"
            )
        ),
    )

    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w+") as config_file:
        yaml.dump(config, config_file)
        config_file.flush()

        process = _start_jar(
            jar,
            [
                f"--spring.config.location=classpath:/application.yml,file://{config_file.name}"
            ],
        )
        _wait_port_open(6565)
        yield "localhost", 6565
        process.terminate()


@pytest.fixture(scope="session")
def feast_serving(
    project_root,
    project_version,
    enable_auth,
    redis_server: RedisExecutor,
    feast_core,
    pytestconfig,
):
    _wait_port_open(6565)  # in case core is restarting with new config

    jar = str(
        project_root
        / "serving"
        / "target"
        / f"feast-serving-{project_version}-exec.jar"
    )
    if pytestconfig.getoption("redis_cluster"):
        store: Dict[str, Any] = dict(
            name="online",
            type="REDIS_CLUSTER",
            config=dict(connection_string=f"{redis_server.host}:{redis_server.port}"),
        )
    else:
        store = dict(
            name="online",
            type="REDIS",
            config=dict(host=redis_server.host, port=redis_server.port),
        )

    config = dict(
        feast=dict(
            stores=[store],
            coreAuthentication=dict(enabled=enable_auth, provider="google"),
            security=dict(authentication=dict(enabled=enable_auth, provider="jwt")),
        )
    )

    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w+") as config_file:
        yaml.dump(config, config_file)
        config_file.flush()

        process = _start_jar(
            jar,
            [
                f"--spring.config.location=classpath:/application.yml,file://{config_file.name}"
            ],
        )
        _wait_port_open(6566)
        yield "localhost", 6566
        process.terminate()


@pytest.fixture(scope="session")
def feast_jobservice(
    pytestconfig,
    ingestion_job_jar,
    redis_server: RedisExecutor,
    feast_core: Tuple[str, int],
    feast_serving: Tuple[str, int],
    global_staging_path,
):
    if not pytestconfig.getoption("with_job_service"):
        yield None
    else:
        env = os.environ.copy()

        if pytestconfig.getoption("env") == "local":
            env["FEAST_CORE_URL"] = f"{feast_core[0]}:{feast_core[1]}"
            env["FEAST_SERVING_URL"] = f"{feast_serving[0]}:{feast_serving[1]}"
            env["FEAST_SPARK_LAUNCHER"] = "standalone"
            env["FEAST_SPARK_STANDALONE_MASTER"] = "local"
            env["FEAST_SPARK_HOME"] = os.getenv("SPARK_HOME") or os.path.dirname(
                pyspark.__file__
            )
            env["FEAST_SPARK_INGESTION_JAR"] = ingestion_job_jar
            env["FEAST_REDIS_HOST"] = redis_server.host
            env["FEAST_REDIS_PORT"] = str(redis_server.port)
            env["FEAST_SPARK_STAGING_LOCATION"] = os.path.join(
                global_staging_path, "spark"
            )
            env["FEAST_HISTORICAL_FEATURE_OUTPUT_LOCATION"] = os.path.join(
                global_staging_path, "historical_output"
            )

        if pytestconfig.getoption("env") == "gcloud":
            env["FEAST_CORE_URL"] = f"{feast_core[0]}:{feast_core[1]}"
            env["FEAST_SERVING_URL"] = f"{feast_serving[0]}:{feast_serving[1]}"
            env["FEAST_SPARK_LAUNCHER"] = "dataproc"
            env["FEAST_DATAPROC_CLUSTER_NAME"] = pytestconfig.getoption(
                "dataproc_cluster_name"
            )
            env["FEAST_DATAPROC_PROJECT"] = pytestconfig.getoption("dataproc_project")
            env["FEAST_DATAPROC_REGION"] = pytestconfig.getoption("dataproc_region")
            env["FEAST_DATAPROC_EXECUTOR_INSTANCES"] = pytestconfig.getoption(
                "dataproc_executor_instances"
            )
            env["FEAST_DATAPROC_EXECUTOR_CORES"] = pytestconfig.getoption(
                "dataproc_executor_cores"
            )
            env["FEAST_DATAPROC_EXECUTOR_MEMORY"] = pytestconfig.getoption(
                "dataproc_executor_memory"
            )
            env["FEAST_SPARK_STAGING_LOCATION"] = os.path.join(
                global_staging_path, "dataproc"
            )
            env["FEAST_SPARK_INGESTION_JAR"] = ingestion_job_jar
            env["FEAST_REDIS_HOST"] = pytestconfig.getoption("redis_url").split(":")[0]
            env["FEAST_REDIS_PORT"] = pytestconfig.getoption("redis_url").split(":")[1]
            env["FEAST_HISTORICAL_FEATURE_OUTPUT_LOCATION"] = os.path.join(
                global_staging_path, "historical_output"
            )

        process = subprocess.Popen(["feast", "server"], env=env)
        _wait_port_open(6568)
        yield "localhost", 6568
        process.terminate()
