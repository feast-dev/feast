import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import pyspark
import pytest
import requests
import yaml
from pytest_kafka import make_kafka_server, make_zookeeper_process
from pytest_postgresql import factories as pg_factories
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_redis import factories as redis_factories
from pytest_redis.executor import RedisExecutor

from feast import Client

__all__ = (
    "project_root",
    "project_version",
    "feast_client",
    "feast_core",
    "feast_serving",
    "ingestion_job_jar",
    "global_staging_path",
    "local_staging_path",
    "redis_server",
    "postgres_server",
    "kafka_server",
    "zookeeper_server",
    "enable_auth",
)


@pytest.fixture(scope="session")
def project_root():
    return Path(__file__).parent.parent.parent


@pytest.fixture(scope="session")
def project_version(pytestconfig):
    if pytestconfig.getoption("feast_version"):
        return pytestconfig.getoption("feast_version")

    return "0.8-SNAPSHOT"


def download_kafka(version="2.12-2.6.0"):
    r = requests.get(f"https://downloads.apache.org/kafka/2.6.0/kafka_{version}.tgz")
    temp_dir = pathlib.Path(tempfile.mkdtemp())
    local_path = temp_dir / "kafka.tgz"

    with open(local_path, "wb") as f:
        f.write(r.content)

    shutil.unpack_archive(str(local_path), str(temp_dir))
    return temp_dir / f"kafka_{version}" / "bin"


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
    scope="session", params=[pytest.param(True, marks=pytest.mark.skip), False]
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
                url=f"jdbc:postgresql://127.0.0.1:{postgres_server.port}/postgres"
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
        yield
        process.terminate()


@pytest.fixture(scope="session")
def feast_serving(
    project_root, project_version, enable_auth, redis_server: RedisExecutor
):
    jar = str(
        project_root
        / "serving"
        / "target"
        / f"feast-serving-{project_version}-exec.jar"
    )
    config = dict(
        feast=dict(
            stores=[
                dict(
                    name="online",
                    type="REDIS",
                    config=dict(host=redis_server.host, port=redis_server.port),
                )
            ],
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
        yield
        process.terminate()


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


@pytest.fixture(scope="session")
def feast_client(
    pytestconfig,
    ingestion_job_jar,
    redis_server: RedisExecutor,
    feast_core,
    feast_serving,
    global_staging_path,
):
    if pytestconfig.getoption("env") == "local":
        return Client(
            core_url=pytestconfig.getoption("core_url"),
            serving_url=pytestconfig.getoption("serving_url"),
            spark_launcher="standalone",
            spark_standalone_master="local",
            spark_home=os.getenv("SPARK_HOME") or os.path.dirname(pyspark.__file__),
            spark_ingestion_jar=ingestion_job_jar,
            redis_host=redis_server.host,
            redis_port=redis_server.port,
            historical_feature_output_location=os.path.join(
                global_staging_path, "historical_output"
            ),
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


if not os.environ.get('POSTGRES_HOST'):
    postgres_server = pg_factories.postgresql_proc(password="password")
else:
    postgres_server = pg_factories.postgresql_noproc(host=os.environ['POSTGRES_HOST'],
                                                     port=os.environ['POSTGRES_PORT'])

redis_server = redis_factories.redis_proc(executable=shutil.which("redis-server"))

KAFKA_BIN = download_kafka()
zookeeper_server = make_zookeeper_process(
    str(KAFKA_BIN / "zookeeper-server-start.sh"),
    zk_config_template="""
dataDir={zk_data_dir}
clientPort={zk_port}
maxClientCnxns=0
admin.enableServer=false""",
)
kafka_server = make_kafka_server(
    kafka_bin=str(KAFKA_BIN / "kafka-server-start.sh"),
    zookeeper_fixture_name="zookeeper_server",
)
