import os
import shutil
import socket
import time
import uuid
import pyspark
import requests
import pathlib

import yaml
import pytest
import subprocess
import tempfile
from pathlib import Path


from pytest_postgresql import factories as pg_factories
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_redis import factories as redis_factories
from pytest_redis.executor import RedisExecutor
from pytest_kafka import make_kafka_server, make_zookeeper_process

from feast import Client


@pytest.fixture(scope="session")
def project_root():
    return Path(__file__).parent.parent.parent


@pytest.fixture(scope="session")
def project_version(pytestconfig):
    if pytestconfig.getoption("version"):
        return pytestconfig.getoption("version")

    return "0.8-SNAPSHOT"


def download_kafka(version="2.12-2.6.0"):
    r = requests.get(f'https://downloads.apache.org/kafka/2.6.0/kafka_{version}.tgz')
    temp_dir = pathlib.Path(tempfile.gettempdir())
    local_path = temp_dir / 'kafka.tgz'

    with open(local_path, 'wb') as f:
        f.write(r.content)

    shutil.unpack_archive(local_path, tempfile.gettempdir())
    return temp_dir / f'kafka_{version}' / "bin"


def _start_jar(jar, options=None) -> subprocess.Popen:
    if not os.path.isfile(jar):
        raise ValueError(f"{jar} doesn't exist")

    cmd = [
        shutil.which("java"),
        "-jar",
        jar
    ]
    if options:
        cmd.extend(options)
    print(' '.join(cmd))
    return subprocess.Popen(cmd)


def _wait_port_open(port, max_wait=60):
    print(f"Waiting for port {port}")
    start = time.time()

    while True:
        try:
            socket.create_connection(('localhost', port), timeout=1)
        except OSError:
            if time.time() - start > max_wait:
                raise

            time.sleep(1)
        else:
            return


@pytest.fixture(scope="session", params=[pytest.param(True, marks=pytest.mark.skip), False])
def enable_auth(request):
    return request.param


@pytest.fixture(scope="session")
def feast_core(project_root, project_version, enable_auth, postgres_server: PostgreSQLExecutor):
    jar = str(project_root / "core" / "target" / f"feast-core-{project_version}-exec.jar")
    config = dict(
        feast=dict(
            security=dict(
                enabled=enable_auth,
                provider="jwt",
                options=dict(jwkEndpointURI="https://www.googleapis.com/oauth2/v3/certs")
            )
        ),
        spring=dict(
            datasource=dict(
                url=f"jdbc:postgresql://127.0.0.1:{postgres_server.port}/postgres"
            )
        )
    )

    with tempfile.NamedTemporaryFile(suffix='.yaml', mode='w+') as config_file:
        yaml.dump(config, config_file)
        config_file.flush()

        process = _start_jar(jar, [f"--spring.config.location=classpath:/application.yml,file://{config_file.name}"])
        _wait_port_open(6565)
        yield
        process.terminate()


@pytest.fixture(scope="session")
def feast_serving(project_root, project_version, enable_auth, redis_server: RedisExecutor):
    jar = str(project_root / "serving" / "target" / f"feast-serving-{project_version}-exec.jar")
    config = dict(
        feast=dict(
            stores=[dict(
                name="online",
                type="REDIS",
                config=dict(
                    host=redis_server.host,
                    port=redis_server.port
                )
            )],
            coreAuthentication=dict(
                enabled=enable_auth,
                provider="google"
            ),
            security=dict(
                authentication=dict(
                    enabled=enable_auth,
                    provider="jwt"
                )
            )
        )
    )

    with tempfile.NamedTemporaryFile(suffix='.yaml', mode='w+') as config_file:
        yaml.dump(config, config_file)
        config_file.flush()

        process = _start_jar(jar, [f"--spring.config.location=classpath:/application.yml,file://{config_file.name}"])
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
def feast_client(pytestconfig,
                 ingestion_job_jar,
                 redis_server: RedisExecutor,
                 feast_core,
                 feast_serving,
                 global_staging_path):
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
            historical_feature_output_location=os.path.join(global_staging_path, "historical_output")
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


postgres_server = pg_factories.postgresql_proc(password="password")
redis_server = redis_factories.redis_proc(executable=shutil.which("redis-server"))

KAFKA_BIN = download_kafka()
zookeeper_server = make_zookeeper_process(str(KAFKA_BIN / "zookeeper-server-start.sh"), zk_config_template="""
dataDir={zk_data_dir}
clientPort={zk_port}
maxClientCnxns=0
admin.enableServer=false""")
kafka_server = make_kafka_server(kafka_bin=str(KAFKA_BIN / "kafka-server-start.sh"),
                                 zookeeper_fixture_name='zookeeper_server')
