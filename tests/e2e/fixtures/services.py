import os
import pathlib
import shutil

import port_for
import pytest
import requests
from pytest_kafka import make_kafka_server, make_zookeeper_process
from pytest_postgresql import factories as pg_factories
from pytest_redis import factories as redis_factories

__all__ = (
    "kafka_server",
    "kafka_port",
    "zookeeper_server",
    "postgres_server",
    "redis_server",
    "statsd_server",
)

from tests.e2e.fixtures.statsd_stub import StatsDStub


def download_kafka(version="2.12-2.6.0"):
    temp_dir = pathlib.Path("/tmp")
    local_path = temp_dir / f"kafka_{version}.tgz"

    if not os.path.isfile(local_path):
        r = requests.get(
            f"https://downloads.apache.org/kafka/2.6.0/kafka_{version}.tgz"
        )

        with open(local_path, "wb") as f:
            f.write(r.content)

    shutil.unpack_archive(str(local_path), str(temp_dir))
    return temp_dir / f"kafka_{version}" / "bin"


@pytest.fixture
def kafka_server(kafka_port):
    _, port = kafka_port
    return "localhost", port


@pytest.fixture
def statsd_server():
    port = port_for.select_random(None)
    server = StatsDStub(port=port)
    server.start()
    yield server
    server.stop()


postgres_server = pg_factories.postgresql_proc(password="password")
redis_server = redis_factories.redis_proc(
    executable=shutil.which("redis-server"), timeout=3600
)

KAFKA_BIN = download_kafka()
zookeeper_server = make_zookeeper_process(
    str(KAFKA_BIN / "zookeeper-server-start.sh"),
    zk_config_template="""
dataDir={zk_data_dir}
clientPort={zk_port}
maxClientCnxns=0
admin.enableServer=false""",
)
kafka_port = make_kafka_server(
    kafka_bin=str(KAFKA_BIN / "kafka-server-start.sh"),
    zookeeper_fixture_name="zookeeper_server",
)
