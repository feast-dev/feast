import pytest
from pytest_redis.executor import NoopRedis

from tests.e2e.fixtures.statsd_stub import PrometheusStatsDServer

__all__ = (
    "feast_core",
    "feast_serving",
    "redis_server",
    "kafka_server",
    "enable_auth",
    "feast_jobservice",
    "statsd_server",
)


@pytest.fixture(scope="session")
def redis_server(pytestconfig):
    host, port = pytestconfig.getoption("redis_url").split(":")
    return NoopRedis(host, port, None)


@pytest.fixture(scope="session")
def feast_core(pytestconfig):
    host, port = pytestconfig.getoption("core_url").split(":")
    return host, port


@pytest.fixture(scope="session")
def feast_serving(pytestconfig):
    host, port = pytestconfig.getoption("serving_url").split(":")
    return host, port


@pytest.fixture(scope="session")
def kafka_server(pytestconfig):
    host, port = pytestconfig.getoption("kafka_brokers").split(":")
    return host, port


@pytest.fixture(scope="session")
def enable_auth():
    return False


@pytest.fixture(scope="session")
def feast_jobservice(pytestconfig):
    host, port = pytestconfig.getoption("job_service_url").split(":")
    return host, port


@pytest.fixture(scope="session")
def statsd_server(pytestconfig):
    host, port = pytestconfig.getoption("statsd_url").split(":")
    prometheus_host, prometheus_port = pytestconfig.getoption("prometheus_url").split(
        ":"
    )
    return PrometheusStatsDServer(host, port, prometheus_host, prometheus_port)
