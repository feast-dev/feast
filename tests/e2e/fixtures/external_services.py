import pytest
from pytest_redis.executor import NoopRedis

__all__ = ("feast_core", "feast_serving", "redis_server", "kafka_server")


@pytest.fixture
def redis_server(pytestconfig):
    host, port = pytestconfig.getoption("redis_url").split(":")
    return NoopRedis(host, port)


@pytest.fixture
def feast_core(pytestconfig):
    host, port = pytestconfig.getoption("core_url").split(":")
    return host, port


@pytest.fixture
def feast_serving(pytestconfig):
    host, port = pytestconfig.getoption("serving_url").split(":")
    return host, port


@pytest.fixture
def kafka_server(pytestconfig):
    host, port = pytestconfig.getoption("kafka_brokers").split(":")
    return host, port
