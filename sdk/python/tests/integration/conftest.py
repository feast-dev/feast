import logging

import pytest
from testcontainers.keycloak import KeycloakContainer
from testcontainers.minio import MinioContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from tests.utils.auth_permissions_util import setup_permissions_on_keycloak

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@pytest.fixture(scope="session")
def start_keycloak_server():
    logger.info("Starting keycloak instance")
    print("Starting keycloak instance print")
    with KeycloakContainer("quay.io/keycloak/keycloak:24.0.1") as keycloak_container:
        setup_permissions_on_keycloak(keycloak_container.get_client())
        yield keycloak_container.get_url()


@pytest.fixture(scope="session")
def mysql_server():
    container = MySqlContainer("mysql:latest")
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="session")
def postgres_server():
    container = PostgresContainer()
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="session")
def minio_server():
    container = MinioContainer()
    container.start()

    yield container

    container.stop()
