import logging
import random
import time
from multiprocessing import Manager

import pytest
from testcontainers.keycloak import KeycloakContainer
from testcontainers.minio import MinioContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from tests.utils.auth_permissions_util import setup_permissions_on_keycloak

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

shared_state = Manager().dict()


@pytest.fixture(scope="session")
def start_keycloak_server():
    # Add random sleep between 0 and 2 before checking the state to avoid concurrency issues.
    random_sleep_time = random.uniform(0, 2)
    time.sleep(random_sleep_time)

    # If the Keycloak instance is already started (in any worker), reuse it
    if shared_state.get("keycloak_started", False):
        return shared_state["keycloak_url"]
    logger.info("Starting keycloak instance")
    with KeycloakContainer("quay.io/keycloak/keycloak:24.0.1") as keycloak_container:
        setup_permissions_on_keycloak(keycloak_container.get_client())
        shared_state["keycloak_started"] = True
        shared_state["keycloak_url"] = keycloak_container.get_url()
        yield shared_state["keycloak_url"]

    # After the fixture is done, cleanup the shared state
    del shared_state["keycloak_started"]
    del shared_state["keycloak_url"]


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
