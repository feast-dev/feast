import logging

import pytest
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.keycloak import KeycloakContainer
from testcontainers.minio import MinioContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from tests.utils.auth_permissions_util import setup_permissions_on_keycloak

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def start_keycloak_server():
    logger.info("Starting keycloak instance")
    with KeycloakContainer("quay.io/keycloak/keycloak:24.0.1") as keycloak_container:
        log_string_to_wait_for = "Running the server in development mode"
        waited = wait_for_logs(
            container=keycloak_container,
            predicate=log_string_to_wait_for,
            timeout=30,
            interval=10,
        )
        logger.info("Waited for %s seconds until keycloak container was up", waited)

        setup_permissions_on_keycloak(keycloak_container.get_client())
        yield keycloak_container.get_url()


@pytest.fixture(scope="session")
def mysql_server():
    container = MySqlContainer("mysql:latest")
    container.start()

    def wait_for_sql_query(container, query, timeout=30):
        from time import sleep, time

        start_time = time()

        while time() - start_time < timeout:
            command = f"mysql -uroot -p{container.root_password} -e '{query};'"
            logger.info(f"Trying {query} command {command}")
            try:
                container.exec(command)
                logger.info(f"Completed in {time() - start_time}s")
                return  # Success, exit the function
            except Exception:
                logger.info(f"Retry in {1}s")
                sleep(1)  # Wait and retry

        raise Exception(f"Timeout waiting for SQL query: {query}")

    wait_for_sql_query(container, "SELECT 10")

    yield container


@pytest.fixture(scope="session")
def postgres_server():
    container = PostgresContainer()
    logger.info(f"Starting container {container._name} from {container.image}")
    container.start()

    def wait_for_sql_query(connection_url, query, timeout=30):
        from time import sleep, time

        import psycopg

        start_time = time()
        connection_url = connection_url.replace("+psycopg2", "")

        while time() - start_time < timeout:
            logger.info(f"Trying {query} from {connection_url}")
            try:
                conn = psycopg.connect(connection_url)
                cur = conn.cursor()
                cur.execute(query)
                cur.close()
                conn.close()
                logger.info(f"Completed in {time() - start_time}s")
                return  # Success, exit the function
            except psycopg.Error:
                logger.info(f"Retry in {1}s")
                sleep(1)  # Wait and retry

        raise Exception(f"Timeout waiting for SQL query: {query}")

    wait_for_sql_query(container.get_connection_url(), "SELECT 10")
    yield container


@pytest.fixture(scope="session")
def minio_server():
    container = MinioContainer()
    container.start()

    yield container

    container.stop()
