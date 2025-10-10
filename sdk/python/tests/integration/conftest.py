import atexit
import json
import logging
import random
import tempfile
import time
from pathlib import Path
from typing import Optional

import pytest
from testcontainers.keycloak import KeycloakContainer
from testcontainers.minio import MinioContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from tests.utils.auth_permissions_util import setup_permissions_on_keycloak

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Shared Keycloak state
_keycloak_container: Optional[KeycloakContainer] = None
_keycloak_info_file = Path(tempfile.gettempdir()) / "feast_keycloak_info.json"


def _is_keycloak_healthy(url: str) -> bool:
    """Health check for Keycloak."""
    try:
        import requests

        response = requests.get(f"{url}/health/ready", timeout=3)
        return response.status_code == 200
    except Exception:
        try:
            import requests

            response = requests.get(f"{url}/auth/realms/master", timeout=3)
            return response.status_code in [200, 404]
        except Exception:
            return False


def _get_shared_keycloak_url() -> Optional[str]:
    """Get URL of existing Keycloak instance if available."""
    try:
        if _keycloak_info_file.exists():
            with open(_keycloak_info_file, "r") as f:
                info = json.load(f)

            url = info.get("url")
            if url and _is_keycloak_healthy(url):
                return url
            else:
                _keycloak_info_file.unlink()
    except Exception as e:
        logger.debug(f"Error reading Keycloak info: {e}")
        try:
            _keycloak_info_file.unlink()
        except Exception:
            pass
    return None


def _save_keycloak_info(url: str):
    """Save Keycloak info to shared file."""
    try:
        info = {"url": url, "timestamp": time.time()}
        with open(_keycloak_info_file, "w") as f:
            json.dump(info, f)
    except Exception as e:
        logger.warning(f"Failed to save Keycloak info: {e}")


def _cleanup_keycloak():
    """Cleanup Keycloak container on exit."""
    global _keycloak_container
    if _keycloak_container:
        try:
            logger.info("Stopping Keycloak container")
            _keycloak_container.stop()
        except Exception as e:
            logger.warning(f"Error stopping Keycloak: {e}")
        finally:
            _keycloak_container = None
            try:
                _keycloak_info_file.unlink()
            except Exception:
                pass


@pytest.fixture(scope="session")
def start_keycloak_server():
    global _keycloak_container

    existing_url = _get_shared_keycloak_url()
    if existing_url:
        logger.info(f"Reusing existing Keycloak at {existing_url}")
        yield existing_url
        return

    time.sleep(random.uniform(0, 0.5))

    existing_url = _get_shared_keycloak_url()
    if existing_url:
        logger.info(f"Found Keycloak started by another process: {existing_url}")
        yield existing_url
        return

    try:
        logger.info("Starting new Keycloak instance")
        _keycloak_container = KeycloakContainer("quay.io/keycloak/keycloak:24.0.1")
        _keycloak_container.start()

        setup_permissions_on_keycloak(_keycloak_container.get_client())

        keycloak_url = _keycloak_container.get_url()

        _save_keycloak_info(keycloak_url)
        atexit.register(_cleanup_keycloak)

        logger.info(f"Keycloak ready at {keycloak_url}")
        yield keycloak_url

    except Exception as e:
        logger.error(f"Failed to start Keycloak: {e}")
        if _keycloak_container:
            try:
                _keycloak_container.stop()
            except Exception:
                pass
            _keycloak_container = None
        raise


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
