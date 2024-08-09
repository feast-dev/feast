import logging

import pytest
from testcontainers.keycloak import KeycloakContainer

from tests.utils.auth_permissions_util import setup_permissions_on_keycloak

logger = logging.getLogger(__name__)


@pytest.mark.xdist_group(name="keycloak")
@pytest.fixture(scope="session")
def start_keycloak_server():
    logger.info("Starting keycloak instance")
    with KeycloakContainer("quay.io/keycloak/keycloak:24.0.1") as keycloak_container:
        setup_permissions_on_keycloak(keycloak_container.get_client())
        yield keycloak_container.get_url()
