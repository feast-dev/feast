# Copyright 2021 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import multiprocessing
import os
import tempfile
from datetime import timedelta
from sys import platform
from textwrap import dedent

import pandas as pd
import pytest

from feast.utils import _utc_now
from tests.utils.http_server import free_port  # noqa: E402

logger = logging.getLogger(__name__)

level = logging.INFO
logging.basicConfig(
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=level,
)
# Override the logging level for already created loggers (due to loggers being created at the import time)
# Note, that format & datefmt does not need to be set, because by default child loggers don't override them

# Also note, that mypy complains that logging.root doesn't have "manager" because of the way it's written.
# So we have to put a type ignore hint for mypy.
for logger_name in logging.root.manager.loggerDict:  # type: ignore
    if "feast" in logger_name:
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)


def pytest_configure(config):
    if platform == "darwin" or platform.startswith("win"):
        multiprocessing.set_start_method("spawn", force=True)
    else:
        multiprocessing.set_start_method("fork")
    config.addinivalue_line(
        "markers", "integration: mark test that has external dependencies"
    )
    config.addinivalue_line("markers", "benchmark: mark benchmarking tests")
    config.addinivalue_line(
        "markers",
        "universal_online_stores: mark tests that can be run against different online stores",
    )
    config.addinivalue_line(
        "markers",
        "universal_offline_stores: mark tests that can be run against different offline stores",
    )
    config.addinivalue_line(
        "markers",
        "ray_offline_stores_only: mark tests that currently only work with Ray offline store",
    )


def pytest_addoption(parser):
    parser.addoption(
        "--benchmark",
        action="store_true",
        default=False,
        help="Run benchmark tests",
    )


@pytest.fixture
def simple_dataset_1() -> pd.DataFrame:
    now = _utc_now()
    ts = pd.Timestamp(now).round("ms")
    data = {
        "id_join_key": [1, 2, 1, 3, 3],
        "float_col": [0.1, 0.2, 0.3, 4, 5],
        "int64_col": [1, 2, 3, 4, 5],
        "string_col": ["a", "b", "c", "d", "e"],
        "ts_1": [
            ts,
            ts - timedelta(hours=4),
            ts - timedelta(hours=3),
            ts - timedelta(hours=2),
            ts - timedelta(hours=1),
        ],
    }
    return pd.DataFrame.from_dict(data)


@pytest.fixture
def simple_dataset_2() -> pd.DataFrame:
    now = _utc_now()
    ts = pd.Timestamp(now).round("ms")
    data = {
        "id_join_key": ["a", "b", "c", "d", "e"],
        "float_col": [0.1, 0.2, 0.3, 4, 5],
        "int64_col": [1, 2, 3, 4, 5],
        "string_col": ["a", "b", "c", "d", "e"],
        "ts_1": [
            ts,
            ts - timedelta(hours=4),
            ts - timedelta(hours=3),
            ts - timedelta(hours=2),
            ts - timedelta(hours=1),
        ],
    }
    return pd.DataFrame.from_dict(data)


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created {temp_dir}")
        yield temp_dir


@pytest.fixture
def server_port():
    return free_port()


@pytest.fixture
def feature_store(temp_dir, auth_config, applied_permissions):
    try:
        from tests.utils.auth_permissions_util import default_store
    except ModuleNotFoundError as e:
        pytest.skip(f"Optional auth test dependency is not installed: {e.name}")

    print(f"Creating store at {temp_dir}")
    return default_store(str(temp_dir), auth_config, applied_permissions)


@pytest.fixture(scope="module")
def all_markers_from_module(request):
    markers = set()
    for item in request.session.items:
        for marker in item.iter_markers():
            markers.add(marker.name)

    return markers


@pytest.fixture(scope="module")
def is_integration_test(all_markers_from_module):
    return "integration" in all_markers_from_module


@pytest.fixture(
    scope="module",
    params=[
        dedent(
            """
          auth:
            type: no_auth
          """
        ),
        dedent(
            """
          auth:
            type: kubernetes
        """
        ),
        dedent(
            """
          auth:
            type: oidc
            client_id: feast-integration-client
            client_secret: feast-integration-client-secret
            username: reader_writer
            password: password
            auth_discovery_url: KEYCLOAK_URL_PLACE_HOLDER/realms/master/.well-known/openid-configuration
        """
        ),
    ],
)
def auth_config(request, is_integration_test):
    auth_configuration = request.param

    if is_integration_test:
        if "kubernetes" in auth_configuration:
            pytest.skip(
                "skipping integration tests for kubernetes platform, unit tests are covering this functionality."
            )
        elif "oidc" in auth_configuration:
            keycloak_url = request.getfixturevalue("start_keycloak_server")
            return auth_configuration.replace("KEYCLOAK_URL_PLACE_HOLDER", keycloak_url)

    return auth_configuration


@pytest.fixture(scope="module")
def tls_mode(request):
    try:
        from tests.utils.ssl_certifcates_util import (
            combine_trust_stores,
            create_ca_trust_store,
            generate_self_signed_cert,
        )
    except ModuleNotFoundError as e:
        pytest.skip(f"Optional TLS test dependency is not installed: {e.name}")

    is_tls_mode = request.param[0]
    output_combined_truststore_path = ""

    if is_tls_mode:
        certificates_path = tempfile.mkdtemp()
        tls_key_path = os.path.join(certificates_path, "key.pem")
        tls_cert_path = os.path.join(certificates_path, "cert.pem")

        generate_self_signed_cert(cert_path=tls_cert_path, key_path=tls_key_path)
        is_ca_trust_store_set = request.param[1]
        if is_ca_trust_store_set:
            # Paths
            feast_ca_trust_store_path = os.path.join(
                certificates_path, "feast_trust_store.pem"
            )
            create_ca_trust_store(
                public_key_path=tls_cert_path,
                private_key_path=tls_key_path,
                output_trust_store_path=feast_ca_trust_store_path,
            )

            # Combine trust stores
            output_combined_path = os.path.join(
                certificates_path, "combined_trust_store.pem"
            )
            combine_trust_stores(feast_ca_trust_store_path, output_combined_path)
    else:
        tls_key_path = ""
        tls_cert_path = ""

    return is_tls_mode, tls_key_path, tls_cert_path, output_combined_truststore_path