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
import random
import tempfile
from datetime import timedelta
from multiprocessing import Process
from sys import platform
from textwrap import dedent
from typing import Any, Dict, List, Tuple, no_type_check
from unittest import mock

import pandas as pd
import pytest
from _pytest.nodes import Item

from feast.data_source import DataSource
from feast.feature_store import FeatureStore  # noqa: E402
from feast.utils import _utc_now
from feast.wait import wait_retry_backoff  # noqa: E402
from tests.data.data_creator import (
    create_basic_driver_dataset,  # noqa: E402
    create_document_dataset,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,  # noqa: E402
)
from tests.integration.feature_repos.repo_configuration import (  # noqa: E402
    AVAILABLE_OFFLINE_STORES,
    AVAILABLE_ONLINE_STORES,
    OFFLINE_STORE_TO_PROVIDER_CONFIG,
    Environment,
    TestData,
    construct_test_environment,
    construct_universal_feature_views,
    construct_universal_test_data,
)
from tests.integration.feature_repos.universal.data_sources.file import (
    FileDataSourceCreator,  # noqa: E402
)
from tests.integration.feature_repos.universal.entities import (  # noqa: E402
    customer,
    driver,
    location,
)
from tests.utils.auth_permissions_util import (
    default_store,
    invalid_list_entities_perm,
    list_entities_perm,
    list_fv_perm,
    list_odfv_perm,
    list_permissions_perm,
    list_sfv_perm,
)
from tests.utils.http_server import check_port_open, free_port  # noqa: E402

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
    if platform in ["darwin", "windows"]:
        multiprocessing.set_start_method("spawn")
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


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run tests with external dependencies",
    )
    parser.addoption(
        "--benchmark",
        action="store_true",
        default=False,
        help="Run benchmark tests",
    )


def pytest_collection_modifyitems(config, items: List[Item]):
    should_run_integration = config.getoption("--integration") is True
    should_run_benchmark = config.getoption("--benchmark") is True

    integration_tests = [t for t in items if "integration" in t.keywords]
    if not should_run_integration:
        for t in integration_tests:
            items.remove(t)
    else:
        items.clear()
        for t in integration_tests:
            items.append(t)

    benchmark_tests = [t for t in items if "benchmark" in t.keywords]
    if not should_run_benchmark:
        for t in benchmark_tests:
            items.remove(t)
    else:
        items.clear()
        for t in benchmark_tests:
            items.append(t)


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


def start_test_local_server(repo_path: str, port: int):
    fs = FeatureStore(repo_path)
    fs.serve(host="localhost", port=port)


@pytest.fixture
def environment(request, worker_id):
    e = construct_test_environment(
        request.param, worker_id=worker_id, fixture_request=request
    )

    e.setup()

    if hasattr(e.data_source_creator, "mock_environ"):
        with mock.patch.dict(os.environ, e.data_source_creator.mock_environ):
            yield e
    else:
        yield e

    e.teardown()


_config_cache: Any = {}


@no_type_check
def pytest_generate_tests(metafunc: pytest.Metafunc):
    """
    This function receives each test function (wrapped in Metafunc)
    at the collection stage (before tests started).
    Here we can access all fixture requests made by the test as well as its markers.
    That allows us to dynamically parametrize the test based on markers and fixtures
    by calling metafunc.parametrize(...).

    See more examples at https://docs.pytest.org/en/6.2.x/example/parametrize.html#paramexamples

    We also utilize indirect parametrization here. Since `environment` is a fixture,
    when we call metafunc.parametrize("environment", ..., indirect=True) we actually
    parametrizing this "environment" fixture and not the test itself.
    Moreover, by utilizing `_config_cache` we are able to share `environment` fixture between different tests.
    In order for pytest to group tests together (and share environment fixture)
    parameter should point to the same Python object (hence, we use _config_cache dict to store those objects).
    """
    if "environment" in metafunc.fixturenames:
        markers = {m.name: m for m in metafunc.definition.own_markers}
        offline_stores = None
        if "universal_offline_stores" in markers:
            # Offline stores can be explicitly requested
            if "only" in markers["universal_offline_stores"].kwargs:
                offline_stores = [
                    OFFLINE_STORE_TO_PROVIDER_CONFIG.get(store_name)
                    for store_name in markers["universal_offline_stores"].kwargs["only"]
                    if store_name in OFFLINE_STORE_TO_PROVIDER_CONFIG
                ]
            else:
                offline_stores = AVAILABLE_OFFLINE_STORES
        else:
            # default offline store for testing online store dimension
            offline_stores = [("local", FileDataSourceCreator)]

        online_stores = None
        if "universal_online_stores" in markers:
            # Online stores can be explicitly requested
            if "only" in markers["universal_online_stores"].kwargs:
                online_stores = [
                    AVAILABLE_ONLINE_STORES.get(store_name)
                    for store_name in markers["universal_online_stores"].kwargs["only"]
                    if store_name in AVAILABLE_ONLINE_STORES
                ]
            else:
                online_stores = AVAILABLE_ONLINE_STORES.values()

        if online_stores is None:
            # No online stores requested -> setting the default or first available
            online_stores = [
                AVAILABLE_ONLINE_STORES.get(
                    "redis",
                    AVAILABLE_ONLINE_STORES.get(
                        "sqlite", next(iter(AVAILABLE_ONLINE_STORES.values()))
                    ),
                )
            ]

        extra_dimensions: List[Dict[str, Any]] = [{}]

        if "python_server" in metafunc.fixturenames:
            extra_dimensions.extend([{"python_feature_server": True}])

        configs = []
        if offline_stores:
            for provider, offline_store_creator in offline_stores:
                for online_store, online_store_creator in online_stores:
                    for dim in extra_dimensions:
                        config = {
                            "provider": provider,
                            "offline_store_creator": offline_store_creator,
                            "online_store": online_store,
                            "online_store_creator": online_store_creator,
                            **dim,
                        }

                        c = IntegrationTestRepoConfig(**config)

                        if c not in _config_cache:
                            _config_cache[c] = c

                        configs.append(_config_cache[c])
        else:
            # No offline stores requested -> setting the default or first available
            offline_stores = [("local", FileDataSourceCreator)]

        metafunc.parametrize(
            "environment", configs, indirect=True, ids=[str(c) for c in configs]
        )


@pytest.fixture
def feature_server_endpoint(environment):
    if not environment.python_feature_server or environment.provider != "local":
        yield environment.feature_store.get_feature_server_endpoint()
        return

    port = free_port()

    proc = Process(
        target=start_test_local_server,
        args=(environment.feature_store.repo_path, port),
    )
    if (
        environment.python_feature_server
        and environment.test_repo_config.provider == "local"
    ):
        proc.start()
        # Wait for server to start
        wait_retry_backoff(
            lambda: (None, check_port_open("localhost", port)),
            timeout_secs=10,
        )

    yield f"http://localhost:{port}"

    if proc.is_alive():
        proc.kill()

        # wait server to free the port
        wait_retry_backoff(
            lambda: (
                None,
                not check_port_open("localhost", environment.get_local_server_port()),
            ),
            timeout_secs=30,
        )


@pytest.fixture
def universal_data_sources(environment) -> TestData:
    return construct_universal_test_data(environment)


@pytest.fixture
def e2e_data_sources(environment: Environment):
    df = create_basic_driver_dataset()
    data_source = environment.data_source_creator.create_data_source(
        df,
        environment.feature_store.project,
        field_mapping={"ts_1": "ts"},
    )

    return df, data_source


@pytest.fixture
def feature_store_for_online_retrieval(
    environment, universal_data_sources
) -> Tuple[FeatureStore, List[str], List[Dict[str, int]]]:
    """
    Returns a feature store that is ready for online retrieval, along with entity rows and feature
    refs that can be used to query for online features.
    """
    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    feast_objects = []
    feast_objects.extend(feature_views.values())
    feast_objects.extend([driver(), customer(), location()])
    fs.apply(feast_objects)
    fs.materialize(environment.start_date, environment.end_date)

    sample_drivers = random.sample(entities.driver_vals, 10)
    sample_customers = random.sample(entities.customer_vals, 10)

    entity_rows = [
        {"driver_id": d, "customer_id": c, "val_to_add": 50}
        for (d, c) in zip(sample_drivers, sample_customers)
    ]

    feature_refs = [
        "driver_stats:conv_rate",
        "driver_stats:avg_daily_trips",
        "customer_profile:current_balance",
        "customer_profile:avg_passenger_count",
        "customer_profile:lifetime_trip_count",
        "conv_rate_plus_100:conv_rate_plus_100",
        "conv_rate_plus_100:conv_rate_plus_val_to_add",
        "global_stats:num_rides",
        "global_stats:avg_ride_length",
    ]

    return fs, feature_refs, entity_rows


@pytest.fixture
def fake_ingest_data():
    """Fake data to ingest into the feature store"""
    data = {
        "driver_id": [1],
        "conv_rate": [0.5],
        "acc_rate": [0.6],
        "avg_daily_trips": [4],
        "event_timestamp": [pd.Timestamp(_utc_now()).round("ms")],
        "created": [pd.Timestamp(_utc_now()).round("ms")],
    }
    return pd.DataFrame(data)


@pytest.fixture
def fake_document_data(environment: Environment) -> Tuple[pd.DataFrame, DataSource]:
    df = create_document_dataset()
    data_source = environment.data_source_creator.create_data_source(
        df,
        environment.feature_store.project,
    )
    return df, data_source


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
    print(f"Creating store at {temp_dir}")
    return default_store(str(temp_dir), auth_config, applied_permissions)


@pytest.fixture(
    scope="module",
    params=[
        dedent("""
          auth:
            type: no_auth
          """),
        dedent("""
          auth:
            type: kubernetes
        """),
        dedent("""
          auth:
            type: oidc
            client_id: feast-integration-client
            client_secret: feast-integration-client-secret
            username: reader_writer
            password: password
            realm: master
            auth_server_url: KEYCLOAK_URL_PLACE_HOLDER
            auth_discovery_url: KEYCLOAK_URL_PLACE_HOLDER/realms/master/.well-known/openid-configuration
        """),
    ],
)
def auth_config(request):
    auth_configuration = request.param
    is_integration_test = request.node.get_closest_marker("integration")
    if is_integration_test is not None:
        if "kubernetes" in auth_configuration:
            pytest.skip("skipping integration tests for kubernetes platform.")
        elif "oidc" in auth_configuration:
            keycloak_url = request.getfixturevalue("start_keycloak_server")
            return auth_configuration.replace("KEYCLOAK_URL_PLACE_HOLDER", keycloak_url)

    return auth_configuration


@pytest.fixture(
    scope="module",
    params=[
        [],
        [invalid_list_entities_perm],
        [
            list_entities_perm,
            list_permissions_perm,
            list_fv_perm,
            list_odfv_perm,
            list_sfv_perm,
        ],
    ],
)
def applied_permissions(request):
    return request.param
