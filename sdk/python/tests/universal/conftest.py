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
import importlib
import os
import random
from multiprocessing import Process
from typing import Any, Dict, List, Optional, Tuple, no_type_check
from unittest import mock

import pandas as pd
import pytest

from feast.data_source import DataSource
from feast.feature_store import FeatureStore  # noqa: E402
from feast.utils import _utc_now
from feast.wait import wait_retry_backoff  # noqa: E402
from tests.data.data_creator import (
    create_basic_driver_dataset,  # noqa: E402
    create_document_dataset,
    create_image_dataset,
)
from tests.utils.http_server import check_port_open, free_port  # noqa: E402

IntegrationTestRepoConfig: Any = None
Environment = Any
TestData = Any
AVAILABLE_OFFLINE_STORES: Any = None
AVAILABLE_ONLINE_STORES: Any = None
OFFLINE_STORE_TO_PROVIDER_CONFIG: Any = None
construct_test_environment: Any = None
construct_universal_feature_views: Any = None
construct_universal_test_data: Any = None
FileDataSourceCreator: Any = None
customer: Any = None
driver: Any = None
location: Any = None
_universal_deps_missing_reason: Optional[str] = None


def _load_universal_feature_repo_deps() -> bool:
    global IntegrationTestRepoConfig
    global Environment
    global TestData
    global AVAILABLE_OFFLINE_STORES
    global AVAILABLE_ONLINE_STORES
    global OFFLINE_STORE_TO_PROVIDER_CONFIG
    global construct_test_environment
    global construct_universal_feature_views
    global construct_universal_test_data
    global FileDataSourceCreator
    global customer
    global driver
    global location
    global _universal_deps_missing_reason

    if IntegrationTestRepoConfig is not None:
        return True

    try:
        integration_config = importlib.import_module(
            "tests.universal.feature_repos.integration_test_repo_config"
        )
        repo_configuration = importlib.import_module(
            "tests.universal.feature_repos.repo_configuration"
        )
        file_data_sources = importlib.import_module(
            "tests.universal.feature_repos.universal.data_sources.file"
        )
        entities = importlib.import_module(
            "tests.universal.feature_repos.universal.entities"
        )
    except ModuleNotFoundError as e:
        _universal_deps_missing_reason = (
            f"Optional integration test dependency is not installed: {e.name}"
        )
        return False

    IntegrationTestRepoConfig = integration_config.IntegrationTestRepoConfig
    Environment = repo_configuration.Environment
    TestData = repo_configuration.TestData
    AVAILABLE_OFFLINE_STORES = repo_configuration.AVAILABLE_OFFLINE_STORES
    AVAILABLE_ONLINE_STORES = repo_configuration.AVAILABLE_ONLINE_STORES
    OFFLINE_STORE_TO_PROVIDER_CONFIG = (
        repo_configuration.OFFLINE_STORE_TO_PROVIDER_CONFIG
    )
    construct_test_environment = repo_configuration.construct_test_environment
    construct_universal_feature_views = (
        repo_configuration.construct_universal_feature_views
    )
    construct_universal_test_data = repo_configuration.construct_universal_test_data
    FileDataSourceCreator = file_data_sources.FileDataSourceCreator
    customer = entities.customer
    driver = entities.driver
    location = entities.location
    _universal_deps_missing_reason = None
    return True


def _skip_missing_universal_feature_repo_deps() -> None:
    if not _load_universal_feature_repo_deps():
        pytest.skip(
            _universal_deps_missing_reason
            or "Optional integration test dependencies are not installed"
        )


def start_test_local_server(repo_path: str, port: int):
    fs = FeatureStore(repo_path)
    fs.serve(host="localhost", port=port)


@pytest.fixture
def environment(request, worker_id):
    _skip_missing_universal_feature_repo_deps()
    e = construct_test_environment(
        request.param,
        worker_id=worker_id,
        fixture_request=request,
    )

    e.setup()

    if hasattr(e.data_source_creator, "mock_environ"):
        with mock.patch.dict(os.environ, e.data_source_creator.mock_environ):
            yield e
    else:
        yield e

    e.teardown()


@pytest.fixture
def vectordb_environment(request, worker_id):
    _skip_missing_universal_feature_repo_deps()
    e = construct_test_environment(
        request.param,
        worker_id=worker_id,
        fixture_request=request,
        entity_key_serialization_version=3,
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
        if not _load_universal_feature_repo_deps():
            metafunc.parametrize(
                "environment",
                [
                    pytest.param(
                        None,
                        marks=pytest.mark.skip(
                            reason=_universal_deps_missing_reason
                            or "Optional integration test dependencies are not installed"
                        ),
                    )
                ],
                indirect=True,
                ids=["missing_optional_integration_deps"],
            )
            return

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
                            marks = [
                                pytest.mark.xdist_group(name=m)
                                for m in c.offline_store_creator.xdist_groups()
                            ]
                            # Check if there are any test markers associated with the creator and add them.
                            if c.offline_store_creator.test_markers():
                                marks.extend(c.offline_store_creator.test_markers())

                            _config_cache[c] = pytest.param(c, marks=marks)

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
    _skip_missing_universal_feature_repo_deps()
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
    _skip_missing_universal_feature_repo_deps()
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
        "driver_metadata": [None],
        "driver_config": [None],
        "driver_profile": [None],
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
def fake_image_data(environment: Environment) -> Tuple[pd.DataFrame, DataSource]:
    df = create_image_dataset()
    data_source = environment.data_source_creator.create_data_source(
        df,
        environment.feature_store.project,
    )
    return df, data_source