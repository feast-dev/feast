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
import time
from datetime import datetime, timedelta
from multiprocessing import Process
from sys import platform
from typing import List

import pandas as pd
import pytest
from _pytest.nodes import Item

from feast import FeatureStore
from tests.data.data_creator import create_dataset
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import (
    FULL_REPO_CONFIGS,
    REDIS_CONFIG,
    Environment,
    construct_test_environment,
    construct_universal_data_sources,
    construct_universal_datasets,
    construct_universal_entities,
)

logger = logging.getLogger(__name__)


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
        "markers", "universal: mark tests that use the universal feature repo"
    )


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run tests with external dependencies",
    )
    parser.addoption(
        "--benchmark", action="store_true", default=False, help="Run benchmark tests",
    )
    parser.addoption(
        "--universal", action="store_true", default=False, help="Run universal tests",
    )


def pytest_collection_modifyitems(config, items: List[Item]):
    should_run_integration = config.getoption("--integration") is True
    should_run_benchmark = config.getoption("--benchmark") is True
    should_run_universal = config.getoption("--universal") is True

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

    universal_tests = [t for t in items if "universal" in t.keywords]
    if should_run_universal:
        items.clear()
        for t in universal_tests:
            items.append(t)


@pytest.fixture
def simple_dataset_1() -> pd.DataFrame:
    now = datetime.utcnow()
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
    now = datetime.utcnow()
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
    fs.serve("localhost", port, no_access_log=True)


@pytest.fixture(
    params=FULL_REPO_CONFIGS, scope="session", ids=[str(c) for c in FULL_REPO_CONFIGS]
)
def environment(request, worker_id: str):
    e = construct_test_environment(request.param, worker_id=worker_id)
    proc = Process(
        target=start_test_local_server,
        args=(e.feature_store.repo_path, e.get_local_server_port()),
        daemon=True,
    )
    if e.python_feature_server and e.test_repo_config.provider == "local":
        proc.start()
        # Wait for server to start
        time.sleep(3)

    def cleanup():
        e.feature_store.teardown()
        if proc.is_alive():
            proc.kill()

    request.addfinalizer(cleanup)

    return e


@pytest.fixture()
def local_redis_environment(request, worker_id):
    e = construct_test_environment(
        IntegrationTestRepoConfig(online_store=REDIS_CONFIG), worker_id=worker_id
    )

    def cleanup():
        e.feature_store.teardown()

    request.addfinalizer(cleanup)
    return e


@pytest.fixture(scope="session")
def universal_data_sources(request, environment):
    entities = construct_universal_entities()
    datasets = construct_universal_datasets(
        entities, environment.start_date, environment.end_date
    )
    datasources = construct_universal_data_sources(
        datasets, environment.data_source_creator
    )

    def cleanup():
        # logger.info("Running cleanup in %s, Request: %s", worker_id, request.param)
        environment.data_source_creator.teardown()

    request.addfinalizer(cleanup)
    return entities, datasets, datasources


@pytest.fixture(scope="session")
def e2e_data_sources(environment: Environment, request):
    df = create_dataset()
    data_source = environment.data_source_creator.create_data_source(
        df, environment.feature_store.project, field_mapping={"ts_1": "ts"},
    )

    def cleanup():
        environment.data_source_creator.teardown()

    request.addfinalizer(cleanup)

    return df, data_source
