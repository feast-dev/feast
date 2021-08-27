# Copyright 2019 The Feast Authors
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
import multiprocessing
from datetime import datetime, timedelta
from sys import platform
from typing import Any, Callable

import pandas as pd
import pytest


def pytest_configure(config):
    if platform in ["darwin", "windows"]:
        multiprocessing.set_start_method("spawn")
    else:
        multiprocessing.set_start_method("fork")
    config.addinivalue_line(
        "markers", "integration: mark test that has external dependencies"
    )


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run tests with external dependencies",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration"):
        return
    skip_integration = pytest.mark.skip(
        reason="not running tests with external dependencies"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


@pytest.fixture
def simple_dataset_1() -> pd.DataFrame:
    now = datetime.utcnow()
    ts = pd.Timestamp(now).round("ms")
    data = {
        "id": [1, 2, 1, 3, 3],
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
        "id": ["a", "b", "c", "d", "e"],
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


class DataSourceCache(dict):
    """
    DataSourceCache is meant to cache datasources to be reused across multiple tests.

    Staging data into a remote store is an expensive operation, so we expose a way through which we can have the
    datasources created shared between tests.
    The key for the cache is an arbitrary string, which should be constructed using a combination of the test name along
    with the backing store.
    The value for the cache is a 4-valued Tuple:
        - the entites that are meant to be used with the data source created
        - The dataframe that is persisted into the remote store.
        - The data source object that represents the table/file; created from the dataframe mentioned above.
        - The data_source_creator object that is used to upload the data frame and construct the data source object.
          This object is needed so that we can invoke the teardown method at the end of all our tests.
    """

    def get_or_create(self, key, c: Callable[[], Any]):
        if key in self:
            return self[key]
        else:
            v = c()
            self[key] = v
            return v


@pytest.fixture(scope="function", autouse=True)
def data_source_cache():
    dsc = DataSourceCache()
    yield dsc
    for _, v in dsc.items():
        v[3].teardown()


@pytest.fixture(scope="session", autouse=True)
def universal_data_source_cache():
    dsc = DataSourceCache()
    yield dsc
    for _, v in dsc.items():
        v[3].teardown()
