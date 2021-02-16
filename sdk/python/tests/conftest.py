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
from sys import platform

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
