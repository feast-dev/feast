# Copyright 2018 The Feast Authors
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

import imp
import os
from setuptools import find_packages, setup, Command

NAME = "feast"
DESCRIPTION = "Python sdk for Feast"
URL = "https://github.com/gojek/feast"
AUTHOR = "Feast"
REQUIRES_PYTHON = ">=3.6.0"
VERSION = imp.load_source("feast.version", os.path.join("feast", "version.py")).VERSION
REQUIRED = [
    "google-api-core>=1.7.0",
    "google-auth>=1.6.0",
    "google-cloud-bigquery>=1.8.0",
    "google-cloud-storage>=1.13.0",
    "googleapis-common-protos>=1.5.5",
    "grpcio>=1.16.1",
    "pandas",
    "protobuf>=3.0.0",
    "PyYAML",
    "fastavro>=0.21.19"
]

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=("tests",)),
    install_requires=REQUIRED,
    include_package_data=True,
    license="Apache",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
)
