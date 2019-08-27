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

from setuptools import find_packages, setup

NAME = "feast"
DESCRIPTION = "Python sdk for Feast"
URL = "https://github.com/gojek/feast"
AUTHOR = "Feast"
REQUIRES_PYTHON = ">=3.6.0"
VERSION_FILE = open(os.path.join("../../", "VERSION"))
VERSION = VERSION_FILE.read().strip()

REQUIRED = [
    "google-api-core==1.*",
    "google-auth==1.*",
    "google-cloud-bigquery==1.*",
    "google-cloud-storage==1.*",
    "googleapis-common-protos==1.*",
    "google-cloud-bigquery-storage==0.*",
    "grpcio==1.*",
    "pandas==0.*",
    "protobuf==3.*",
    "PyYAML==3.*",
    "fastavro==0.*",
    "kafka-python==1.4.*",
    "tqdm==4.*",
    'numpy'
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
    # https://stackoverflow.com/questions/28509965/setuptools-development-requirements
    # Install dev requirements with: pip install -e .[dev]
    extras_require={"dev": ["mypy-protobuf==1.*", "grpcio-testing==1.*"]},
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
