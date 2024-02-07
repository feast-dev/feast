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
import glob
import os
import pathlib
import re
import shutil
import subprocess
import sys
from distutils.cmd import Command
from pathlib import Path

from setuptools import find_packages

try:
    from setuptools import setup
    from setuptools.command.build_ext import build_ext as _build_ext
    from setuptools.command.build_py import build_py
    from setuptools.command.develop import develop
    from setuptools.command.install import install

except ImportError:
    from distutils.command.build_ext import build_ext as _build_ext
    from distutils.command.build_py import build_py
    from distutils.core import setup

NAME = "feast"
DESCRIPTION = "Python SDK for Feast"
URL = "https://github.com/feast-dev/feast"
AUTHOR = "Feast"
REQUIRES_PYTHON = ">=3.8.0"

REQUIRED = [
    "click>=7.0.0,<9.0.0",
    "colorama>=0.3.9,<1",
    "dill~=0.3.0",
    "fastavro>=1.1.0,<2",
    "grpcio>=1.56.2,<2",
    "grpcio-tools>=1.56.2,<2",
    "grpcio-reflection>=1.56.2,<2",
    "grpcio-health-checking>=1.56.2,<2",
    "mypy-protobuf==3.1",
    "Jinja2>=2,<4",
    "jsonschema",
    "mmh3",
    "numpy>=1.22,<3",
    "pandas>=1.4.3,<2",
    # For some reason pandavro higher than 1.5.* only support pandas less than 1.3.
    "pandavro~=1.5.0",
    # Higher than 4.23.4 seems to cause a seg fault
    "protobuf<4.23.4,>3.20",
    "proto-plus>=1.20.0,<2",
    "pyarrow>=4,<12",
    "pydantic>=1,<2",
    "pygments>=2.12.0,<3",
    "PyYAML>=5.4.0,<7",
    "requests",
    "SQLAlchemy[mypy]>1,<2",
    "tabulate>=0.8.0,<1",
    "tenacity>=7,<9",
    "toml>=0.10.0,<1",
    "tqdm>=4,<5",
    "typeguard==2.13.3",
    "fastapi>=0.68.0,<0.100",
    "uvicorn[standard]>=0.14.0,<1",
    "gunicorn",
    "dask>=2021.1.0",
    "bowler",  # Needed for automatic repo upgrades
    # FastAPI does not correctly pull starlette dependency on httpx see thread(https://github.com/tiangolo/fastapi/issues/5656).
    "httpx>=0.23.3",
]

GCP_REQUIRED = [
    "google-api-core>=1.23.0,<3",
    "googleapis-common-protos>=1.52.0,<2",
    "google-cloud-bigquery[pandas]>=2,<4",
    "google-cloud-bigquery-storage >= 2.0.0,<3",
    "google-cloud-datastore>=2.1.0,<3",
    "google-cloud-storage>=1.34.0,<3",
    "google-cloud-bigtable>=2.11.0,<3",
]

REDIS_REQUIRED = [
    "redis>=4.2.2,<5",
    "hiredis>=2.0.0,<3",
]

AWS_REQUIRED = ["boto3>=1.17.0,<2", "docker>=5.0.2"]

BYTEWAX_REQUIRED = ["bytewax==0.15.1", "docker>=5.0.2", "kubernetes<=20.13.0"]

SNOWFLAKE_REQUIRED = [
    "snowflake-connector-python[pandas]>=3,<4",
]

SPARK_REQUIRED = [
    "pyspark>=3.0.0,<4",
]

TRINO_REQUIRED = ["trino>=0.305.0,<0.400.0", "regex"]

POSTGRES_REQUIRED = [
    "psycopg2-binary>=2.8.3,<3",
]

MYSQL_REQUIRED = ["mysqlclient", "pymysql", "types-PyMySQL"]

HBASE_REQUIRED = [
    "happybase>=1.2.0,<3",
]

CASSANDRA_REQUIRED = [
    "cassandra-driver>=3.24.0,<4",
]

GE_REQUIRED = ["great_expectations>=0.15.41,<0.16.0"]

AZURE_REQUIRED = [
    "azure-storage-blob>=0.37.0",
    "azure-identity>=1.6.1",
    "SQLAlchemy>=1.4.19",
    "pyodbc>=4.0.30",
    "pymssql",
]

ROCKSET_REQUIRED = [
    "rockset>=1.0.3",
]

HAZELCAST_REQUIRED = [
    "hazelcast-python-client>=5.1",
]

CI_REQUIRED = (
    [
        "build",
        "virtualenv==20.23.0",
        "cryptography>=35.0,<42",
        "flake8>=6.0.0,<6.1.0",
        "black>=22.6.0,<23",
        "isort>=5,<6",
        "grpcio-testing>=1.56.2,<2",
        "minio==7.1.0",
        "mock==2.0.0",
        "moto",
        "mypy>=0.981,<0.990",
        "avro==1.10.0",
        "gcsfs>=0.4.0,<=2022.01.0",
        "urllib3>=1.25.4,<2",
        "psutil==5.9.0",
        "py>=1.11.0",  # https://github.com/pytest-dev/pytest/issues/10420
        "pytest>=6.0.0,<8",
        "pytest-cov",
        "pytest-xdist",
        "pytest-benchmark>=3.4.1,<4",
        "pytest-lazy-fixture==0.6.3",
        "pytest-timeout==1.4.2",
        "pytest-ordering~=0.6.0",
        "pytest-mock==1.10.4",
        "Sphinx>4.0.0,<7",
        "testcontainers>=3.5,<4",
        "adlfs==0.5.9",
        "firebase-admin>=5.2.0,<6",
        "pre-commit<3.3.2",
        "assertpy==1.1",
        "pip-tools",
        "pybindgen",
        "types-protobuf~=3.19.22",
        "types-python-dateutil",
        "types-pytz",
        "types-PyYAML",
        "types-redis",
        "types-requests",
        "types-setuptools",
        "types-tabulate",
        "virtualenv<20.24.2"
    ]
    + GCP_REQUIRED
    + REDIS_REQUIRED
    + AWS_REQUIRED
    + BYTEWAX_REQUIRED
    + SNOWFLAKE_REQUIRED
    + SPARK_REQUIRED
    + POSTGRES_REQUIRED
    + MYSQL_REQUIRED
    + TRINO_REQUIRED
    + GE_REQUIRED
    + HBASE_REQUIRED
    + CASSANDRA_REQUIRED
    + AZURE_REQUIRED
    + ROCKSET_REQUIRED
    + HAZELCAST_REQUIRED
)


# rtd builds fail because of mysql not being installed in their environment.
# We can add mysql there, but it's not strictly needed. This will be faster for builds.
DOCS_REQUIRED = CI_REQUIRED.copy()
for _r in MYSQL_REQUIRED:
    DOCS_REQUIRED.remove(_r)

DEV_REQUIRED = ["mypy-protobuf==3.1", "grpcio-testing~=1.0"] + CI_REQUIRED

# Get git repo root directory
repo_root = str(pathlib.Path(__file__).resolve().parent)

# README file from Feast repo root directory
README_FILE = os.path.join(repo_root, "README.md")
with open(README_FILE, "r", encoding="utf8") as f:
    LONG_DESCRIPTION = f.read()

# Add Support for parsing tags that have a prefix containing '/' (ie 'sdk/go') to setuptools_scm.
# Regex modified from default tag regex in:
# https://github.com/pypa/setuptools_scm/blob/2a1b46d38fb2b8aeac09853e660bcd0d7c1bc7be/src/setuptools_scm/config.py#L9
TAG_REGEX = re.compile(
    r"^(?:[\/\w-]+)?(?P<version>[vV]?\d+(?:\.\d+){0,2}[^\+]*)(?:\+.*)?$"
)

# Only set use_scm_version if git executable exists (setting this variable causes pip to use git under the hood)
if shutil.which("git"):
    use_scm_version = {"root": ".", "relative_to": __file__, "tag_regex": TAG_REGEX}
else:
    use_scm_version = None

PROTO_SUBDIRS = ["core", "serving", "types", "storage"]
PYTHON_CODE_PREFIX = "sdk/python"


class BuildPythonProtosCommand(Command):
    description = "Builds the proto files into Python files."
    user_options = [
        ("inplace", "i", "Write generated proto files to source directory."),
    ]

    def initialize_options(self):
        self.python_protoc = [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
        ]  # find_executable("protoc")
        self.proto_folder = os.path.join(repo_root, "protos")
        self.sub_folders = PROTO_SUBDIRS
        self.build_lib = None
        self.inplace = 0

    def finalize_options(self):
        self.set_undefined_options("build", ("build_lib", "build_lib"))

    @property
    def python_folder(self):
        if self.inplace:
            return os.path.join(
                os.path.dirname(__file__) or os.getcwd(), "sdk/python/feast/protos"
            )

        return os.path.join(self.build_lib, "feast/protos")

    def _generate_python_protos(self, path: str):
        proto_files = glob.glob(os.path.join(self.proto_folder, path))
        Path(self.python_folder).mkdir(parents=True, exist_ok=True)
        subprocess.check_call(
            self.python_protoc
            + [
                "-I",
                self.proto_folder,
                "--python_out",
                self.python_folder,
                "--grpc_python_out",
                self.python_folder,
                "--mypy_out",
                self.python_folder,
            ]
            + proto_files
        )

    def run(self):
        for sub_folder in self.sub_folders:
            self._generate_python_protos(f"feast/{sub_folder}/*.proto")
            # We need the __init__ files for each of the generated subdirs
            # so that they are regular packages, and don't need the `--namespace-packages` flags
            # when being typechecked using mypy.
            with open(f"{self.python_folder}/feast/{sub_folder}/__init__.py", "w"):
                pass

        with open(f"{self.python_folder}/__init__.py", "w"):
            pass
        with open(f"{self.python_folder}/feast/__init__.py", "w"):
            pass

        for path in Path(self.python_folder).rglob("*.py"):
            for folder in self.sub_folders:
                # Read in the file
                with open(path, "r") as file:
                    filedata = file.read()

                # Replace the target string
                filedata = filedata.replace(
                    f"from feast.{folder}", f"from feast.protos.feast.{folder}"
                )

                # Write the file out again
                with open(path, "w") as file:
                    file.write(filedata)


class BuildCommand(build_py):
    """Custom build command."""

    def run(self):
        self.run_command("build_python_protos")

        self.run_command("build_ext")
        build_py.run(self)


class DevelopCommand(develop):
    """Custom develop command."""

    def run(self):
        self.reinitialize_command("build_python_protos", inplace=1)
        self.run_command("build_python_protos")

        develop.run(self)


setup(
    name=NAME,
    author=AUTHOR,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(
        where=PYTHON_CODE_PREFIX, exclude=("java", "infra", "sdk/python/tests", "ui")
    ),
    package_dir={"": PYTHON_CODE_PREFIX},
    install_requires=REQUIRED,
    # https://stackoverflow.com/questions/28509965/setuptools-development-requirements
    # Install dev requirements with: pip install -e .[dev]
    extras_require={
        "dev": DEV_REQUIRED,
        "ci": CI_REQUIRED,
        "gcp": GCP_REQUIRED,
        "aws": AWS_REQUIRED,
        "bytewax": BYTEWAX_REQUIRED,
        "redis": REDIS_REQUIRED,
        "snowflake": SNOWFLAKE_REQUIRED,
        "spark": SPARK_REQUIRED,
        "trino": TRINO_REQUIRED,
        "postgres": POSTGRES_REQUIRED,
        "azure": AZURE_REQUIRED,
        "mysql": MYSQL_REQUIRED,
        "ge": GE_REQUIRED,
        "hbase": HBASE_REQUIRED,
        "docs": DOCS_REQUIRED,
        "cassandra": CASSANDRA_REQUIRED,
        "hazelcast": HAZELCAST_REQUIRED,
        "rockset": ROCKSET_REQUIRED,
    },
    include_package_data=True,
    license="Apache",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    entry_points={"console_scripts": ["feast=feast.cli:cli"]},
    use_scm_version=use_scm_version,
    setup_requires=[
        "setuptools_scm",
        "grpcio>=1.56.2,<2",
        "grpcio-tools>=1.56.2,<2",
        "mypy-protobuf==3.1",
        "pybindgen==0.22.0",
    ],
    cmdclass={
        "build_python_protos": BuildPythonProtosCommand,
        "build_py": BuildCommand,
        "develop": DevelopCommand,
    },
)
