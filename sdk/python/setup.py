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
import re
import shutil
import subprocess
import pathlib

from distutils.cmd import Command
from setuptools import find_packages

try:
    from setuptools import setup
    from setuptools.command.install import install
    from setuptools.command.develop import develop
    from setuptools.command.egg_info import egg_info
    from setuptools.command.sdist import sdist
    from setuptools.command.build_py import build_py
except ImportError:
    from distutils.core import setup
    from distutils.command.install import install
    from distutils.command.build_py import build_py

NAME = "feast"
DESCRIPTION = "Python SDK for Feast"
URL = "https://github.com/feast-dev/feast"
AUTHOR = "Feast"
REQUIRES_PYTHON = ">=3.7.0"

REQUIRED = [
    "Click==7.*",
    "colorama>=0.3.9",
    "dill==0.3.*",
    "fastavro>=1.1.0",
    "google-api-core>=1.23.0",
    "googleapis-common-protos==1.52.*",
    "grpcio>=1.34.0",
    "Jinja2>=2.0.0",
    "jsonschema",
    "mmh3",
    "pandas>=1.0.0",
    "pandavro==1.5.*",
    "protobuf>=3.10",
    "pyarrow>=2.0.0",
    "pydantic>=1.0.0",
    "PyYAML>=5.4.*",
    "tabulate==0.8.*",
    "tenacity>=7.*",
    "toml==0.10.*",
    "tqdm==4.*",
    "fastapi>=0.68.0",
    "uvicorn[standard]>=0.14.0",
]

GCP_REQUIRED = [
    "google-cloud-bigquery>=2.0.*",
    "google-cloud-bigquery-storage >= 2.0.0",
    "google-cloud-datastore>=2.1.*",
    "google-cloud-storage>=1.34.*",
    "google-cloud-core==1.4.*",
]

REDIS_REQUIRED = [
    "redis-py-cluster==2.1.2",
]

AWS_REQUIRED = [
    "boto3==1.17.*",
]

CI_REQUIRED = [
    "cryptography==3.3.2",
    "flake8",
    "black==19.10b0",
    "isort>=5",
    "grpcio-tools==1.34.0",
    "grpcio-testing==1.34.0",
    "minio==7.1.0",
    "mock==2.0.0",
    "moto",
    "mypy==0.790",
    "mypy-protobuf==1.24",
    "avro==1.10.0",
    "gcsfs",
    "urllib3>=1.25.4",
    "pytest==6.0.0",
    "pytest-cov",
    "pytest-xdist",
    "pytest-benchmark>=3.4.1",
    "pytest-lazy-fixture==0.6.3",
    "pytest-timeout==1.4.2",
    "pytest-ordering==0.6.*",
    "pytest-mock==1.10.4",
    "Sphinx!=4.0.0",
    "sphinx-rtd-theme",
    "testcontainers==3.4.2",
    "adlfs==0.5.9",
    "firebase-admin==4.5.2",
    "pre-commit",
    "assertpy==1.1",
    "google-cloud-bigquery>=2.0.*",
    "google-cloud-bigquery-storage >= 2.0.0",
    "google-cloud-datastore>=2.1.*",
    "google-cloud-storage>=1.20.*",
    "google-cloud-core==1.4.*",
    "redis-py-cluster==2.1.2",
    "boto3==1.17.*",
]

# Get git repo root directory
repo_root = str(pathlib.Path(__file__).resolve().parent.parent.parent)

# README file from Feast repo root directory
README_FILE = os.path.join(repo_root, "README.md")
with open(README_FILE, "r") as f:
    LONG_DESCRIPTION = f.read()

# Add Support for parsing tags that have a prefix containing '/' (ie 'sdk/go') to setuptools_scm.
# Regex modified from default tag regex in:
# https://github.com/pypa/setuptools_scm/blob/2a1b46d38fb2b8aeac09853e660bcd0d7c1bc7be/src/setuptools_scm/config.py#L9
TAG_REGEX = re.compile(
    r"^(?:[\/\w-]+)?(?P<version>[vV]?\d+(?:\.\d+){0,2}[^\+]*)(?:\+.*)?$"
)

# Only set use_scm_version if git executable exists (setting this variable causes pip to use git under the hood)
if shutil.which("git"):
    use_scm_version = {"root": "../..", "relative_to": __file__, "tag_regex": TAG_REGEX}
else:
    use_scm_version = None


class BuildProtoCommand(Command):
    description = "Builds the proto files into python files."

    def initialize_options(self):
        self.protoc = ["python", "-m", "grpc_tools.protoc"]  # find_executable("protoc")
        self.proto_folder = os.path.join(repo_root, "protos")
        self.this_package = os.path.join(os.path.dirname(__file__) or os.getcwd(), 'feast/protos')
        self.sub_folders = ["core", "serving", "types", "storage"]

    def finalize_options(self):
        pass

    def _generate_protos(self, path):
        proto_files = glob.glob(os.path.join(self.proto_folder, path))

        subprocess.check_call(self.protoc + [
            '-I', self.proto_folder,
            '--python_out', self.this_package,
            '--grpc_python_out', self.this_package,
            '--mypy_out', self.this_package] + proto_files)

    def run(self):
        for sub_folder in self.sub_folders:
            self._generate_protos(f'feast/{sub_folder}/*.proto')

        from pathlib import Path

        for path in Path('feast/protos').rglob('*.py'):
            for folder in self.sub_folders:
                # Read in the file
                with open(path, 'r') as file:
                    filedata = file.read()

                # Replace the target string
                filedata = filedata.replace(f'from feast.{folder}', f'from feast.protos.feast.{folder}')

                # Write the file out again
                with open(path, 'w') as file:
                    file.write(filedata)


class BuildCommand(build_py):
    """Custom build command."""

    def run(self):
        self.run_command('build_proto')
        build_py.run(self)


class DevelopCommand(develop):
    """Custom develop command."""

    def run(self):
        self.run_command('build_proto')
        develop.run(self)


setup(
    name=NAME,
    author=AUTHOR,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=("tests",)),
    install_requires=REQUIRED,
    # https://stackoverflow.com/questions/28509965/setuptools-development-requirements
    # Install dev requirements with: pip install -e .[dev]
    extras_require={
        "dev": ["mypy-protobuf==1.*", "grpcio-testing==1.*"],
        "ci": CI_REQUIRED,
        "gcp": GCP_REQUIRED,
        "aws": AWS_REQUIRED,
        "redis": REDIS_REQUIRED,
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
    setup_requires=["setuptools_scm", "grpcio", "grpcio-tools==1.34.0", "mypy-protobuf", "sphinx!=4.0.0"],
    package_data={
        "": [
            "protos/feast/**/*.proto",
            "protos/feast/third_party/grpc/health/v1/*.proto",
            "protos/tensorflow_metadata/proto/v0/*.proto",
            "feast/protos/feast/**/*.py",
            "tensorflow_metadata/proto/v0/*.py"
        ],
    },
    cmdclass={
        "build_proto": BuildProtoCommand,
        "build_py": BuildCommand,
        "develop": DevelopCommand,
    },
)
