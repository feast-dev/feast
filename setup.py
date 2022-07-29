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
import copy
import glob
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
from distutils.cmd import Command
from distutils.dir_util import copy_tree
from pathlib import Path
from subprocess import CalledProcessError

from setuptools import Extension, find_packages

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
REQUIRES_PYTHON = ">=3.7.0"

REQUIRED = [
    "click>=7.0.0,<9.0.0",
    "colorama>=0.3.9,<1",
    "dill==0.3.*",
    "fastavro>=1.1.0,<2",
    "google-api-core>=1.23.0,<3",
    "googleapis-common-protos>=1.52.*,<2",
    "grpcio>=1.47.0,<2",
    "grpcio-reflection>=1.47.0,<2",
    "Jinja2>=2,<4",
    "jsonschema",
    "mmh3",
    "numpy>=1.22,<3",
    "pandas>=1.4.3,<2",
    "pandavro==1.5.*", # For some reason pandavro higher than 1.5.* only support pandas less than 1.3.
    "protobuf>3.20,<4",
    "proto-plus>=1.20.0,<2",
    "pyarrow>=4,<9",
    "pydantic>=1,<2",
    "pygments>=2.12.0,<3",
    "PyYAML>=5.4.*,<7",
    "SQLAlchemy[mypy]>1,<2",
    "tabulate>=0.8.0,<1",
    "tenacity>=7,<9",
    "toml>=0.10.0,<1",
    "tqdm>=4,<5",
    "typeguard",
    "fastapi>=0.68.0,<1",
    "uvicorn[standard]>=0.14.0,<1",
    "tensorflow-metadata>=1.0.0,<2.0.0",
    "dask>=2021.*,<2022.02.0",
    "bowler",  # Needed for automatic repo upgrades
]

GCP_REQUIRED = [
    "google-cloud-bigquery[pandas]>=2,<4",
    "google-cloud-bigquery-storage >= 2.0.0,<3",
    "google-cloud-datastore>=2.1.*,<3",
    "google-cloud-storage>=1.34.*,<3",
]

REDIS_REQUIRED = [
    "redis==4.2.2",
    "hiredis>=2.0.0,<3",
]

AWS_REQUIRED = ["boto3>=1.17.0,<=1.20.23", "docker>=5.0.2", "s3fs>=0.4.0,<=2022.01.0"]

SNOWFLAKE_REQUIRED = [
    "snowflake-connector-python[pandas]>=2.7.3,<=2.7.8",
]

SPARK_REQUIRED = [
    "pyspark>=3.0.0,<4",
]

TRINO_REQUIRED = [
    "trino>=0.305.0,<0.400.0",
]

POSTGRES_REQUIRED = [
    "psycopg2-binary>=2.8.3,<3",
]

MYSQL_REQUIRED = [
    "mysqlclient",
]

HBASE_REQUIRED = [
    "happybase>=1.2.0,<3",
]

GE_REQUIRED = ["great_expectations>=0.14.0,<0.15.0"]

GO_REQUIRED = [
    "cffi==1.15.*,<2",
]

CI_REQUIRED = (
    [
        "build",
        "cryptography>=35.0,<36",
        "flake8",
        "black>=22.6.0,<23",
        "isort>=5,<6",
        "grpcio-tools>=1.47.0",
        "grpcio-testing>=1.47.0",
        "minio==7.1.0",
        "mock==2.0.0",
        "moto",
        "mypy>=0.931",
        "mypy-protobuf==3.1",
        "avro==1.10.0",
        "gcsfs>=0.4.0,<=2022.01.0",
        "urllib3>=1.25.4,<2",
        "psutil==5.9.0",
        "pytest>=6.0.0,<8",
        "pytest-cov",
        "pytest-xdist",
        "pytest-benchmark>=3.4.1,<4",
        "pytest-lazy-fixture==0.6.3",
        "pytest-timeout==1.4.2",
        "pytest-ordering==0.6.*",
        "pytest-mock==1.10.4",
        "Sphinx!=4.0.0,<4.4.0",
        "sphinx-rtd-theme",
        "testcontainers>=3.5,<4",
        "adlfs==0.5.9",
        "firebase-admin>=5.2.0,<6",
        "pre-commit",
        "assertpy==1.1",
        "pip-tools",
        "pybindgen",
        "types-protobuf",
        "types-python-dateutil",
        "types-pytz",
        "types-PyYAML",
        "types-redis",
        "types-requests",
        "types-setuptools",
        "types-tabulate",
    ]
    + GCP_REQUIRED
    + REDIS_REQUIRED
    + AWS_REQUIRED
    + SNOWFLAKE_REQUIRED
    + SPARK_REQUIRED
    + POSTGRES_REQUIRED
    + MYSQL_REQUIRED
    + TRINO_REQUIRED
    + GE_REQUIRED
    + HBASE_REQUIRED
)


# rtd builds fail because of mysql not being installed in their environment.
# We can add mysql there, but it's not strictly needed. This will be faster for builds.
DOCS_REQUIRED = CI_REQUIRED.copy()
for _r in MYSQL_REQUIRED:
    DOCS_REQUIRED.remove(_r)

DEV_REQUIRED = ["mypy-protobuf==3.1", "grpcio-testing==1.*"] + CI_REQUIRED

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


def _generate_path_with_gopath():
    go_path = subprocess.check_output(["go", "env", "GOPATH"]).decode("utf-8")
    go_path = go_path.strip()
    path_val = os.getenv("PATH")
    path_val = f"{path_val}:{go_path}/bin"

    return path_val


def _ensure_go_and_proto_toolchain():
    try:
        version = subprocess.check_output(["go", "version"])
    except Exception as e:
        raise RuntimeError("Unable to find go toolchain") from e

    semver_string = re.search(r"go[\S]+", str(version)).group().lstrip("go")
    parts = semver_string.split(".")
    if not (int(parts[0]) >= 1 and int(parts[1]) >= 16):
        raise RuntimeError(f"Go compiler too old; expected 1.16+ found {semver_string}")

    path_val = _generate_path_with_gopath()

    try:
        subprocess.check_call(["protoc-gen-go", "--version"], env={"PATH": path_val})
        subprocess.check_call(
            ["protoc-gen-go-grpc", "--version"], env={"PATH": path_val}
        )
    except Exception as e:
        raise RuntimeError("Unable to find go/grpc extensions for protoc") from e


class BuildGoProtosCommand(Command):
    description = "Builds the proto files into Go files."
    user_options = []

    def initialize_options(self):
        self.go_protoc = [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
        ]  # find_executable("protoc")
        self.proto_folder = os.path.join(repo_root, "protos")
        self.go_folder = os.path.join(repo_root, "go/protos")
        self.sub_folders = PROTO_SUBDIRS
        self.path_val = _generate_path_with_gopath()

    def finalize_options(self):
        pass

    def _generate_go_protos(self, path: str):
        proto_files = glob.glob(os.path.join(self.proto_folder, path))

        try:
            subprocess.check_call(
                self.go_protoc
                + [
                    "-I",
                    self.proto_folder,
                    "--go_out",
                    self.go_folder,
                    "--go_opt=module=github.com/feast-dev/feast/go/protos",
                    "--go-grpc_out",
                    self.go_folder,
                    "--go-grpc_opt=module=github.com/feast-dev/feast/go/protos",
                ]
                + proto_files,
                env={"PATH": self.path_val},
            )
        except CalledProcessError as e:
            print(f"Stderr: {e.stderr}")
            print(f"Stdout: {e.stdout}")

    def run(self):
        go_dir = Path(repo_root) / "go" / "protos"
        go_dir.mkdir(exist_ok=True)
        for sub_folder in self.sub_folders:
            self._generate_go_protos(f"feast/{sub_folder}/*.proto")


class BuildCommand(build_py):
    """Custom build command."""

    def run(self):
        self.run_command("build_python_protos")
        if os.getenv("COMPILE_GO", "false").lower() == "true":
            _ensure_go_and_proto_toolchain()
            self.run_command("build_go_protos")

        self.run_command("build_ext")
        build_py.run(self)


class DevelopCommand(develop):
    """Custom develop command."""

    def run(self):
        self.reinitialize_command("build_python_protos", inplace=1)
        self.run_command("build_python_protos")
        if os.getenv("COMPILE_GO", "false").lower() == "true":
            _ensure_go_and_proto_toolchain()
            self.run_command("build_go_protos")

        develop.run(self)


class build_ext(_build_ext):
    def finalize_options(self) -> None:
        super().finalize_options()
        if os.getenv("COMPILE_GO", "false").lower() == "false":
            self.extensions = [e for e in self.extensions if not self._is_go_ext(e)]

    def _is_go_ext(self, ext: Extension):
        return any(
            source.endswith(".go") or source.startswith("github")
            for source in ext.sources
        )

    def build_extension(self, ext: Extension):
        print(f"Building extension {ext}")
        if not self._is_go_ext(ext):
            # the base class may mutate `self.compiler`
            compiler = copy.deepcopy(self.compiler)
            self.compiler, compiler = compiler, self.compiler
            try:
                return _build_ext.build_extension(self, ext)
            finally:
                self.compiler, compiler = compiler, self.compiler

        bin_path = _generate_path_with_gopath()
        go_env = json.loads(
            subprocess.check_output(["go", "env", "-json"]).decode("utf-8").strip()
        )

        print(f"Go env: {go_env}")
        print(f"CWD: {os.getcwd()}")

        destination = os.path.dirname(os.path.abspath(self.get_ext_fullpath(ext.name)))
        subprocess.check_call(
            ["go", "install", "golang.org/x/tools/cmd/goimports"],
            env={"PATH": bin_path, **go_env},
        )
        subprocess.check_call(
            ["go", "get", "github.com/go-python/gopy@v0.4.4"],
            env={"PATH": bin_path, **go_env},
        )
        subprocess.check_call(
            ["go", "install", "github.com/go-python/gopy"],
            env={"PATH": bin_path, **go_env},
        )
        subprocess.check_call(
            [
                "gopy",
                "build",
                "-output",
                destination,
                "-vm",
                sys.executable,
                "--build-tags",
                "cgo,ccalloc",
                "--dynamic-link=True",
                "-no-make",
                *ext.sources,
            ],
            env={
                "PATH": bin_path,
                "CGO_LDFLAGS_ALLOW": ".*",
                **go_env,
            },
        )

    def copy_extensions_to_source(self):
        build_py = self.get_finalized_command("build_py")
        for ext in self.extensions:
            fullname = self.get_ext_fullname(ext.name)
            modpath = fullname.split(".")
            package = ".".join(modpath[:-1])
            package_dir = build_py.get_package_dir(package)

            src_dir = dest_dir = package_dir

            if src_dir.startswith(PYTHON_CODE_PREFIX):
                src_dir = package_dir[len(PYTHON_CODE_PREFIX) :]
            src_dir = src_dir.lstrip("/")

            src_dir = os.path.join(self.build_lib, src_dir)

            # copy whole directory
            print(f"Copying from {src_dir} to {dest_dir}")
            copy_tree(src_dir, dest_dir)


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
        "redis": REDIS_REQUIRED,
        "snowflake": SNOWFLAKE_REQUIRED,
        "spark": SPARK_REQUIRED,
        "trino": TRINO_REQUIRED,
        "postgres": POSTGRES_REQUIRED,
        "mysql": MYSQL_REQUIRED,
        "ge": GE_REQUIRED,
        "hbase": HBASE_REQUIRED,
        "go": GO_REQUIRED,
        "docs": DOCS_REQUIRED,
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
        "grpcio>=1.47.0",
        "grpcio-tools>=1.47.0",
        "mypy-protobuf==3.1",
        "pybindgen==0.22.0",
        "sphinx!=4.0.0",
    ],
    cmdclass={
        "build_python_protos": BuildPythonProtosCommand,
        "build_go_protos": BuildGoProtosCommand,
        "build_py": BuildCommand,
        "develop": DevelopCommand,
        "build_ext": build_ext,
    },
    ext_modules=[
        Extension(
            "feast.embedded_go.lib._embedded",
            ["github.com/feast-dev/feast/go/embedded"],
        )
    ],
)
