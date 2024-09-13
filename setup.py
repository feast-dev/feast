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

from pathlib import Path

from setuptools import find_packages, setup, Command
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop

# Get git repo root directory
repo_root = str(pathlib.Path(__file__).resolve().parent)

# Add Support for parsing tags that have a prefix containing '/' (ie 'sdk/go') to setuptools_scm.
# Regex modified from default tag regex in:
# https://github.com/pypa/setuptools_scm/blob/2a1b46d38fb2b8aeac09853e660bcd0d7c1bc7be/src/setuptools_scm/config.py#L9
TAG_REGEX = re.compile(
    r"^(?:[\/\w-]+)?(?P<version>[vV]?\d+(?:\.\d+){0,2}[^\+]*)(?:\+.*)?$"
)

if shutil.which("git"):
    use_scm_version = {"root": ".", "relative_to": __file__, "tag_regex": TAG_REGEX}
else:
    use_scm_version = None

PROTO_SUBDIRS = ["core", "registry", "serving", "types", "storage"]
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
    packages=find_packages(
        where=PYTHON_CODE_PREFIX, exclude=("java", "infra", "sdk/python/tests", "ui")
    ),
    package_dir={"": PYTHON_CODE_PREFIX},
    use_scm_version=use_scm_version,
    include_package_data=True,
    cmdclass={
        "build_python_protos": BuildPythonProtosCommand,
        "build_py": BuildCommand,
        "develop": DevelopCommand,
    },
)
