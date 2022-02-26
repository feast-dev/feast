# Copyright 2022 The Feast Authors
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
import os
import pathlib
import shutil
import subprocess

# Build go server for 3 targets: macos (intel), macos (m1), linux (64 bit)
# First start by clearing the necessary directory
binaries_path = (pathlib.Path(__file__) / "../feast/binaries").resolve()
binaries_path_abs = str(binaries_path.absolute())
if binaries_path.exists():
    shutil.rmtree(binaries_path_abs)
os.mkdir(binaries_path_abs)
# Then, iterate over target architectures and build executables
for goos, goarch in (("darwin", "amd64"), ("darwin", "arm64"), ("linux", "amd64")):
    subprocess.check_output(
        [
            "go",
            "build",
            "-o",
            f"{binaries_path_abs}/go_server_{goos}_{goarch}",
            "github.com/feast-dev/feast/go/server",
        ],
        env={"GOOS": goos, "GOARCH": goarch, **os.environ},
    )
