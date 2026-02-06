# Copyright 2024 The Feast Authors
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

import pytest


def test_validate_image_format_raises_when_deps_missing(monkeypatch):
    from feast import image_utils

    monkeypatch.setattr(image_utils, "_image_dependencies_available", False)

    with pytest.raises(
        ImportError, match="Image processing dependencies are not installed"
    ):
        image_utils.validate_image_format(b"anything")


def test_get_image_metadata_raises_when_deps_missing(monkeypatch):
    from feast import image_utils

    monkeypatch.setattr(image_utils, "_image_dependencies_available", False)

    with pytest.raises(
        ImportError, match="Image processing dependencies are not installed"
    ):
        image_utils.get_image_metadata(b"anything")
