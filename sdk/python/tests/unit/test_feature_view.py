# Copyright 2020 The Feast Authors
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

from datetime import timedelta

from feast import FeatureView, FileSource


def test_with_name_method():
    test_fv = FeatureView(
        name="test_fv",
        entities=["entity"],
        ttl=timedelta(days=1),
        batch_source=FileSource(path="non_existent"),
    )

    test_fv_2 = test_fv.with_name("test_fv_2")

    assert test_fv.name == "test_fv"
    assert test_fv_2.name == "test_fv_2"
