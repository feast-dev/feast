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

from feast.sdk.resources.feature_group import FeatureGroup


class TestFeatureGroupSpec(object):
    def test_read_from_yaml(self):
        feature_group = FeatureGroup.from_yaml(
            "tests/sample/valid_feature_group.yaml")
        assert feature_group.id == "my_fg"
        assert feature_group.tags == ["tag1", "tag2"]
