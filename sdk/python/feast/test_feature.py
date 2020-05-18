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

from feast.feature import FeatureRef


class TestFeatureRef:
    def test_str_ref(self):
        original_ref = FeatureRef(project="test", name="test")
        ref_str = repr(original_ref)
        parsed_ref = FeatureRef.from_str(ref_str)
        assert original_ref == parsed_ref
