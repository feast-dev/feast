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
from feast.feature_set import FeatureSet
import yaml


def from_yaml(resouce_yaml: str):
    resource_dict = yaml.safe_load(resouce_yaml)
    if isinstance(resource_dict, dict) and "kind" in resource_dict:
        if resource_dict["kind"].strip() == "feature_set":
            return FeatureSet.from_yaml(resource_dict)
