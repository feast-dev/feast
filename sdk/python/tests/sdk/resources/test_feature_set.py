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

import pytest
from feast.sdk.resources.feature_set import FeatureSet, DatasetInfo


class TestFeatureSet(object):
    def test_features(self):
        entity_name = "driver"
        features = ["driver.hour.feature1", "driver.hour.feature2"]

        feature_set = FeatureSet(entity_name, features)
        assert len(feature_set.features) == 2

        assert len(set(feature_set.features) & set(features)) == 2

    def test_different_entity(self):
        entity_name = "driver"
        features = ["customer.hour.feature1", "driver.day.feature1"]
        with pytest.raises(
                ValueError,
                match="feature set has different entity: customer"):
            FeatureSet(entity_name, features)


class TestDatasetInfo(object):
    def test_creation(self):
        name = "dataset_name"
        table_id = "gcp-project.dataset.table_name"
        dataset = DatasetInfo(name, table_id)
        assert dataset.name == name
        assert dataset.table_id == table_id
