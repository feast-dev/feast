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


from feast.feature_set import FeatureSet, Feature
from feast.value_type import ValueType
import pandas as pd
import pytest

CORE_URL = "core.feast.ai"
SERVING_URL = "serving.feast.ai"


class TestFeatureSet:
    def test_add_remove_features_success(self):
        fs = FeatureSet("my-feature-set")
        fs.add(Feature(name="my-feature-1", dtype=ValueType.INT64))
        fs.add(Feature(name="my-feature-2", dtype=ValueType.INT64))
        fs.drop(name="my-feature-1")
        assert len(fs.features) == 1 and fs.features[0].name == "my-feature-2"

    def test_remove_feature_failure(self):
        with pytest.raises(ValueError):
            fs = FeatureSet("my-feature-set")
            fs.drop(name="my-feature-1")

    def test_update_from_source_success(self):
        df = pd.read_csv("tests/data/driver_features.csv", index_col=None)
        df["datetime"] = pd.to_datetime(df["datetime"], unit="s")
        fs = FeatureSet("driver-feature-set")
        fs.update_from_source(df)
        assert len(fs.features) == 5 and fs.features[1].name == "completed"

    def test_update_from_source_failure(self):
        with pytest.raises(Exception):
            df = pd.DataFrame()
            fs = FeatureSet("driver-feature-set")
            fs.update_from_source(df)
