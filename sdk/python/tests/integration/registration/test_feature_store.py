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
from datetime import timedelta
from tempfile import mkstemp

import pytest
from pytest_lazyfixture import lazy_fixture

from feast.entity import Entity
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Float64, Int64, String
from tests.utils.data_source_test_creator import prep_file_source


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
@pytest.mark.parametrize("dataframe_source", [lazy_fixture("simple_dataset_1")])
def test_feature_view_inference_success(test_feature_store, dataframe_source):
    with prep_file_source(df=dataframe_source, timestamp_field="ts_1") as file_source:
        entity = Entity(name="id", join_keys=["id_join_key"])

        fv1 = FeatureView(
            name="fv1",
            entities=[entity],
            ttl=timedelta(minutes=5),
            online=True,
            source=file_source,
            tags={},
        )

        test_feature_store.apply([entity, fv1])  # Register Feature Views
        feature_view_1 = test_feature_store.list_feature_views()[0]

        actual_file_source = {
            (feature.name, feature.dtype) for feature in feature_view_1.features
        }
        expected = {
            ("float_col", Float64),
            ("int64_col", Int64),
            ("string_col", String),
        }

        assert expected == actual_file_source

        test_feature_store.teardown()


@pytest.fixture
def feature_store_with_local_registry():
    fd, registry_path = mkstemp()
    fd, online_store_path = mkstemp()
    return FeatureStore(
        config=RepoConfig(
            registry=registry_path,
            project="default",
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=online_store_path),
            entity_key_serialization_version=3,
        )
    )
