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

import pandas as pd
import pytest
from pytest_lazyfixture import lazy_fixture

from feast import FileSource
from feast.entity import Entity
from feast.errors import ConflictingFeatureViewNames
from feast.feature_store import FeatureStore, _validate_feature_views
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.on_demand_feature_view import on_demand_feature_view
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import Float32, Float64, Int64, String
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


def test_validate_feature_views_cross_type_conflict():
    """
    Test that _validate_feature_views() catches cross-type name conflicts.

    This is a unit test for the validation that happens during feast plan/apply.
    The validation must catch conflicts across FeatureView, StreamFeatureView,
    and OnDemandFeatureView to prevent silent data correctness bugs in
    get_online_features (which uses fixed-order lookup).

    See: https://github.com/feast-dev/feast/issues/5995
    """
    # Create a simple entity
    entity = Entity(name="driver_entity", join_keys=["test_key"])

    # Create a regular FeatureView
    file_source = FileSource(name="my_file_source", path="test.parquet")
    feature_view = FeatureView(
        name="my_feature_view",
        entities=[entity],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
    )

    # Create a StreamFeatureView with the SAME name
    stream_feature_view = StreamFeatureView(
        name="my_feature_view",  # Same name as FeatureView!
        entities=[entity],
        ttl=timedelta(days=30),
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
    )

    # Validate should raise ConflictingFeatureViewNames
    with pytest.raises(ConflictingFeatureViewNames) as exc_info:
        _validate_feature_views([feature_view, stream_feature_view])

    # Verify error message contains type information
    error_message = str(exc_info.value)
    assert "my_feature_view" in error_message
    assert "FeatureView" in error_message
    assert "StreamFeatureView" in error_message


def test_validate_feature_views_same_type_conflict():
    """
    Test that _validate_feature_views() also catches same-type name conflicts
    with a proper error message indicating duplicate FeatureViews.
    """
    # Create a simple entity
    entity = Entity(name="driver_entity", join_keys=["test_key"])

    # Create two FeatureViews with the same name
    file_source = FileSource(name="my_file_source", path="test.parquet")
    fv1 = FeatureView(
        name="duplicate_fv",
        entities=[entity],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
    )
    fv2 = FeatureView(
        name="duplicate_fv",  # Same name!
        entities=[entity],
        schema=[Field(name="feature2", dtype=Float32)],
        source=file_source,
    )

    # Validate should raise ConflictingFeatureViewNames
    with pytest.raises(ConflictingFeatureViewNames) as exc_info:
        _validate_feature_views([fv1, fv2])

    # Verify error message indicates same-type duplicate
    error_message = str(exc_info.value)
    assert "duplicate_fv" in error_message
    assert "Multiple FeatureViews" in error_message
    assert "case-insensitively unique" in error_message


def test_validate_feature_views_case_insensitive():
    """
    Test that _validate_feature_views() catches case-insensitive conflicts.
    """
    entity = Entity(name="driver_entity", join_keys=["test_key"])
    file_source = FileSource(name="my_file_source", path="test.parquet")

    fv1 = FeatureView(
        name="MyFeatureView",
        entities=[entity],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
    )
    fv2 = FeatureView(
        name="myfeatureview",  # Same name, different case!
        entities=[entity],
        schema=[Field(name="feature2", dtype=Float32)],
        source=file_source,
    )

    # Validate should raise ConflictingFeatureViewNames (case-insensitive)
    with pytest.raises(ConflictingFeatureViewNames):
        _validate_feature_views([fv1, fv2])


def test_validate_feature_views_odfv_conflict():
    """
    Test that _validate_feature_views() catches OnDemandFeatureView name conflicts.
    """
    entity = Entity(name="driver_entity", join_keys=["test_key"])
    file_source = FileSource(name="my_file_source", path="test.parquet")

    fv = FeatureView(
        name="shared_name",
        entities=[entity],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
    )

    @on_demand_feature_view(
        sources=[fv],
        schema=[Field(name="output", dtype=Float32)],
    )
    def shared_name(inputs: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"output": inputs["feature1"] * 2})

    # Validate should raise ConflictingFeatureViewNames
    with pytest.raises(ConflictingFeatureViewNames) as exc_info:
        _validate_feature_views([fv, shared_name])

    error_message = str(exc_info.value)
    assert "shared_name" in error_message
    assert "FeatureView" in error_message
    assert "OnDemandFeatureView" in error_message
