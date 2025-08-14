from datetime import timedelta

import pytest

from feast import (
    Entity,
    FeatureStore,
    FeatureView,
    Field,
    FileSource,
    Project,
    SortedFeatureView,
    SparkSource,
    ValueType,
)
from feast.infra.registry.registry import Registry
from feast.repo_config import RegistryConfig
from feast.repo_contents import RepoContents
from feast.repo_operations import apply_total_with_repo_instance
from feast.sort_key import SortKey
from feast.types import Float64, Int64, String
from feast.utils import _utc_now, make_tzaware


@pytest.fixture
def driver_entity():
    return Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)


@pytest.fixture
def file_source():
    return FileSource(path="file:///data.parquet", event_timestamp_column="ts")


@pytest.fixture
def spark_source():
    return SparkSource(
        name="datasource1",
        description="datasource1",
        query="""select entity1, val1, val2, val3, val4, val5, CURRENT_DATE AS event_timestamp from table1 WHERE entity1 < 100000""",
        timestamp_field="event_timestamp",
        tags={"tag1": "val1", "tag2": "val2", "tag3": "val3"},
        owner="x@xyz.com",
    )


@pytest.fixture
def entity1():
    return Entity(
        name="entity1",
        join_keys=["entity1"],
        value_type=ValueType.INT64,
        description="entity1",
        owner="x@xyz.com",
        tags={"tag1": "val1", "tag2": "val2", "tag3": "val3"},
    )


@pytest.fixture
def dummy_repo_contents_fv(driver_entity, spark_source, file_source, entity1):
    fv = FeatureView(
        name="fv1",
        entities=[entity1],
        ttl=timedelta(days=365),
        schema=[
            Field(name="entity1", dtype=Int64),
            Field(name="val1", dtype=String),
            Field(name="val2", dtype=String),
        ],
        source=spark_source,
        description="view1",
        owner="x@xyz.com",
    )
    return RepoContents(
        projects=[Project(name="my_project")],
        data_sources=[file_source],
        entities=[driver_entity],
        feature_views=[fv],
        sorted_feature_views=[],
        on_demand_feature_views=[],
        stream_feature_views=[],
        feature_services=[],
        permissions=[],
    )


@pytest.fixture
def dummy_repo_contents_fv_updated(driver_entity, spark_source, file_source, entity1):
    fv = FeatureView(
        name="fv1",
        entities=[entity1],
        ttl=timedelta(days=365),
        schema=[
            Field(name="entity1", dtype=Int64),
            Field(name="val1", dtype=String),
            Field(name="val2", dtype=String),
            Field(name="val3", dtype=Int64),
        ],
        source=spark_source,
        description="view1",
        owner="x@xyz.com",
    )
    return RepoContents(
        projects=[Project(name="my_project")],
        data_sources=[file_source],
        entities=[driver_entity],
        feature_views=[fv],
        sorted_feature_views=[],
        on_demand_feature_views=[],
        stream_feature_views=[],
        feature_services=[],
        permissions=[],
    )


@pytest.fixture
def dummy_repo_contents_sfv(driver_entity, file_source, entity1):
    key = SortKey(name="f1", value_type=ValueType.INT64, default_sort_order=1)
    sfv = SortedFeatureView(
        name="sfv1",
        entities=[entity1],
        ttl=timedelta(days=365),
        schema=[Field(name="f1", dtype=Int64)],
        source=file_source,
        sort_keys=[key],
        _skip_validation=True,
    )
    return RepoContents(
        projects=[Project(name="my_project")],
        data_sources=[file_source],
        entities=[driver_entity],
        feature_views=[],
        sorted_feature_views=[sfv],
        on_demand_feature_views=[],
        stream_feature_views=[],
        feature_services=[],
        permissions=[],
    )


@pytest.fixture
def dummy_repo_contents_sfv_and_fv(driver_entity, file_source, entity1):
    key = SortKey(name="f1", value_type=ValueType.INT64, default_sort_order=0)
    sfv = SortedFeatureView(
        name="sfv1",
        entities=[entity1],
        ttl=timedelta(days=365),
        schema=[Field(name="f1", dtype=Int64)],
        source=file_source,
        sort_keys=[key],
        _skip_validation=True,
    )
    fv = FeatureView(
        name="fv1",
        entities=[entity1],
        ttl=timedelta(days=365),
        schema=[
            Field(name="entity1", dtype=Int64),
            Field(name="val1", dtype=String),
            Field(name="val2", dtype=String),
        ],
        source=spark_source,
        description="view1",
        owner="x@xyz.com",
    )
    return RepoContents(
        projects=[Project(name="my_project")],
        data_sources=[file_source],
        entities=[driver_entity],
        feature_views=[fv],
        sorted_feature_views=[sfv],
        on_demand_feature_views=[],
        stream_feature_views=[],
        feature_services=[],
        permissions=[],
    )


@pytest.fixture
def dummy_repo_contents_sfv_update(driver_entity, file_source, entity1):
    key = SortKey(name="f1", value_type=ValueType.INT64, default_sort_order=1)
    sfv = SortedFeatureView(
        name="sfv1",
        entities=[entity1],
        ttl=timedelta(days=365),
        schema=[Field(name="f1", dtype=Int64), Field(name="f2", dtype=Float64)],
        source=file_source,
        sort_keys=[key],
        _skip_validation=True,
    )
    return RepoContents(
        projects=[Project(name="my_project")],
        data_sources=[file_source],
        entities=[driver_entity],
        feature_views=[],
        sorted_feature_views=[sfv],
        on_demand_feature_views=[],
        stream_feature_views=[],
        feature_services=[],
        permissions=[],
    )


@pytest.fixture
def dummy_repo_contents_sfv_invalid_update(driver_entity, file_source, entity1):
    key = SortKey(name="f1", value_type=ValueType.INT64, default_sort_order=0)
    sfv = SortedFeatureView(
        name="sfv1",
        entities=[entity1],
        ttl=timedelta(days=365),
        schema=[Field(name="f2", dtype=Float64)],
        source=file_source,
        sort_keys=[key],
        _skip_validation=True,
    )
    return RepoContents(
        projects=[Project(name="my_project")],
        data_sources=[file_source],
        entities=[driver_entity],
        feature_views=[],
        sorted_feature_views=[sfv],
        on_demand_feature_views=[],
        stream_feature_views=[],
        feature_services=[],
        permissions=[],
    )


@pytest.fixture
def dummy_repo_contents_fv_invalid_update(
    driver_entity, spark_source, file_source, entity1
):
    fv = FeatureView(
        name="fv1",
        entities=[entity1],
        ttl=timedelta(days=365),
        schema=[
            Field(name="val1", dtype=String),
            Field(name="val2", dtype=String),
            Field(name="val3", dtype=Int64),
        ],
        source=spark_source,
        description="view1",
        owner="x@xyz.com",
    )
    return RepoContents(
        projects=[Project(name="my_project")],
        data_sources=[file_source],
        entities=[driver_entity],
        feature_views=[fv],
        sorted_feature_views=[],
        on_demand_feature_views=[],
        stream_feature_views=[],
        feature_services=[],
        permissions=[],
    )


@pytest.fixture
def registry(tmp_path):
    registry_cfg = RegistryConfig(path=str(tmp_path / "reg.db"), cache_ttl_seconds=0)
    return Registry(
        project="my_project",
        registry_config=registry_cfg,
        repo_path=None,
    )


@pytest.fixture
def store_mock(mocker):
    store = mocker.create_autospec(FeatureStore, instance=True)
    store._should_use_plan.return_value = False
    return store


def test_apply_only_calls_store_apply_for_new_fv(
    dummy_repo_contents_fv, registry, store_mock
):
    apply_total_with_repo_instance(
        store=store_mock,
        project_name="my_project",
        registry=registry,
        repo=dummy_repo_contents_fv,
        skip_source_validation=True,
    )
    # Verify that the apply method was called with the correct parameters
    expected_apply_list = (
        dummy_repo_contents_fv.projects
        + dummy_repo_contents_fv.data_sources
        + dummy_repo_contents_fv.entities
        + dummy_repo_contents_fv.feature_views
    )

    store_mock.apply.assert_called_once_with(
        expected_apply_list,
        objects_to_delete=[],
        partial=False,
    )


def test_apply_persists_feature_view(dummy_repo_contents_fv, registry, store_mock):
    apply_total_with_repo_instance(
        store=store_mock,
        project_name="my_project",
        registry=registry,
        repo=dummy_repo_contents_fv,
        skip_source_validation=True,
    )
    (ds,) = dummy_repo_contents_fv.data_sources
    (fv,) = dummy_repo_contents_fv.feature_views
    expected = (
        dummy_repo_contents_fv.projects + [ds] + dummy_repo_contents_fv.entities + [fv]
    )
    store_mock.apply.assert_called_once_with(
        expected,
        objects_to_delete=[],
        partial=False,
    )


def test_update_feature_view_add_field(
    dummy_repo_contents_fv, dummy_repo_contents_fv_updated, registry, store_mock
):
    # First apply original FV
    apply_total_with_repo_instance(
        store=store_mock,
        project_name="my_project",
        registry=registry,
        repo=dummy_repo_contents_fv,
        skip_source_validation=True,
    )

    # Manually persist the FV into the registry so get_feature_view() will succeed:
    original_fv = dummy_repo_contents_fv.feature_views[0]
    registry.apply_feature_view(original_fv, project="my_project", commit=True)

    store_mock.apply.reset_mock()

    # Re-apply
    apply_total_with_repo_instance(
        store=store_mock,
        project_name="my_project",
        registry=registry,
        repo=dummy_repo_contents_fv_updated,
        skip_source_validation=True,
    )

    (ds,) = dummy_repo_contents_fv_updated.data_sources
    (fv1,) = dummy_repo_contents_fv_updated.feature_views
    expected = (
        dummy_repo_contents_fv_updated.projects
        + [ds]
        + dummy_repo_contents_fv_updated.entities
        + [fv1]
    )
    store_mock.apply.assert_called_once_with(
        expected,
        objects_to_delete=[],
        partial=False,
    )


def test_update_feature_view_remove_entity_key_fails(
    dummy_repo_contents_fv,
    dummy_repo_contents_fv_invalid_update,
    file_source,
    registry,
    store_mock,
):
    apply_total_with_repo_instance(
        store=store_mock,
        project_name="my_project",
        registry=registry,
        repo=dummy_repo_contents_fv,
        skip_source_validation=True,
    )

    original_fv = dummy_repo_contents_fv.feature_views[0]
    current_time = _utc_now()
    start_date = make_tzaware(current_time - timedelta(days=1))
    end_date = make_tzaware(current_time)
    original_fv.materialization_intervals.append((start_date, end_date))

    registry.apply_feature_view(original_fv, project="my_project", commit=True)

    with pytest.raises(ValueError) as excinfo:
        apply_total_with_repo_instance(
            store=store_mock,
            project_name="my_project",
            registry=registry,
            repo=dummy_repo_contents_fv_invalid_update,
            skip_source_validation=True,
        )

    assert "entity key and cannot be removed" in str(excinfo.value)


def test_update_sorted_feature_view(
    dummy_repo_contents_sfv, dummy_repo_contents_sfv_update, registry, store_mock
):
    apply_total_with_repo_instance(
        store=store_mock,
        project_name="my_project",
        registry=registry,
        repo=dummy_repo_contents_sfv,
        skip_source_validation=True,
    )

    # Manually persist the FV into the registry so get_feature_view() will succeed:
    original_fv = dummy_repo_contents_sfv.sorted_feature_views[0]
    registry.apply_feature_view(original_fv, project="my_project", commit=True)

    store_mock.apply.reset_mock()

    apply_total_with_repo_instance(
        store=store_mock,
        project_name="my_project",
        registry=registry,
        repo=dummy_repo_contents_sfv_update,
        skip_source_validation=True,
    )

    (ds,) = dummy_repo_contents_sfv_update.data_sources
    (fv1,) = dummy_repo_contents_sfv_update.sorted_feature_views
    expected = (
        dummy_repo_contents_sfv_update.projects
        + [ds]
        + dummy_repo_contents_sfv_update.entities
        + [fv1]
    )
    store_mock.apply.assert_called_once_with(
        expected,
        objects_to_delete=[],
        partial=False,
    )
