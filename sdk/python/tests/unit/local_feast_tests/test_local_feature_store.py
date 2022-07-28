from datetime import datetime, timedelta
from tempfile import mkstemp

import pytest
from pytest_lazyfixture import lazy_fixture

from feast import FileSource
from feast.data_format import ParquetFormat
from feast.entity import Entity
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Array, Bytes, Int64, String
<<<<<<< HEAD
from tests.utils.data_source_test_creator import prep_file_source
=======
from tests.utils.data_source_utils import prep_file_source
>>>>>>> 2b22e7ea9 (address review)


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_entity_success(test_feature_store):
    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        tags={"team": "matchmaking"},
    )

    # Register Entity
    test_feature_store.apply(entity)

    entities = test_feature_store.list_entities()

    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )

    test_feature_store.teardown()


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_feature_view_success(test_feature_store):
    # Create Feature Views
    batch_source = FileSource(
        file_format=ParquetFormat(),
        path="file://feast/*",
        timestamp_field="ts_col",
        created_timestamp_column="timestamp",
        date_partition_column="date_partition_col",
    )

    entity = Entity(name="fs1_my_entity_1", join_keys=["entity_id"])

    fv1 = FeatureView(
        name="my_feature_view_1",
        schema=[
            Field(name="fs1_my_feature_1", dtype=Int64),
            Field(name="fs1_my_feature_2", dtype=String),
            Field(name="fs1_my_feature_3", dtype=Array(String)),
            Field(name="fs1_my_feature_4", dtype=Array(Bytes)),
            Field(name="entity_id", dtype=Int64),
        ],
        entities=[entity],
        tags={"team": "matchmaking"},
        batch_source=batch_source,
        ttl=timedelta(minutes=5),
    )

    # Register Feature View
    test_feature_store.apply([entity, fv1])

    feature_views = test_feature_store.list_feature_views()

    # List Feature Views
    assert (
        len(feature_views) == 1
        and feature_views[0].name == "my_feature_view_1"
        and feature_views[0].features[0].name == "fs1_my_feature_1"
        and feature_views[0].features[0].dtype == Int64
        and feature_views[0].features[1].name == "fs1_my_feature_2"
        and feature_views[0].features[1].dtype == String
        and feature_views[0].features[2].name == "fs1_my_feature_3"
        and feature_views[0].features[2].dtype == Array(String)
        and feature_views[0].features[3].name == "fs1_my_feature_4"
        and feature_views[0].features[3].dtype == Array(Bytes)
        and feature_views[0].entities[0] == "fs1_my_entity_1"
    )

    test_feature_store.teardown()


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_object_and_read(test_feature_store):
    assert isinstance(test_feature_store, FeatureStore)
    # Create Feature Views
    batch_source = FileSource(
        file_format=ParquetFormat(),
        path="file://feast/*",
        timestamp_field="ts_col",
        created_timestamp_column="timestamp",
    )

    e1 = Entity(name="fs1_my_entity_1", description="something")

    e2 = Entity(name="fs1_my_entity_2", description="something")

    fv1 = FeatureView(
        name="my_feature_view_1",
        schema=[
            Field(name="fs1_my_feature_1", dtype=Int64),
            Field(name="fs1_my_feature_2", dtype=String),
            Field(name="fs1_my_feature_3", dtype=Array(String)),
            Field(name="fs1_my_feature_4", dtype=Array(Bytes)),
            Field(name="fs1_my_entity_1", dtype=Int64),
        ],
        entities=[e1],
        tags={"team": "matchmaking"},
        batch_source=batch_source,
        ttl=timedelta(minutes=5),
    )

    fv2 = FeatureView(
        name="my_feature_view_2",
        schema=[
            Field(name="fs1_my_feature_1", dtype=Int64),
            Field(name="fs1_my_feature_2", dtype=String),
            Field(name="fs1_my_feature_3", dtype=Array(String)),
            Field(name="fs1_my_feature_4", dtype=Array(Bytes)),
            Field(name="fs1_my_entity_2", dtype=Int64),
        ],
        entities=[e2],
        tags={"team": "matchmaking"},
        batch_source=batch_source,
        ttl=timedelta(minutes=5),
    )

    # Register Feature View
    test_feature_store.apply([fv1, e1, fv2, e2])

    fv1_actual = test_feature_store.get_feature_view("my_feature_view_1")
    e1_actual = test_feature_store.get_entity("fs1_my_entity_1")

    assert e1 == e1_actual
    assert fv2 != fv1_actual
    assert e2 != e1_actual

    test_feature_store.teardown()


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
@pytest.mark.parametrize("dataframe_source", [lazy_fixture("simple_dataset_1")])
def test_reapply_feature_view_success(test_feature_store, dataframe_source):
    with prep_file_source(df=dataframe_source, timestamp_field="ts_1") as file_source:

        e = Entity(name="id", join_keys=["id_join_key"])

        # Create Feature View
        fv1 = FeatureView(
            name="my_feature_view_1",
            schema=[Field(name="string_col", dtype=String)],
            entities=[e],
            batch_source=file_source,
            ttl=timedelta(minutes=5),
        )

        # Register Feature View
        test_feature_store.apply([fv1, e])

        # Check Feature View
        fv_stored = test_feature_store.get_feature_view(fv1.name)
        assert len(fv_stored.materialization_intervals) == 0

        # Run materialization
        test_feature_store.materialize(datetime(2020, 1, 1), datetime(2021, 1, 1))

        # Check Feature View
        fv_stored = test_feature_store.get_feature_view(fv1.name)
        assert len(fv_stored.materialization_intervals) == 1

        # Apply again
        test_feature_store.apply([fv1])

        # Check Feature View
        fv_stored = test_feature_store.get_feature_view(fv1.name)
        assert len(fv_stored.materialization_intervals) == 1

        # Change and apply Feature View
        fv1 = FeatureView(
            name="my_feature_view_1",
            schema=[Field(name="int64_col", dtype=Int64)],
            entities=[e],
            batch_source=file_source,
            ttl=timedelta(minutes=5),
        )
        test_feature_store.apply([fv1])

        # Check Feature View
        fv_stored = test_feature_store.get_feature_view(fv1.name)
        assert len(fv_stored.materialization_intervals) == 0

        test_feature_store.teardown()


def test_apply_conflicting_featureview_names(feature_store_with_local_registry):
    """Test applying feature views with non-case-insensitively unique names"""
    driver = Entity(name="driver", join_keys=["driver_id"])
    customer = Entity(name="customer", join_keys=["customer_id"])

    driver_stats = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        ttl=timedelta(seconds=10),
        online=False,
        batch_source=FileSource(path="driver_stats.parquet"),
        tags={},
    )

    customer_stats = FeatureView(
        name="DRIVER_HOURLY_STATS",
        entities=[customer],
        ttl=timedelta(seconds=10),
        online=False,
        batch_source=FileSource(path="customer_stats.parquet"),
        tags={},
    )
    try:
        feature_store_with_local_registry.apply([driver_stats, customer_stats])
        error = None
    except ValueError as e:
        error = e
    assert (
        isinstance(error, ValueError)
        and "Please ensure that all feature view names are case-insensitively unique"
        in error.args[0]
    )

    feature_store_with_local_registry.teardown()


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
        )
    )
