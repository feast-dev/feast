from datetime import datetime, timedelta
from tempfile import mkstemp
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest
from pytest_lazyfixture import lazy_fixture

from feast import BatchFeatureView, utils
from feast.aggregation import Aggregation
from feast.data_format import AvroFormat, ParquetFormat
from feast.data_source import KafkaSource
from feast.entity import Entity
from feast.feast_object import ALL_RESOURCE_TYPES
from feast.feature_store import FeatureStore
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_NAME, FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.dynamodb import DynamoDBOnlineStore
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.permissions.action import AuthzedAction
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.repo_config import RegistryConfig, RepoConfig
from feast.stream_feature_view import stream_feature_view
from feast.types import Array, Bytes, Float32, Int64, String, ValueType, from_value_type
from tests.integration.feature_repos.universal.feature_views import TAGS
from tests.utils.cli_repo_creator import CliRunner, get_example_repo
from tests.utils.data_source_test_creator import prep_file_source


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_entity(test_feature_store):
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
def test_apply_feature_view(test_feature_store):
    # Create Feature Views
    batch_source = FileSource(
        file_format=ParquetFormat(),
        path="file://feast/*",
        timestamp_field="ts_col",
        created_timestamp_column="timestamp",
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
        source=batch_source,
        ttl=timedelta(minutes=5),
    )

    bfv = BatchFeatureView(
        name="batch_feature_view",
        schema=[
            Field(name="fs1_my_feature_1", dtype=Int64),
            Field(name="fs1_my_feature_2", dtype=String),
            Field(name="fs1_my_feature_3", dtype=Array(String)),
            Field(name="fs1_my_feature_4", dtype=Array(Bytes)),
            Field(name="entity_id", dtype=Int64),
        ],
        udf=lambda df: df,
        entities=[entity],
        tags={"team": "matchmaking", "tag": "two"},
        source=batch_source,
        ttl=timedelta(minutes=5),
    )

    # Register Feature View
    test_feature_store.apply([entity, fv1, bfv])

    # List Feature Views
    assert len(test_feature_store.list_batch_feature_views({})) == 2
    feature_views = test_feature_store.list_batch_feature_views()
    assert (
        len(feature_views) == 2
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

    assert utils.tags_str_to_dict() == {}
    assert utils.tags_list_to_dict() is None
    assert utils.tags_list_to_dict([]) is None
    assert utils.tags_list_to_dict([""]) == {}
    assert utils.tags_list_to_dict(
        (
            "team : driver_performance, other:tag",
            "blanktag:",
            "other:two",
            "other:3",
            "missing",
        )
    ) == {"team": "driver_performance", "other": "3", "blanktag": ""}
    assert utils.has_all_tags({})

    tags_dict = {"team": "matchmaking"}
    tags_filter = utils.tags_str_to_dict("('team:matchmaking',)")
    assert tags_filter == tags_dict
    tags_filter = utils.tags_list_to_dict(("team:matchmaking", "test"))
    assert tags_filter == tags_dict

    # List Feature Views
    feature_views = test_feature_store.list_batch_feature_views(tags=tags_filter)
    assert (
        len(feature_views) == 2
        and utils.has_all_tags(feature_views[0].tags, tags_filter)
        and utils.has_all_tags(feature_views[1].tags, tags_filter)
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

    tags_dict = {"team": "matchmaking", "tag": "two"}
    tags_filter = utils.tags_list_to_dict((" team :matchmaking, tag: two ",))
    assert tags_filter == tags_dict

    # List Feature Views
    feature_views = test_feature_store.list_batch_feature_views(tags=tags_filter)
    assert (
        len(feature_views) == 1
        and utils.has_all_tags(feature_views[0].tags, tags_filter)
        and feature_views[0].name == "batch_feature_view"
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

    tags_dict = {"missing": "tag"}
    tags_filter = utils.tags_list_to_dict(("missing:tag,fdsa", "fdas"))
    assert tags_filter == tags_dict

    # List Feature Views
    assert len(test_feature_store.list_batch_feature_views(tags=tags_filter)) == 0

    test_feature_store.teardown()


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_feature_view_with_inline_batch_source(
    test_feature_store, simple_dataset_1
) -> None:
    """Test that a feature view and an inline batch source are both correctly applied."""
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity = Entity(name="driver_entity", join_keys=["test_key"])
        driver_fv = FeatureView(
            name="driver_fv",
            entities=[entity],
            schema=[Field(name="test_key", dtype=Int64)],
            source=file_source,
        )

        test_feature_store.apply([entity, driver_fv])

        fvs = test_feature_store.list_batch_feature_views()
        dfv = fvs[0]
        assert len(fvs) == 1
        assert dfv == driver_fv

        ds = test_feature_store.list_data_sources()
        assert len(ds) == 1
        assert ds[0] == file_source


def test_apply_feature_view_with_inline_batch_source_from_repo() -> None:
    """Test that a feature view and an inline batch source are both correctly applied."""
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_with_inline_batch_source.py"), "file"
    ) as store:
        ds = store.list_data_sources()
        assert len(ds) == 1


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_feature_view_with_inline_stream_source(
    test_feature_store, simple_dataset_1
) -> None:
    """Test that a feature view and an inline stream source are both correctly applied."""
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity = Entity(name="driver_entity", join_keys=["test_key"])

        stream_source = KafkaSource(
            name="kafka",
            timestamp_field="event_timestamp",
            kafka_bootstrap_servers="",
            message_format=AvroFormat(""),
            topic="topic",
            batch_source=file_source,
            watermark_delay_threshold=timedelta(days=1),
        )

        driver_fv = FeatureView(
            name="driver_fv",
            entities=[entity],
            schema=[Field(name="test_key", dtype=Int64)],
            source=stream_source,
        )

        test_feature_store.apply([entity, driver_fv])

        fvs = test_feature_store.list_batch_feature_views()
        assert len(fvs) == 1
        assert fvs[0] == driver_fv

        ds = test_feature_store.list_data_sources()
        assert len(ds) == 2
        if isinstance(ds[0], FileSource):
            assert ds[0] == file_source
            assert ds[1] == stream_source
        else:
            assert ds[0] == stream_source
            assert ds[1] == file_source


def test_apply_feature_view_with_inline_stream_source_from_repo() -> None:
    """Test that a feature view and an inline stream source are both correctly applied."""
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_with_inline_stream_source.py"), "file"
    ) as store:
        ds = store.list_data_sources()
        assert len(ds) == 2


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_entities_and_feature_views(test_feature_store):
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
        source=batch_source,
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
        source=batch_source,
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
def test_apply_dummy_entity_and_feature_view_columns(test_feature_store):
    assert isinstance(test_feature_store, FeatureStore)
    # Create Feature Views
    batch_source = FileSource(
        file_format=ParquetFormat(),
        path="file://feast/*",
        timestamp_field="ts_col",
        created_timestamp_column="timestamp",
    )

    e1 = Entity(
        name="fs1_my_entity_1", description="something", value_type=ValueType.INT64
    )

    fv_no_entity = FeatureView(
        name="my_feature_view_no_entity",
        schema=[
            Field(name="fs1_my_feature_1", dtype=Int64),
            Field(name="fs1_my_entity_1", dtype=Int64),
        ],
        entities=[],
        tags={"team": "matchmaking"},
        source=batch_source,
        ttl=timedelta(minutes=5),
    )
    fv_with_entity = FeatureView(
        name="my_feature_view_with_entity",
        schema=[
            Field(name="fs1_my_feature_1", dtype=Int64),
            Field(name="fs1_my_entity_1", dtype=Int64),
        ],
        entities=[e1],
        tags={"team": "matchmaking"},
        source=batch_source,
        ttl=timedelta(minutes=5),
    )

    # Check that the entity_columns are empty before applying
    assert fv_no_entity.entities == [DUMMY_ENTITY_NAME]
    assert fv_no_entity.entity_columns == []
    # Note that this test is a special case rooted in the entity being included in the schema
    assert fv_with_entity.entity_columns == [
        Field(name=e1.join_key, dtype=from_value_type(e1.value_type))
    ]
    # Register Feature View
    test_feature_store.apply([e1, fv_no_entity, fv_with_entity])
    fv_from_online_store = test_feature_store.get_feature_view(
        "my_feature_view_no_entity"
    )
    # Note that after the apply() the feature_view serializes the Dummy Entity ID
    assert fv_no_entity.entity_columns[0].name == DUMMY_ENTITY_ID
    assert fv_from_online_store.entity_columns[0].name == DUMMY_ENTITY_ID
    assert fv_from_online_store.entities == []
    assert fv_no_entity.entities == [DUMMY_ENTITY_NAME]
    assert fv_with_entity.entity_columns[0].name == e1.join_key

    test_feature_store.teardown()


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_permissions(test_feature_store):
    assert isinstance(test_feature_store, FeatureStore)

    permission = Permission(
        name="reader",
        types=ALL_RESOURCE_TYPES,
        policy=RoleBasedPolicy(roles=["reader"]),
        actions=[AuthzedAction.DESCRIBE],
    )

    # Register Permission
    test_feature_store.apply([permission])

    permissions = test_feature_store.list_permissions()
    assert len(permissions) == 1
    assert permissions[0] == permission

    # delete Permission
    test_feature_store.apply(objects=[], objects_to_delete=[permission], partial=False)

    permissions = test_feature_store.list_permissions()
    assert len(permissions) == 0

    test_feature_store.teardown()


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
@pytest.mark.parametrize("dataframe_source", [lazy_fixture("simple_dataset_1")])
def test_reapply_feature_view(test_feature_store, dataframe_source):
    with prep_file_source(df=dataframe_source, timestamp_field="ts_1") as file_source:
        e = Entity(name="id", join_keys=["id_join_key"])

        # Create Feature View
        fv1 = FeatureView(
            name="my_feature_view_1",
            schema=[Field(name="string_col", dtype=String)],
            entities=[e],
            source=file_source,
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
            source=file_source,
            ttl=timedelta(minutes=5),
        )
        test_feature_store.apply([fv1])

        # Check Feature View
        fv_stored = test_feature_store.get_feature_view(fv1.name)
        assert len(fv_stored.materialization_intervals) == 1

        # Change and apply Feature View, this time, only the name
        fv2 = FeatureView(
            name="my_feature_view_2",
            schema=[Field(name="int64_col", dtype=Int64)],
            entities=[e],
            source=file_source,
            ttl=timedelta(minutes=5),
        )
        test_feature_store.apply([fv2])

        # Check Feature View
        fv_stored = test_feature_store.get_feature_view(fv2.name)
        assert len(fv_stored.materialization_intervals) == 0

        test_feature_store.teardown()


def test_apply_conflicting_feature_view_names(feature_store_with_local_registry):
    """Test applying feature views with non-case-insensitively unique names"""
    driver = Entity(name="driver", join_keys=["driver_id"])
    customer = Entity(name="customer", join_keys=["customer_id"])

    driver_stats = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        schema=[Field(name="driver_id", dtype=Int64)],
        ttl=timedelta(seconds=10),
        online=False,
        source=FileSource(path="driver_stats.parquet"),
        tags={},
    )

    customer_stats = FeatureView(
        name="DRIVER_HOURLY_STATS",
        entities=[customer],
        schema=[Field(name="customer_id", dtype=Int64)],
        ttl=timedelta(seconds=10),
        online=False,
        source=FileSource(path="customer_stats.parquet"),
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


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_stream_feature_view(test_feature_store, simple_dataset_1) -> None:
    """Test that a stream feature view is correctly applied."""
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity = Entity(name="driver_entity", join_keys=["test_key"])

        stream_source = KafkaSource(
            name="kafka",
            timestamp_field="event_timestamp",
            kafka_bootstrap_servers="",
            message_format=AvroFormat(""),
            topic="topic",
            batch_source=file_source,
            watermark_delay_threshold=timedelta(days=1),
        )

        @stream_feature_view(
            entities=[entity],
            ttl=timedelta(days=30),
            owner="test@example.com",
            online=True,
            schema=[Field(name="dummy_field", dtype=Float32)],
            description="desc",
            aggregations=[
                Aggregation(
                    column="dummy_field",
                    function="max",
                    time_window=timedelta(days=1),
                ),
                Aggregation(
                    column="dummy_field2",
                    function="count",
                    time_window=timedelta(days=24),
                ),
            ],
            timestamp_field="event_timestamp",
            mode="spark",
            source=stream_source,
            tags={},
        )
        def simple_sfv(df):
            return df

        test_feature_store.apply([entity, simple_sfv])

        stream_feature_views = test_feature_store.list_stream_feature_views()
        assert len(stream_feature_views) == 1
        assert stream_feature_views[0] == simple_sfv

        features = test_feature_store.get_online_features(
            features=["simple_sfv:dummy_field"],
            entity_rows=[{"test_key": 1001}],
        ).to_dict(include_event_timestamps=True)

        assert "test_key" in features
        assert features["test_key"] == [1001]
        assert "dummy_field" in features
        assert features["dummy_field"] == [None]


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_stream_feature_view_udf(test_feature_store, simple_dataset_1) -> None:
    """Test that a stream feature view with a udf is correctly applied."""
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity = Entity(name="driver_entity", join_keys=["test_key"])

        stream_source = KafkaSource(
            name="kafka",
            timestamp_field="event_timestamp",
            kafka_bootstrap_servers="",
            message_format=AvroFormat(""),
            topic="topic",
            batch_source=file_source,
            watermark_delay_threshold=timedelta(days=1),
        )

        @stream_feature_view(
            entities=[entity],
            ttl=timedelta(days=30),
            owner="test@example.com",
            online=True,
            schema=[Field(name="dummy_field", dtype=Float32)],
            description="desc",
            aggregations=[
                Aggregation(
                    column="dummy_field",
                    function="max",
                    time_window=timedelta(days=1),
                ),
                Aggregation(
                    column="dummy_field2",
                    function="count",
                    time_window=timedelta(days=24),
                ),
            ],
            timestamp_field="event_timestamp",
            mode="spark",
            source=stream_source,
            tags={},
        )
        def pandas_view(pandas_df):
            import pandas as pd

            assert type(pandas_df) == pd.DataFrame
            df = pandas_df.transform(lambda x: x + 10)
            df.insert(2, "C", [20.2, 230.0, 34.0], True)
            return df

        import pandas as pd

        test_feature_store.apply([entity, pandas_view])

        stream_feature_views = test_feature_store.list_stream_feature_views()
        assert len(stream_feature_views) == 1
        assert stream_feature_views[0] == pandas_view

        sfv = stream_feature_views[0]

        df = pd.DataFrame({"A": [1, 2, 3], "B": [10, 20, 30]})
        new_df = sfv.udf(df)
        expected_df = pd.DataFrame(
            {"A": [11, 12, 13], "B": [20, 30, 40], "C": [20.2, 230.0, 34.0]}
        )
        assert new_df.equals(expected_df)


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_batch_source(test_feature_store, simple_dataset_1) -> None:
    """Test that a batch source is applied correctly."""
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        # Store original timestamps before applying
        original_created = file_source.created_timestamp
        original_updated = file_source.last_updated_timestamp

        test_feature_store.apply([file_source])

        ds = test_feature_store.list_data_sources()
        assert len(ds) == 1
        assert ds[0] == file_source

        # Verify timestamps are handled correctly during apply/registry operations
        applied_source = ds[0]
        assert applied_source.created_timestamp == original_created
        assert applied_source.last_updated_timestamp >= original_updated

        # Verify timestamps are included in protobuf representation
        proto = applied_source.to_proto()
        assert proto.HasField("meta")
        assert proto.meta.HasField("created_timestamp")
        assert proto.meta.HasField("last_updated_timestamp")

        # Verify roundtrip serialization preserves timestamps
        from feast.data_source import DataSource

        roundtrip_source = DataSource.from_proto(proto)
        assert roundtrip_source.created_timestamp == original_created
        assert (
            roundtrip_source.last_updated_timestamp
            == applied_source.last_updated_timestamp
        )


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_stream_source(test_feature_store, simple_dataset_1) -> None:
    """Test that a stream source is applied correctly."""
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        stream_source = KafkaSource(
            name="kafka",
            timestamp_field="event_timestamp",
            kafka_bootstrap_servers="",
            message_format=AvroFormat(""),
            topic="topic",
            batch_source=file_source,
            watermark_delay_threshold=timedelta(days=1),
            tags=TAGS,
        )

        # Store original timestamps before applying
        original_stream_created = stream_source.created_timestamp
        original_stream_updated = stream_source.last_updated_timestamp
        original_file_created = file_source.created_timestamp
        original_file_updated = file_source.last_updated_timestamp

        test_feature_store.apply([stream_source])

        assert len(test_feature_store.list_data_sources(tags=TAGS)) == 1
        ds = test_feature_store.list_data_sources()
        assert len(ds) == 2

        # Find the applied sources
        applied_stream_source = None
        applied_file_source = None
        for source in ds:
            if isinstance(source, FileSource):
                applied_file_source = source
            else:
                applied_stream_source = source

        assert applied_file_source == file_source
        assert applied_stream_source == stream_source

        assert applied_stream_source.created_timestamp == original_stream_created
        assert applied_stream_source.last_updated_timestamp >= original_stream_updated
        assert applied_file_source.created_timestamp == original_file_created
        assert applied_file_source.last_updated_timestamp >= original_file_updated

        stream_proto = applied_stream_source.to_proto()
        assert stream_proto.HasField("meta")
        assert stream_proto.meta.HasField("created_timestamp")
        assert stream_proto.meta.HasField("last_updated_timestamp")

        file_proto = applied_file_source.to_proto()
        assert file_proto.HasField("meta")
        assert file_proto.meta.HasField("created_timestamp")
        assert file_proto.meta.HasField("last_updated_timestamp")


def test_apply_stream_source_from_repo() -> None:
    """Test that a stream source is applied correctly."""
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_with_stream_source.py"), "file"
    ) as store:
        ds = store.list_data_sources()
        assert len(ds) == 2


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


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_refreshes_registry_cache_sync_mode(test_feature_store):
    """Test that apply() refreshes registry cache when cache_mode is 'sync' (default)"""
    # Create a simple entity (no FeatureView to avoid file path issues)
    entity = Entity(name="test_entity", join_keys=["id"])

    # Mock the refresh_registry method to verify it's called
    with patch.object(test_feature_store, "refresh_registry") as mock_refresh:
        # Apply the entity
        test_feature_store.apply([entity])

        # Verify refresh_registry was called once (due to sync mode)
        mock_refresh.assert_called_once()

    test_feature_store.teardown()


@pytest.mark.parametrize(
    "test_feature_store",
    [lazy_fixture("feature_store_with_local_registry")],
)
def test_apply_skips_refresh_registry_cache_thread_mode(test_feature_store):
    """Test that apply() skips registry refresh when cache_mode is 'thread'"""
    # Create a simple entity
    entity = Entity(name="test_entity", join_keys=["id"])

    # Temporarily change cache_mode to 'thread'
    original_cache_mode = test_feature_store.config.registry.cache_mode
    test_feature_store.config.registry.cache_mode = "thread"

    try:
        # Mock the refresh_registry method to verify it's NOT called
        with patch.object(test_feature_store, "refresh_registry") as mock_refresh:
            # Apply the entity
            test_feature_store.apply([entity])

            # Verify refresh_registry was NOT called (due to thread mode)
            mock_refresh.assert_not_called()
    finally:
        # Restore original cache_mode
        test_feature_store.config.registry.cache_mode = original_cache_mode

    test_feature_store.teardown()


def test_registry_config_cache_mode_default():
    """Test that RegistryConfig has cache_mode with default value 'sync'"""
    config = RegistryConfig()
    assert hasattr(config, "cache_mode")
    assert config.cache_mode == "sync"


def test_registry_config_cache_mode_can_be_set():
    """Test that RegistryConfig cache_mode can be set to different values"""
    config = RegistryConfig(cache_mode="thread")
    assert config.cache_mode == "thread"

    config = RegistryConfig(cache_mode="sync")
    assert config.cache_mode == "sync"


# Tests for update_online_store functionality


def test_update_online_store_not_supported():
    """Test that update_online_store raises NotImplementedError for non-DynamoDB stores."""

    # Create a FeatureStore with SQLite online store (doesn't support update expressions)
    fd, registry_path = mkstemp()
    fd, online_store_path = mkstemp()
    store = FeatureStore(
        config=RepoConfig(
            registry=registry_path,
            project="default",
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=online_store_path),
            entity_key_serialization_version=3,
        )
    )

    df = pd.DataFrame({"entity_id": ["1", "2"], "transactions": [["tx1"], ["tx2"]]})

    update_expressions = {"transactions": "list_append(transactions, :new_val)"}

    with pytest.raises(NotImplementedError) as exc_info:
        store.update_online_store(
            feature_view_name="test_fv", df=df, update_expressions=update_expressions
        )

    assert "does not support update expressions" in str(exc_info.value)
    assert "DynamoDB online store" in str(exc_info.value)


def test_update_online_store_empty_dataframe():
    """Test that update_online_store handles empty dataframe gracefully."""

    with (
        patch("feast.feature_store.load_repo_config") as mock_load_config,
        patch("feast.feature_store.Registry"),
        patch("feast.feature_store.get_provider") as mock_get_provider,
        patch(
            "feast.feature_store.FeatureStore._get_feature_view_and_df_for_online_write"
        ) as mock_get_fv_df,
    ):
        mock_provider = Mock()
        mock_provider.online_store = DynamoDBOnlineStore()
        mock_provider.online_store.update_online_store = Mock()
        mock_get_provider.return_value = mock_provider
        mock_load_config.return_value = Mock()

        empty_df = pd.DataFrame()
        mock_feature_view = Mock()
        mock_get_fv_df.return_value = (mock_feature_view, empty_df)

        store = FeatureStore()
        update_expressions = {"transactions": "list_append(transactions, :new_val)"}

        with patch("feast.feature_store.warnings.warn") as mock_warn:
            store.update_online_store(
                feature_view_name="test_fv",
                df=empty_df,
                update_expressions=update_expressions,
            )
            mock_warn.assert_called_once_with("Cannot update with empty dataframe")
            mock_provider.online_store.update_online_store.assert_not_called()


def test_update_online_store_success():
    """Test successful update_online_store call."""

    with (
        patch(
            "feast.feature_store.FeatureStore._get_feature_view_and_df_for_online_write"
        ) as mock_get_fv_df,
        patch(
            "feast.infra.passthrough_provider.PassthroughProvider._prep_rows_to_write_for_ingestion"
        ) as mock_prep,
        patch("feast.feature_store.load_repo_config") as mock_load_config,
        patch("feast.feature_store.Registry"),
        patch("feast.feature_store.get_provider") as mock_get_provider,
    ):
        mock_provider = Mock()
        mock_provider.online_store = DynamoDBOnlineStore()
        mock_provider.online_store.update_online_store = Mock()
        mock_get_provider.return_value = mock_provider
        mock_prep.return_value = []
        mock_load_config.return_value = Mock()

        df = pd.DataFrame({"entity_id": ["1", "2"], "transactions": [["tx1"], ["tx2"]]})
        mock_feature_view = Mock()
        mock_feature_view.features = [Field(name="transactions", dtype=Array(String))]
        mock_get_fv_df.return_value = (mock_feature_view, df)

        store = FeatureStore()
        update_expressions = {"transactions": "list_append(transactions, :new_val)"}

        store.update_online_store(
            feature_view_name="test_fv", df=df, update_expressions=update_expressions
        )

        mock_provider.online_store.update_online_store.assert_called_once()


@pytest.mark.asyncio
async def test_update_online_store_async_not_supported():
    """Test that async update raises NotImplementedError for non-DynamoDB stores."""

    fd, registry_path = mkstemp()
    fd, online_store_path = mkstemp()
    store = FeatureStore(
        config=RepoConfig(
            registry=registry_path,
            project="default",
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=online_store_path),
            entity_key_serialization_version=3,
        )
    )

    df = pd.DataFrame({"entity_id": ["1", "2"], "transactions": [["tx1"], ["tx2"]]})

    update_expressions = {"transactions": "list_append(transactions, :new_val)"}

    with pytest.raises(NotImplementedError) as exc_info:
        await store.update_online_store_async(
            feature_view_name="test_fv", df=df, update_expressions=update_expressions
        )

    assert "does not support async update expressions" in str(exc_info.value)
    assert "DynamoDB online store" in str(exc_info.value)


@pytest.mark.asyncio
async def test_update_online_store_async_success():
    """Test successful async update_online_store call."""

    with (
        patch(
            "feast.feature_store.FeatureStore._get_feature_view_and_df_for_online_write"
        ) as mock_get_fv_df,
        patch(
            "feast.infra.passthrough_provider.PassthroughProvider._prep_rows_to_write_for_ingestion"
        ) as mock_prep,
        patch("feast.feature_store.load_repo_config") as mock_load_config,
        patch("feast.feature_store.Registry"),
        patch("feast.feature_store.get_provider") as mock_get_provider,
    ):
        mock_online_store = Mock(spec=DynamoDBOnlineStore)
        mock_online_store.update_online_store_async = AsyncMock()
        mock_provider = Mock()
        mock_provider.online_store = mock_online_store
        mock_get_provider.return_value = mock_provider
        mock_prep.return_value = []
        mock_load_config.return_value = Mock()

        df = pd.DataFrame({"entity_id": ["1", "2"], "transactions": [["tx1"], ["tx2"]]})
        mock_feature_view = Mock()
        mock_feature_view.features = [Field(name="transactions", dtype=Array(String))]
        mock_get_fv_df.return_value = (mock_feature_view, df)

        store = FeatureStore()
        update_expressions = {"transactions": "list_append(transactions, :new_val)"}

        await store.update_online_store_async(
            feature_view_name="test_fv", df=df, update_expressions=update_expressions
        )

        mock_online_store.update_online_store_async.assert_called_once()
