from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from unittest.mock import patch

import boto3
import pytest
from moto import mock_dynamodb

from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.online_stores.dynamodb import (
    DynamoDBOnlineStore,
    DynamoDBOnlineStoreConfig,
    _get_table_name,
    _latest_data_to_write,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from tests.utils.dynamo_table_creator import (
    create_n_customer_test_samples,
    create_test_table,
    insert_data_test_table,
)

REGISTRY = "s3://test_registry/registry.db"
PROJECT = "test_aws"
PROVIDER = "aws"
TABLE_NAME = "dynamodb_online_store"
REGION = "us-west-2"


@dataclass
class MockFeatureView:
    name: str
    tags: Optional[dict[str, str]] = None


@dataclass
class MockOnlineConfig:
    tags: Optional[dict[str, str]] = None


@pytest.fixture
def repo_config():
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=DynamoDBOnlineStoreConfig(region=REGION),
        # online_store={"type": "dynamodb", "region": REGION},
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )


@pytest.fixture
def dynamodb_online_store():
    return DynamoDBOnlineStore()


def test_dynamodb_online_store_config_default():
    """Test DynamoDBOnlineStoreConfig default parameters."""
    aws_region = "us-west-2"
    dynamodb_store_config = DynamoDBOnlineStoreConfig(region=aws_region)
    assert dynamodb_store_config.type == "dynamodb"
    assert dynamodb_store_config.batch_size == 100
    assert dynamodb_store_config.endpoint_url is None
    assert dynamodb_store_config.region == aws_region
    assert dynamodb_store_config.table_name_template == "{project}.{table_name}"
    # Verify other optimized defaults
    assert dynamodb_store_config.max_pool_connections == 50
    assert dynamodb_store_config.keepalive_timeout == 30.0
    assert dynamodb_store_config.connect_timeout == 5
    assert dynamodb_store_config.read_timeout == 10
    assert dynamodb_store_config.total_max_retry_attempts == 3
    assert dynamodb_store_config.retry_mode == "adaptive"


def test_dynamodb_online_store_config_custom_params():
    """Test DynamoDBOnlineStoreConfig custom parameters."""
    aws_region = "us-west-2"
    batch_size = 20
    endpoint_url = "http://localhost:8000"
    table_name_template = "feast_test.dynamodb_table"
    dynamodb_store_config = DynamoDBOnlineStoreConfig(
        region=aws_region,
        batch_size=batch_size,
        endpoint_url=endpoint_url,
        table_name_template=table_name_template,
    )
    assert dynamodb_store_config.type == "dynamodb"
    assert dynamodb_store_config.batch_size == batch_size
    assert dynamodb_store_config.endpoint_url == endpoint_url
    assert dynamodb_store_config.region == aws_region
    assert dynamodb_store_config.table_name_template == table_name_template


def test_dynamodb_online_store_config_dynamodb_client(dynamodb_online_store):
    """Test DynamoDBOnlineStoreConfig configure DynamoDB client with endpoint_url."""
    aws_region = "us-west-2"
    endpoint_url = "http://localhost:8000"
    dynamodb_store_config = DynamoDBOnlineStoreConfig(
        region=aws_region, endpoint_url=endpoint_url
    )
    dynamodb_client = dynamodb_online_store._get_dynamodb_client(
        dynamodb_store_config.region, dynamodb_store_config.endpoint_url
    )
    assert dynamodb_client.meta.region_name == aws_region
    assert dynamodb_client.meta.endpoint_url == endpoint_url


def test_dynamodb_online_store_config_dynamodb_resource(dynamodb_online_store):
    """Test DynamoDBOnlineStoreConfig configure DynamoDB Resource with endpoint_url."""
    aws_region = "us-west-2"
    endpoint_url = "http://localhost:8000"
    dynamodb_store_config = DynamoDBOnlineStoreConfig(
        region=aws_region, endpoint_url=endpoint_url
    )
    dynamodb_resource = dynamodb_online_store._get_dynamodb_resource(
        dynamodb_store_config.region, dynamodb_store_config.endpoint_url
    )
    assert dynamodb_resource.meta.client.meta.region_name == aws_region
    assert dynamodb_resource.meta.client.meta.endpoint_url == endpoint_url


@mock_dynamodb
@pytest.mark.parametrize("n_samples", [5, 50, 100])
def test_dynamodb_online_store_online_read(
    repo_config, dynamodb_online_store, n_samples
):
    """Test DynamoDBOnlineStore online_read method."""
    db_table_name = f"{TABLE_NAME}_online_read_{n_samples}"
    create_test_table(PROJECT, db_table_name, REGION)
    data = create_n_customer_test_samples(n=n_samples)
    insert_data_test_table(data, PROJECT, db_table_name, REGION)

    entity_keys, features, *rest = zip(*data)
    returned_items = dynamodb_online_store.online_read(
        config=repo_config,
        table=MockFeatureView(name=db_table_name),
        entity_keys=entity_keys,
    )
    assert len(returned_items) == len(data)
    assert [item[1] for item in returned_items] == list(features)


@mock_dynamodb
@pytest.mark.parametrize("n_samples", [5, 50, 100])
def test_dynamodb_online_store_online_write_batch(
    repo_config, dynamodb_online_store, n_samples
):
    """Test DynamoDBOnlineStore online_write_batch method."""
    db_table_name = f"{TABLE_NAME}_online_write_batch_{n_samples}"
    create_test_table(PROJECT, db_table_name, REGION)
    data = create_n_customer_test_samples()

    entity_keys, features, *rest = zip(*data)
    dynamodb_online_store.online_write_batch(
        config=repo_config,
        table=MockFeatureView(name=db_table_name),
        data=data,
        progress=None,
    )
    stored_items = dynamodb_online_store.online_read(
        config=repo_config,
        table=MockFeatureView(name=db_table_name),
        entity_keys=entity_keys,
    )
    assert stored_items is not None
    assert len(stored_items) == len(data)
    assert [item[1] for item in stored_items] == list(features)


def _get_tags(dynamodb_client, table_name):
    table_arn = dynamodb_client.describe_table(TableName=table_name)["Table"][
        "TableArn"
    ]
    return dynamodb_client.list_tags_of_resource(ResourceArn=table_arn).get("Tags")


@mock_dynamodb
def test_dynamodb_online_store_update(repo_config, dynamodb_online_store):
    """Test DynamoDBOnlineStore update method."""
    # create dummy table to keep
    db_table_keep_name = f"{TABLE_NAME}_keep_update"
    create_test_table(PROJECT, db_table_keep_name, REGION)
    # create dummy table to delete
    db_table_delete_name = f"{TABLE_NAME}_delete_update"
    create_test_table(PROJECT, db_table_delete_name, REGION)

    # Mock _get_tags to avoid moto authentication issues in CI
    with (
        patch(f"{__name__}._get_tags") as mock_get_tags,
        patch.object(dynamodb_online_store, "_update_tags"),
    ):
        mock_get_tags.return_value = [{"Key": "some", "Value": "tag"}]

        dynamodb_online_store.update(
            config=repo_config,
            tables_to_delete=[MockFeatureView(name=db_table_delete_name)],
            tables_to_keep=[
                MockFeatureView(name=db_table_keep_name, tags={"some": "tag"})
            ],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        # check only db_table_keep_name exists
        dynamodb_client = dynamodb_online_store._get_dynamodb_client(REGION)
        existing_tables = dynamodb_client.list_tables()
        existing_tables = existing_tables.get("TableNames", None)

        assert existing_tables is not None
        assert len(existing_tables) == 1
        assert existing_tables[0] == f"test_aws.{db_table_keep_name}"

        # Call _get_tags and verify it returns the expected result
        result = _get_tags(dynamodb_client, existing_tables[0])
        assert result == [{"Key": "some", "Value": "tag"}]

        # Verify _get_tags was called with the correct table name
        mock_get_tags.assert_called_once_with(dynamodb_client, existing_tables[0])


@mock_dynamodb
def test_dynamodb_online_store_update_tags(repo_config, dynamodb_online_store):
    """Test DynamoDBOnlineStore update method."""
    # create dummy table to update with new tags and tag values
    table_name = f"{TABLE_NAME}_keep_update_tags"
    create_test_table(PROJECT, table_name, REGION)

    # Mock _update_tags to avoid moto authentication issues
    with patch.object(dynamodb_online_store, "_update_tags") as mock_update_tags:
        # add tags on update
        dynamodb_online_store.update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                MockFeatureView(
                    name=table_name,
                    tags={"key1": "val1", "key2": "val2", "key3": "val3"},
                )
            ],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=None,
        )

        # Verify _update_tags was called with correct arguments
        mock_update_tags.assert_called_once()
        call_args = mock_update_tags.call_args
        assert call_args[0][1] == f"{PROJECT}.{table_name}"  # table_name
        assert call_args[0][2] == [
            {"Key": "key1", "Value": "val1"},
            {"Key": "key2", "Value": "val2"},
            {"Key": "key3", "Value": "val3"},
        ]  # tags

        # Reset mock for next call
        mock_update_tags.reset_mock()

        # update tags
        dynamodb_online_store.update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                MockFeatureView(
                    name=table_name,
                    tags={"key1": "new-val1", "key2": "val2", "key4": "val4"},
                )
            ],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=None,
        )

        # Verify _update_tags was called with updated tags
        mock_update_tags.assert_called_once()
        call_args = mock_update_tags.call_args
        assert call_args[0][1] == f"{PROJECT}.{table_name}"  # table_name
        assert call_args[0][2] == [
            {"Key": "key1", "Value": "new-val1"},
            {"Key": "key2", "Value": "val2"},
            {"Key": "key4", "Value": "val4"},
        ]  # tags

        # Reset mock for next call
        mock_update_tags.reset_mock()

        # Check that table still exists
        dynamodb_client = dynamodb_online_store._get_dynamodb_client(REGION)
        existing_tables = dynamodb_client.list_tables().get("TableNames", None)
        assert existing_tables is not None
        assert len(existing_tables) == 1

        # and then remove all tags (tags=None means no tags)
        dynamodb_online_store.update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[MockFeatureView(name=table_name, tags=None)],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=None,
        )

        # Verify _update_tags was called with None tags
        mock_update_tags.assert_called_once()
        call_args = mock_update_tags.call_args
        assert call_args[0][1] == f"{PROJECT}.{table_name}"  # table_name
        assert call_args[0][2] == []  # tags should be empty list when None is passed


@mock_dynamodb
@pytest.mark.parametrize(
    "global_tags, table_tags, expected",
    [
        (None, {"key": "val"}, [{"Key": "key", "Value": "val"}]),
        ({"key": "val"}, None, [{"Key": "key", "Value": "val"}]),
        (
            {"key1": "val1"},
            {"key2": "val2"},
            [{"Key": "key1", "Value": "val1"}, {"Key": "key2", "Value": "val2"}],
        ),
        (
            {"key": "val", "key2": "val2"},
            {"key": "new-val"},
            [{"Key": "key", "Value": "new-val"}, {"Key": "key2", "Value": "val2"}],
        ),
    ],
)
def test_dynamodb_online_store_tag_priority(
    global_tags, table_tags, expected, dynamodb_online_store
):
    actual = dynamodb_online_store._table_tags(
        MockOnlineConfig(tags=global_tags),
        MockFeatureView(name="table", tags=table_tags),
    )
    assert actual == expected


@mock_dynamodb
def test_dynamodb_online_store_teardown(repo_config, dynamodb_online_store):
    """Test DynamoDBOnlineStore teardown method."""
    db_table_delete_name_one = f"{TABLE_NAME}_delete_teardown_1"
    db_table_delete_name_two = f"{TABLE_NAME}_delete_teardown_2"
    create_test_table(PROJECT, db_table_delete_name_one, REGION)
    create_test_table(PROJECT, db_table_delete_name_two, REGION)

    dynamodb_online_store.teardown(
        config=repo_config,
        tables=[
            MockFeatureView(name=db_table_delete_name_one),
            MockFeatureView(name=db_table_delete_name_two),
        ],
        entities=None,
    )

    # Check tables non exist
    dynamodb_client = dynamodb_online_store._get_dynamodb_client(REGION)
    existing_tables = dynamodb_client.list_tables()
    existing_tables = existing_tables.get("TableNames", None)

    assert existing_tables is not None
    assert len(existing_tables) == 0


@mock_dynamodb
def test_dynamodb_online_store_online_read_unknown_entity(
    repo_config, dynamodb_online_store
):
    """Test DynamoDBOnlineStore online_read method."""
    n_samples = 2
    create_test_table(PROJECT, f"{TABLE_NAME}_unknown_entity_{n_samples}", REGION)
    data = create_n_customer_test_samples(n=n_samples)
    insert_data_test_table(
        data, PROJECT, f"{TABLE_NAME}_unknown_entity_{n_samples}", REGION
    )

    entity_keys, features, *rest = zip(*data)
    # Append a nonsensical entity to search for
    entity_keys = list(entity_keys)
    features = list(features)

    # Have the unknown entity be in the beginning, middle, and end of the list of entities.
    for pos in range(len(entity_keys)):
        entity_keys_with_unknown = deepcopy(entity_keys)
        entity_keys_with_unknown.insert(
            pos,
            EntityKeyProto(
                join_keys=["customer"], entity_values=[ValueProto(string_val="12359")]
            ),
        )
        features_with_none = deepcopy(features)
        features_with_none.insert(pos, None)
        returned_items = dynamodb_online_store.online_read(
            config=repo_config,
            table=MockFeatureView(name=f"{TABLE_NAME}_unknown_entity_{n_samples}"),
            entity_keys=entity_keys_with_unknown,
        )
        assert len(returned_items) == len(entity_keys_with_unknown)
        assert [item[1] for item in returned_items] == list(features_with_none)
        # The order should match the original entity key order
        assert returned_items[pos] == (None, None)


@mock_dynamodb
def test_write_batch_non_duplicates(repo_config, dynamodb_online_store):
    """Test DynamoDBOnline Store deduplicate write batch request items."""
    dynamodb_tbl = f"{TABLE_NAME}_batch_non_duplicates"
    create_test_table(PROJECT, dynamodb_tbl, REGION)
    data = create_n_customer_test_samples()
    data_duplicate = deepcopy(data)
    dynamodb_resource = boto3.resource("dynamodb", region_name=REGION)
    table_instance = dynamodb_resource.Table(f"{PROJECT}.{dynamodb_tbl}")
    # Insert duplicate data
    dynamodb_online_store._write_batch_non_duplicates(
        table_instance, data + data_duplicate, None, repo_config
    )
    # Request more items than inserted
    response = table_instance.scan(Limit=20)
    returned_items = response.get("Items", None)
    assert returned_items is not None
    assert len(returned_items) == len(data)


@mock_dynamodb
def test_dynamodb_online_store_online_read_unknown_entity_end_of_batch(
    repo_config, dynamodb_online_store
):
    """
    Test DynamoDBOnlineStore online_read method with unknown entities at
    the end of the batch.
    """
    batch_size = repo_config.online_store.batch_size
    n_samples = batch_size
    create_test_table(PROJECT, f"{TABLE_NAME}_unknown_entity_{n_samples}", REGION)
    data = create_n_customer_test_samples(n=n_samples)
    insert_data_test_table(
        data, PROJECT, f"{TABLE_NAME}_unknown_entity_{n_samples}", REGION
    )

    entity_keys, features, *rest = zip(*data)
    entity_keys = list(entity_keys)
    features = list(features)

    # Append a nonsensical entity to search for as the only item in the 2nd batch
    entity_keys.append(
        EntityKeyProto(
            join_keys=["customer"], entity_values=[ValueProto(string_val="12359")]
        )
    )
    features.append(None)

    returned_items = dynamodb_online_store.online_read(
        config=repo_config,
        table=MockFeatureView(name=f"{TABLE_NAME}_unknown_entity_{n_samples}"),
        entity_keys=entity_keys,
    )

    # ensure the entity is not dropped
    assert len(returned_items) == len(entity_keys)
    assert returned_items[-1] == (None, None)


def test_batch_write_deduplication():
    def to_ek_proto(val):
        return EntityKeyProto(
            join_keys=["customer"], entity_values=[ValueProto(string_val=val)]
        )

    # is out of order and has duplicate keys
    data = [
        (to_ek_proto("key-1"), {}, datetime(2024, 1, 1), None),
        (to_ek_proto("key-2"), {}, datetime(2024, 1, 1), None),
        (to_ek_proto("key-1"), {}, datetime(2024, 1, 3), None),
        (to_ek_proto("key-1"), {}, datetime(2024, 1, 2), None),
        (to_ek_proto("key-3"), {}, datetime(2024, 1, 2), None),
    ]

    # assert we only keep the most recent record per key
    actual = list(_latest_data_to_write(data))
    expected = [data[2], data[1], data[4]]
    assert expected == actual


def _create_entity_key(entity_id: str) -> EntityKeyProto:
    """Helper function to create EntityKeyProto for testing."""
    return EntityKeyProto(
        join_keys=["customer"], entity_values=[ValueProto(string_val=entity_id)]
    )


def _create_string_list_value(items: list[str]) -> ValueProto:
    """Helper function to create ValueProto with string list."""
    from feast.protos.feast.types.Value_pb2 import StringList

    return ValueProto(string_list_val=StringList(val=items))


def _create_int32_list_value(items: list[int]) -> ValueProto:
    """Helper function to create ValueProto with int32 list."""
    from feast.protos.feast.types.Value_pb2 import Int32List

    return ValueProto(int32_list_val=Int32List(val=items))


def _extract_string_list(value_proto: ValueProto) -> list[str]:
    """Helper function to extract string list from ValueProto."""
    return list(value_proto.string_list_val.val)


def _extract_int32_list(value_proto: ValueProto) -> list[int]:
    """Helper function to extract int32 list from ValueProto."""
    return list(value_proto.int32_list_val.val)


@mock_dynamodb
def test_dynamodb_update_online_store_list_append(repo_config, dynamodb_online_store):
    """Test DynamoDB update_online_store with list_append operation."""

    table_name = f"{TABLE_NAME}_update_list_append"
    create_test_table(PROJECT, table_name, REGION)

    # Create initial data with existing transactions
    initial_data = [
        (
            _create_entity_key("entity1"),
            {"transactions": _create_string_list_value(["tx1", "tx2"])},
            datetime.utcnow(),
            None,
        )
    ]

    # Write initial data using standard method
    dynamodb_online_store.online_write_batch(
        repo_config, MockFeatureView(name=table_name), initial_data, None
    )

    # Update with list_append - should append new transaction
    update_data = [
        (
            _create_entity_key("entity1"),
            {"transactions": _create_string_list_value(["tx3"])},
            datetime.utcnow(),
            None,
        )
    ]

    update_expressions = {"transactions": "list_append(transactions, :new_val)"}

    dynamodb_online_store.update_online_store(
        repo_config,
        MockFeatureView(name=table_name),
        update_data,
        update_expressions,
        None,
    )

    # Verify result - should have all three transactions
    result = dynamodb_online_store.online_read(
        repo_config, MockFeatureView(name=table_name), [_create_entity_key("entity1")]
    )

    assert len(result) == 1
    assert result[0][0] is not None  # timestamp should exist
    assert result[0][1] is not None  # features should exist
    transactions = result[0][1]["transactions"]
    assert _extract_string_list(transactions) == ["tx1", "tx2", "tx3"]


@mock_dynamodb
def test_dynamodb_update_online_store_list_prepend(repo_config, dynamodb_online_store):
    """Test DynamoDB update_online_store with list prepend operation."""

    table_name = f"{TABLE_NAME}_update_list_prepend"
    create_test_table(PROJECT, table_name, REGION)

    # Create initial data
    initial_data = [
        (
            _create_entity_key("entity1"),
            {"recent_items": _create_string_list_value(["item2", "item3"])},
            datetime.utcnow(),
            None,
        )
    ]

    dynamodb_online_store.online_write_batch(
        repo_config, MockFeatureView(name=table_name), initial_data, None
    )

    # Update with list prepend - should add new item at the beginning
    update_data = [
        (
            _create_entity_key("entity1"),
            {"recent_items": _create_string_list_value(["item1"])},
            datetime.utcnow(),
            None,
        )
    ]

    update_expressions = {"recent_items": "list_append(:new_val, recent_items)"}

    dynamodb_online_store.update_online_store(
        repo_config,
        MockFeatureView(name=table_name),
        update_data,
        update_expressions,
        None,
    )

    # Verify result - new item should be first
    result = dynamodb_online_store.online_read(
        repo_config, MockFeatureView(name=table_name), [_create_entity_key("entity1")]
    )

    assert len(result) == 1
    recent_items = result[0][1]["recent_items"]
    assert _extract_string_list(recent_items) == ["item1", "item2", "item3"]


@mock_dynamodb
def test_dynamodb_update_online_store_new_entity(repo_config, dynamodb_online_store):
    """Test DynamoDB update_online_store with new entity (no existing data)."""

    table_name = f"{TABLE_NAME}_update_new_entity"
    create_test_table(PROJECT, table_name, REGION)

    # Update entity that doesn't exist yet - should create new item
    update_data = [
        (
            _create_entity_key("new_entity"),
            {"transactions": _create_string_list_value(["tx1"])},
            datetime.utcnow(),
            None,
        )
    ]

    update_expressions = {"transactions": "list_append(transactions, :new_val)"}

    dynamodb_online_store.update_online_store(
        repo_config,
        MockFeatureView(name=table_name),
        update_data,
        update_expressions,
        None,
    )

    # Verify result - should create new item with the transaction
    result = dynamodb_online_store.online_read(
        repo_config,
        MockFeatureView(name=table_name),
        [_create_entity_key("new_entity")],
    )

    assert len(result) == 1
    assert result[0][0] is not None  # timestamp should exist
    assert result[0][1] is not None  # features should exist
    transactions = result[0][1]["transactions"]
    assert _extract_string_list(transactions) == ["tx1"]


@mock_dynamodb
def test_dynamodb_update_online_store_mixed_operations(
    repo_config, dynamodb_online_store
):
    """Test DynamoDB update_online_store with mixed update and replace operations."""

    table_name = f"{TABLE_NAME}_update_mixed"
    create_test_table(PROJECT, table_name, REGION)

    # Create initial data
    initial_data = [
        (
            _create_entity_key("entity1"),
            {
                "transactions": _create_string_list_value(["tx1"]),
                "user_score": ValueProto(int32_val=100),
            },
            datetime.utcnow(),
            None,
        )
    ]

    dynamodb_online_store.online_write_batch(
        repo_config, MockFeatureView(name=table_name), initial_data, None
    )

    # Update with mixed operations - append to list and replace scalar
    update_data = [
        (
            _create_entity_key("entity1"),
            {
                "transactions": _create_string_list_value(["tx2"]),
                "user_score": ValueProto(int32_val=150),
            },
            datetime.utcnow(),
            None,
        )
    ]

    update_expressions = {
        "transactions": "list_append(transactions, :new_val)",
        # user_score will use standard replacement (no expression)
    }

    dynamodb_online_store.update_online_store(
        repo_config,
        MockFeatureView(name=table_name),
        update_data,
        update_expressions,
        None,
    )

    # Verify result
    result = dynamodb_online_store.online_read(
        repo_config, MockFeatureView(name=table_name), [_create_entity_key("entity1")]
    )

    assert len(result) == 1
    features = result[0][1]

    # Transactions should be appended
    transactions = features["transactions"]
    assert _extract_string_list(transactions) == ["tx1", "tx2"]

    # User score should be replaced
    user_score = features["user_score"]
    assert user_score.int32_val == 150


@mock_dynamodb
def test_dynamodb_update_online_store_int_list(repo_config, dynamodb_online_store):
    """Test DynamoDB update_online_store with integer list."""

    table_name = f"{TABLE_NAME}_update_int_list"
    create_test_table(PROJECT, table_name, REGION)

    # Create initial data with integer list
    initial_data = [
        (
            _create_entity_key("entity1"),
            {"scores": _create_int32_list_value([10, 20])},
            datetime.utcnow(),
            None,
        )
    ]

    dynamodb_online_store.online_write_batch(
        repo_config, MockFeatureView(name=table_name), initial_data, None
    )

    # Update with list_append for integer list
    update_data = [
        (
            _create_entity_key("entity1"),
            {"scores": _create_int32_list_value([30])},
            datetime.utcnow(),
            None,
        )
    ]

    update_expressions = {"scores": "list_append(scores, :new_val)"}

    dynamodb_online_store.update_online_store(
        repo_config,
        MockFeatureView(name=table_name),
        update_data,
        update_expressions,
        None,
    )

    # Verify result
    result = dynamodb_online_store.online_read(
        repo_config, MockFeatureView(name=table_name), [_create_entity_key("entity1")]
    )

    assert len(result) == 1
    scores = result[0][1]["scores"]
    assert _extract_int32_list(scores) == [10, 20, 30]


@mock_dynamodb
def test_dynamodb_online_store_online_read_empty_entities(
    repo_config, dynamodb_online_store
):
    """Test DynamoDBOnlineStore online_read with empty entity list."""
    db_table_name = f"{TABLE_NAME}_empty_entities"
    create_test_table(PROJECT, db_table_name, REGION)

    returned_items = dynamodb_online_store.online_read(
        config=repo_config,
        table=MockFeatureView(name=db_table_name),
        entity_keys=[],
    )
    assert returned_items == []


@mock_dynamodb
def test_dynamodb_online_store_online_read_parallel_batches(
    repo_config, dynamodb_online_store
):
    """Test DynamoDBOnlineStore online_read with multiple batches (parallel execution).

    With batch_size=100 (default), 250 entities should create 3 batches
    that are executed in parallel via ThreadPoolExecutor.
    """
    n_samples = 250
    db_table_name = f"{TABLE_NAME}_parallel_batches"
    create_test_table(PROJECT, db_table_name, REGION)
    data = create_n_customer_test_samples(n=n_samples)
    insert_data_test_table(data, PROJECT, db_table_name, REGION)

    entity_keys, features, *rest = zip(*data)
    returned_items = dynamodb_online_store.online_read(
        config=repo_config,
        table=MockFeatureView(name=db_table_name),
        entity_keys=entity_keys,
    )

    # Verify all items returned
    assert len(returned_items) == n_samples
    # Verify order is preserved
    assert [item[1] for item in returned_items] == list(features)


@mock_dynamodb
def test_dynamodb_online_store_online_read_single_batch_no_parallel(
    repo_config, dynamodb_online_store
):
    """Test DynamoDBOnlineStore online_read with single batch (no parallelization).

    With batch_size=100, 50 entities should use single batch path
    without ThreadPoolExecutor overhead.
    """
    n_samples = 50
    db_table_name = f"{TABLE_NAME}_single_batch"
    create_test_table(PROJECT, db_table_name, REGION)
    data = create_n_customer_test_samples(n=n_samples)
    insert_data_test_table(data, PROJECT, db_table_name, REGION)

    entity_keys, features, *rest = zip(*data)
    returned_items = dynamodb_online_store.online_read(
        config=repo_config,
        table=MockFeatureView(name=db_table_name),
        entity_keys=entity_keys,
    )

    assert len(returned_items) == n_samples
    assert [item[1] for item in returned_items] == list(features)


@mock_dynamodb
def test_dynamodb_online_store_online_read_order_preservation_across_batches(
    repo_config, dynamodb_online_store
):
    """Test that entity order is preserved across parallel batch reads.

    This is critical: parallel execution must not change the order of results.
    """
    n_samples = 150  # 2 batches with batch_size=100
    db_table_name = f"{TABLE_NAME}_order_preservation"
    create_test_table(PROJECT, db_table_name, REGION)
    data = create_n_customer_test_samples(n=n_samples)
    insert_data_test_table(data, PROJECT, db_table_name, REGION)

    entity_keys, features, *rest = zip(*data)

    # Read multiple times to verify consistent ordering
    for _ in range(3):
        returned_items = dynamodb_online_store.online_read(
            config=repo_config,
            table=MockFeatureView(name=db_table_name),
            entity_keys=entity_keys,
        )
        assert len(returned_items) == n_samples
        # Verify exact order matches
        for i, (returned, expected) in enumerate(zip(returned_items, features)):
            assert returned[1] == expected, f"Mismatch at index {i}"


@mock_dynamodb
def test_dynamodb_online_store_online_read_small_batch_size(dynamodb_online_store):
    """Test parallel reads with small batch_size.

    Verifies correctness with small batch sizes that create multiple batches.
    """
    small_batch_config = RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=DynamoDBOnlineStoreConfig(region=REGION, batch_size=5),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )

    n_samples = 25  # 5 batches with batch_size=5
    db_table_name = f"{TABLE_NAME}_small_batch"
    create_test_table(PROJECT, db_table_name, REGION)
    data = create_n_customer_test_samples(n=n_samples)
    insert_data_test_table(data, PROJECT, db_table_name, REGION)

    entity_keys, features, *rest = zip(*data)
    returned_items = dynamodb_online_store.online_read(
        config=small_batch_config,
        table=MockFeatureView(name=db_table_name),
        entity_keys=entity_keys,
    )

    assert len(returned_items) == n_samples
    assert [item[1] for item in returned_items] == list(features)


@mock_dynamodb
def test_dynamodb_online_store_online_read_many_batches(dynamodb_online_store):
    """Test parallel reads with many batches (>10).

    Verifies correctness when number of batches exceeds max_workers cap.
    """
    many_batch_config = RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=DynamoDBOnlineStoreConfig(region=REGION, batch_size=10),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )

    n_samples = 150  # 15 batches with batch_size=10
    db_table_name = f"{TABLE_NAME}_many_batches"
    create_test_table(PROJECT, db_table_name, REGION)
    data = create_n_customer_test_samples(n=n_samples)
    insert_data_test_table(data, PROJECT, db_table_name, REGION)

    entity_keys, features, *rest = zip(*data)
    returned_items = dynamodb_online_store.online_read(
        config=many_batch_config,
        table=MockFeatureView(name=db_table_name),
        entity_keys=entity_keys,
    )

    assert len(returned_items) == n_samples
    assert [item[1] for item in returned_items] == list(features)


@mock_dynamodb
def test_dynamodb_online_store_max_workers_capped_at_config(dynamodb_online_store):
    """Verify ThreadPoolExecutor max_workers uses max_read_workers config.

    Bug: Old code used min(len(batches), batch_size) which fails with small batch_size.
    Fix: New code uses min(len(batches), max_read_workers) for proper parallelization.

    This test uses batch_size=5 with 15 batches to expose the bug:
    - OLD (buggy): max_workers = min(15, 5) = 5  (insufficient parallelism)
    - NEW (fixed): max_workers = min(15, 10) = 10 (uses max_read_workers default)
    """
    # Use small batch_size to expose the bug
    small_batch_config = RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=DynamoDBOnlineStoreConfig(region=REGION, batch_size=5),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )

    n_samples = 75  # 15 batches with batch_size=5
    db_table_name = f"{TABLE_NAME}_max_workers_cap"
    create_test_table(PROJECT, db_table_name, REGION)
    data = create_n_customer_test_samples(n=n_samples)
    insert_data_test_table(data, PROJECT, db_table_name, REGION)

    entity_keys, features, *rest = zip(*data)

    with patch(
        "feast.infra.online_stores.dynamodb.ThreadPoolExecutor"
    ) as mock_executor:
        # Configure mock to work like real ThreadPoolExecutor
        mock_executor.return_value.__enter__.return_value.map.return_value = iter(
            [{"Responses": {}} for _ in range(15)]
        )

        dynamodb_online_store.online_read(
            config=small_batch_config,
            table=MockFeatureView(name=db_table_name),
            entity_keys=entity_keys,
        )

        # Verify ThreadPoolExecutor was called with max_workers=10 (capped at 10, NOT batch_size=5)
        mock_executor.assert_called_once()
        call_kwargs = mock_executor.call_args
        assert call_kwargs[1]["max_workers"] == 10, (
            f"Expected max_workers=10 (capped), got {call_kwargs[1]['max_workers']}. "
            f"If got 5, the bug is using batch_size instead of 10 as cap."
        )


@mock_dynamodb
def test_dynamodb_online_store_thread_safety_uses_shared_client(
    dynamodb_online_store,
):
    """Verify multi-batch reads use a shared thread-safe boto3 client.

    boto3 clients ARE thread-safe, so we share a single client across threads
    for better performance (avoids creating new sessions per thread).
    https://docs.aws.amazon.com/boto3/latest/guide/clients.html#multithreading-or-multiprocessing-with-clients
    """
    config = RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=DynamoDBOnlineStoreConfig(region=REGION, batch_size=50),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )

    n_samples = 150  # 3 batches
    db_table_name = f"{TABLE_NAME}_thread_safety"
    create_test_table(PROJECT, db_table_name, REGION)
    data = create_n_customer_test_samples(n=n_samples)
    insert_data_test_table(data, PROJECT, db_table_name, REGION)

    entity_keys, features, *rest = zip(*data)

    # Track clients created to verify thread-safety via shared client
    clients_created = []
    original_client = boto3.client

    def tracking_client(*args, **kwargs):
        client = original_client(*args, **kwargs)
        clients_created.append(id(client))
        return client

    with patch.object(boto3, "client", side_effect=tracking_client):
        returned_items = dynamodb_online_store.online_read(
            config=config,
            table=MockFeatureView(name=db_table_name),
            entity_keys=entity_keys,
        )

    # Verify results are correct (functional correctness)
    assert len(returned_items) == n_samples

    # Verify only one client was created (shared across threads)
    # The client is cached and reused for all batch requests
    dynamodb_clients = [c for c in clients_created]
    assert len(set(dynamodb_clients)) == 1, (
        f"Expected 1 shared client for thread-safety, "
        f"got {len(set(dynamodb_clients))} unique clients"
    )


@dataclass
class MockProjection:
    version_tag: Optional[int] = None


@dataclass
class MockFeatureViewWithProjection:
    name: str
    projection: MockProjection
    current_version_number: Optional[int] = None
    tags: Optional[dict] = None


def _make_repo_config(enable_versioning: bool = False) -> RepoConfig:
    from feast.repo_config import RegistryConfig

    registry_cfg = RegistryConfig(
        path="s3://test_registry/registry.db",
        enable_online_feature_view_versioning=enable_versioning,
    )
    return RepoConfig(
        registry=registry_cfg,
        project=PROJECT,
        provider=PROVIDER,
        online_store=DynamoDBOnlineStoreConfig(region=REGION),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )


def test_get_table_name_no_versioning():
    """Without versioning enabled, table name is always the plain template."""
    config = _make_repo_config(enable_versioning=False)
    online_config = config.online_store
    fv = MockFeatureViewWithProjection(
        name="driver_stats",
        projection=MockProjection(version_tag=2),
        current_version_number=2,
    )
    assert _get_table_name(online_config, config, fv) == f"{PROJECT}.driver_stats"


def test_get_table_name_versioning_with_projection_version_tag():
    """When versioning is enabled, projection.version_tag takes precedence."""
    config = _make_repo_config(enable_versioning=True)
    online_config = config.online_store
    fv = MockFeatureViewWithProjection(
        name="driver_stats",
        projection=MockProjection(version_tag=2),
        current_version_number=5,  # should be ignored in favour of projection tag
    )
    assert _get_table_name(online_config, config, fv) == f"{PROJECT}.driver_stats_v2"


def test_get_table_name_versioning_falls_back_to_current_version_number():
    """When projection.version_tag is None, current_version_number is used."""
    config = _make_repo_config(enable_versioning=True)
    online_config = config.online_store
    fv = MockFeatureViewWithProjection(
        name="driver_stats",
        projection=MockProjection(version_tag=None),
        current_version_number=3,
    )
    assert _get_table_name(online_config, config, fv) == f"{PROJECT}.driver_stats_v3"


def test_get_table_name_versioning_version_zero_is_unversioned():
    """A version value of 0 should not add a suffix (sentinel for 'no version')."""
    config = _make_repo_config(enable_versioning=True)
    online_config = config.online_store
    fv = MockFeatureViewWithProjection(
        name="driver_stats",
        projection=MockProjection(version_tag=0),
        current_version_number=0,
    )
    assert _get_table_name(online_config, config, fv) == f"{PROJECT}.driver_stats"


def test_get_table_name_versioning_no_version_info():
    """When versioning is enabled but no version is set, plain name is used."""
    config = _make_repo_config(enable_versioning=True)
    online_config = config.online_store
    fv = MockFeatureViewWithProjection(
        name="driver_stats",
        projection=MockProjection(version_tag=None),
        current_version_number=None,
    )
    assert _get_table_name(online_config, config, fv) == f"{PROJECT}.driver_stats"


def test_get_table_name_custom_template_with_versioning():
    """Custom table_name_template respects the versioned table_name fragment."""
    from feast.repo_config import RegistryConfig

    registry_cfg = RegistryConfig(
        path="s3://test_registry/registry.db",
        enable_online_feature_view_versioning=True,
    )
    config = RepoConfig(
        registry=registry_cfg,
        project=PROJECT,
        provider=PROVIDER,
        online_store=DynamoDBOnlineStoreConfig(
            region=REGION,
            table_name_template="{project}__{table_name}",
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=3,
    )
    online_config = config.online_store
    fv = MockFeatureViewWithProjection(
        name="driver_stats",
        projection=MockProjection(version_tag=4),
    )
    assert _get_table_name(online_config, config, fv) == f"{PROJECT}__driver_stats_v4"
