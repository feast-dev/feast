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
    assert dynamodb_store_config.batch_size == 40
    assert dynamodb_store_config.endpoint_url is None
    assert dynamodb_store_config.region == aws_region
    assert dynamodb_store_config.table_name_template == "{project}.{table_name}"


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
