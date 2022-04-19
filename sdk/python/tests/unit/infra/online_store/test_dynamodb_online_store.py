from copy import deepcopy
from dataclasses import dataclass

import boto3
import pytest
from moto import mock_dynamodb2

from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.online_stores.dynamodb import (
    DynamoDBOnlineStore,
    DynamoDBOnlineStoreConfig,
    DynamoDBTable,
)
from feast.repo_config import RepoConfig
from tests.utils.online_store_utils import (
    _create_n_customer_test_samples,
    _create_test_table,
    _insert_data_test_table,
)

REGISTRY = "s3://test_registry/registry.db"
PROJECT = "test_aws"
PROVIDER = "aws"
TABLE_NAME = "dynamodb_online_store"
REGION = "us-west-2"


@dataclass
class MockFeatureView:
    name: str


@pytest.fixture
def repo_config():
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=DynamoDBOnlineStoreConfig(region=REGION),
        offline_store=FileOfflineStoreConfig(),
    )


@pytest.fixture
def dynamodb_online_store():
    return DynamoDBOnlineStore()


def test_online_store_config_default():
    """Test DynamoDBOnlineStoreConfig default parameters."""
    aws_region = "us-west-2"
    dynamodb_store_config = DynamoDBOnlineStoreConfig(region=aws_region)
    assert dynamodb_store_config.type == "dynamodb"
    assert dynamodb_store_config.batch_size == 40
    assert dynamodb_store_config.endpoint_url is None
    assert dynamodb_store_config.region == aws_region
    assert dynamodb_store_config.sort_response is True
    assert dynamodb_store_config.table_name_template == "{project}.{table_name}"


def test_dynamodb_table_default_params():
    """Test DynamoDBTable default parameters."""
    tbl_name = "dynamodb-test"
    aws_region = "us-west-2"
    dynamodb_table = DynamoDBTable(tbl_name, aws_region)
    assert dynamodb_table.name == tbl_name
    assert dynamodb_table.region == aws_region
    assert dynamodb_table.endpoint_url is None
    assert dynamodb_table._dynamodb_client is None
    assert dynamodb_table._dynamodb_resource is None


def test_online_store_config_custom_params():
    """Test DynamoDBOnlineStoreConfig custom parameters."""
    aws_region = "us-west-2"
    batch_size = 20
    endpoint_url = "http://localhost:8000"
    sort_response = False
    table_name_template = "feast_test.dynamodb_table"
    dynamodb_store_config = DynamoDBOnlineStoreConfig(
        region=aws_region,
        batch_size=batch_size,
        endpoint_url=endpoint_url,
        sort_response=sort_response,
        table_name_template=table_name_template,
    )
    assert dynamodb_store_config.type == "dynamodb"
    assert dynamodb_store_config.batch_size == batch_size
    assert dynamodb_store_config.endpoint_url == endpoint_url
    assert dynamodb_store_config.region == aws_region
    assert dynamodb_store_config.sort_response == sort_response
    assert dynamodb_store_config.table_name_template == table_name_template


def test_dynamodb_table_custom_params():
    """Test DynamoDBTable custom parameters."""
    tbl_name = "dynamodb-test"
    aws_region = "us-west-2"
    endpoint_url = "http://localhost:8000"
    dynamodb_table = DynamoDBTable(tbl_name, aws_region, endpoint_url)
    assert dynamodb_table.name == tbl_name
    assert dynamodb_table.region == aws_region
    assert dynamodb_table.endpoint_url == endpoint_url
    assert dynamodb_table._dynamodb_client is None
    assert dynamodb_table._dynamodb_resource is None


def test_online_store_config_dynamodb_client():
    """Test DynamoDBOnlineStoreConfig configure DynamoDB client with endpoint_url."""
    aws_region = "us-west-2"
    endpoint_url = "http://localhost:8000"
    dynamodb_store = DynamoDBOnlineStore()
    dynamodb_store_config = DynamoDBOnlineStoreConfig(
        region=aws_region, endpoint_url=endpoint_url
    )
    dynamodb_client = dynamodb_store._get_dynamodb_client(
        dynamodb_store_config.region, dynamodb_store_config.endpoint_url
    )
    assert dynamodb_client.meta.region_name == aws_region
    assert dynamodb_client.meta.endpoint_url == endpoint_url


def test_dynamodb_table_dynamodb_client():
    """Test DynamoDBTable configure DynamoDB client with endpoint_url."""
    tbl_name = "dynamodb-test"
    aws_region = "us-west-2"
    endpoint_url = "http://localhost:8000"
    dynamodb_table = DynamoDBTable(tbl_name, aws_region, endpoint_url)
    dynamodb_client = dynamodb_table._get_dynamodb_client(
        dynamodb_table.region, dynamodb_table.endpoint_url
    )
    assert dynamodb_client.meta.region_name == aws_region
    assert dynamodb_client.meta.endpoint_url == endpoint_url


def test_online_store_config_dynamodb_resource(dynamodb_online_store):
    """Test DynamoDBOnlineStoreConfig configure DynamoDB Resource with endpoint_url."""
    aws_region = "us-west-2"
    endpoint_url = "http://localhost:8000"
    dynamodb_store = dynamodb_online_store
    dynamodb_store_config = DynamoDBOnlineStoreConfig(
        region=aws_region, endpoint_url=endpoint_url
    )
    dynamodb_resource = dynamodb_store._get_dynamodb_resource(
        dynamodb_store_config.region, dynamodb_store_config.endpoint_url
    )
    assert dynamodb_resource.meta.client.meta.region_name == aws_region
    assert dynamodb_resource.meta.client.meta.endpoint_url == endpoint_url


def test_dynamodb_table_dynamodb_resource():
    """Test DynamoDBTable configure DynamoDB resource with endpoint_url."""
    tbl_name = "dynamodb-test"
    aws_region = "us-west-2"
    endpoint_url = "http://localhost:8000"
    dynamodb_table = DynamoDBTable(tbl_name, aws_region, endpoint_url)
    dynamodb_resource = dynamodb_table._get_dynamodb_resource(
        dynamodb_table.region, dynamodb_table.endpoint_url
    )
    assert dynamodb_resource.meta.client.meta.region_name == aws_region
    assert dynamodb_resource.meta.client.meta.endpoint_url == endpoint_url


@mock_dynamodb2
@pytest.mark.parametrize("n_samples", [5, 50, 100])
def test_online_read(repo_config, dynamodb_online_store, n_samples):
    """Test DynamoDBOnlineStore online_read method."""
    db_table_name = f"{TABLE_NAME}_online_read_{n_samples}"
    _create_test_table(PROJECT, db_table_name, REGION)
    data = _create_n_customer_test_samples(n=n_samples)
    _insert_data_test_table(data, PROJECT, db_table_name, REGION)

    entity_keys, features, *rest = zip(*data)
    dynamodb_store = dynamodb_online_store
    returned_items = dynamodb_store.online_read(
        config=repo_config,
        table=MockFeatureView(name=db_table_name),
        entity_keys=entity_keys,
    )
    assert len(returned_items) == len(data)
    assert [item[1] for item in returned_items] == list(features)


@mock_dynamodb2
@pytest.mark.parametrize("n_samples", [5, 50, 100])
def test_online_write_batch(repo_config, dynamodb_online_store, n_samples):
    """Test DynamoDBOnlineStore online_write_batch method."""
    db_table_name = f"{TABLE_NAME}_online_write_batch_{n_samples}"
    _create_test_table(PROJECT, db_table_name, REGION)
    data = _create_n_customer_test_samples()

    entity_keys, features, *rest = zip(*data)
    dynamodb_store = dynamodb_online_store
    dynamodb_store.online_write_batch(
        config=repo_config,
        table=MockFeatureView(name=db_table_name),
        data=data,
        progress=None,
    )
    stored_items = dynamodb_store.online_read(
        config=repo_config,
        table=MockFeatureView(name=db_table_name),
        entity_keys=entity_keys,
    )
    assert stored_items is not None
    assert len(stored_items) == len(data)
    assert [item[1] for item in stored_items] == list(features)


@mock_dynamodb2
def test_update(repo_config, dynamodb_online_store):
    """Test DynamoDBOnlineStore update method."""
    # create dummy table to keep
    db_table_keep_name = f"{TABLE_NAME}_keep_update"
    _create_test_table(PROJECT, db_table_keep_name, REGION)
    # create dummy table to delete
    db_table_delete_name = f"{TABLE_NAME}_delete_update"
    _create_test_table(PROJECT, db_table_delete_name, REGION)

    dynamodb_store = dynamodb_online_store
    dynamodb_store.update(
        config=repo_config,
        tables_to_delete=[MockFeatureView(name=db_table_delete_name)],
        tables_to_keep=[MockFeatureView(name=db_table_keep_name)],
        entities_to_delete=None,
        entities_to_keep=None,
        partial=None,
    )

    # check only db_table_keep_name exists
    dynamodb_client = dynamodb_store._get_dynamodb_client(REGION)
    existing_tables = dynamodb_client.list_tables()
    existing_tables = existing_tables.get("TableNames", None)

    assert existing_tables is not None
    assert len(existing_tables) == 1
    assert existing_tables[0] == f"test_aws.{db_table_keep_name}"


@mock_dynamodb2
def test_write_batch_non_duplicates(repo_config, dynamodb_online_store):
    """Test DynamoDBOnline Store deduplicate write batch request items."""
    dynamodb_tbl = f"{TABLE_NAME}_batch_non_duplicates"
    _create_test_table(PROJECT, dynamodb_tbl, REGION)
    data = _create_n_customer_test_samples()
    data_duplicate = deepcopy(data)
    dynamodb_resource = boto3.resource("dynamodb", region_name=REGION)
    table_instance = dynamodb_resource.Table(f"{PROJECT}.{dynamodb_tbl}")
    dynamodb_store = dynamodb_online_store
    # Insert duplicate data
    dynamodb_store._write_batch_non_duplicates(
        table_instance, data + data_duplicate, progress=None
    )
    # Request more items than inserted
    response = table_instance.scan(Limit=20)
    returned_items = response.get("Items", None)
    assert returned_items is not None
    assert len(returned_items) == len(data)
