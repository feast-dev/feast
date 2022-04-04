from dataclasses import dataclass

import pytest
from moto import mock_dynamodb2

from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.online_stores.dynamodb import (
    DynamoDBOnlineStore,
    DynamoDBOnlineStoreConfig,
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


@mock_dynamodb2
@pytest.mark.parametrize("n_samples", [5, 50, 100])
def test_online_read(repo_config, n_samples):
    """Test DynamoDBOnlineStore online_read method."""
    _create_test_table(PROJECT, f"{TABLE_NAME}_{n_samples}", REGION)
    data = _create_n_customer_test_samples(n=n_samples)
    _insert_data_test_table(data, PROJECT, f"{TABLE_NAME}_{n_samples}", REGION)

    entity_keys, features = zip(*data)
    dynamodb_store = DynamoDBOnlineStore()
    returned_items = dynamodb_store.online_read(
        config=repo_config,
        table=MockFeatureView(name=f"{TABLE_NAME}_{n_samples}"),
        entity_keys=entity_keys,
    )
    assert len(returned_items) == len(data)
    assert [item[1] for item in returned_items] == list(features)
