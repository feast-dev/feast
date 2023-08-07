import logging
from dataclasses import dataclass
from typing import List, Optional

import pytest
from pymilvus import CollectionSchema, DataType, FieldSchema, utility
from pymilvus.client.stub import Milvus

from feast.expediagroup.vectordb.milvus_online_store import (
    MilvusOnlineStore,
    MilvusOnlineStoreConfig,
)
from feast.field import Field
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.repo_config import RepoConfig
from tests.expediagroup.milvus_online_store_creator import MilvusOnlineStoreCreator

logging.basicConfig(level=logging.INFO)

REGISTRY = "s3://test_registry/registry.db"
PROJECT = "test_aws"
PROVIDER = "aws"
TABLE_NAME = "milvus_online_store"
REGION = "us-west-2"
HOST = "localhost"


@dataclass
class MockFeatureView:
    name: str
    schema: Optional[List[Field]]


@pytest.fixture
def repo_config():
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=MilvusOnlineStoreConfig(host=HOST, region=REGION),
        offline_store=FileOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )


@pytest.fixture
def milvus_online_store():
    return MilvusOnlineStore()


@pytest.fixture(scope="class")
def milvus_online_setup():
    # Creating an online store through embedded Milvus for all tests in the class
    online_store_creator = MilvusOnlineStoreCreator("milvus")
    online_store_creator.create_online_store()

    yield online_store_creator

    # Tearing down the Milvus instance after all tests in the class
    online_store_creator.teardown()


class TestMilvusOnlineStore:
    def test_milvus_start_stop(self):
        # this is just an example how to start / stop Milvus. Once a real test is implemented this test can be deleted
        online_store_creator = MilvusOnlineStoreCreator("milvus")
        online_store_creator.create_online_store()

        # access through a stub
        milvus = Milvus(online_store_creator.host, 19530)

        # Create collection demo_collection if it doesn't exist.
        collection_name = "example_collection_"

        ok = milvus.has_collection(collection_name)
        if not ok:
            fields = {
                "fields": [
                    {"name": "key", "type": DataType.INT64, "is_primary": True},
                    {
                        "name": "vector",
                        "type": DataType.FLOAT_VECTOR,
                        "params": {"dim": 32},
                    },
                ],
                "auto_id": True,
            }

            milvus.create_collection(collection_name, fields)

        collection = milvus.describe_collection(collection_name)
        assert collection.get("collection_name") == collection_name
        online_store_creator.teardown()

    def test_milvus_update(self, milvus_online_setup):

        collection_to_delete = "Collection1"
        collection_to_write = "Collection2"
        MilvusOnlineStoreConfig(host=HOST)

        # Creating a common schema for collection
        schema = CollectionSchema(
            fields=[
                FieldSchema(
                    "int64", DataType.INT64, description="int64", is_primary=True
                ),
                FieldSchema(
                    "float_vector", DataType.FLOAT_VECTOR, is_primary=False, dim=128
                ),
            ]
        )

        # Ensuring no collections exist at the start of the test
        utility.drop_collection(collection_to_delete)
        utility.drop_collection(collection_to_write)

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[MockFeatureView(name=collection_to_delete, schema=schema)],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        assert len(utility.list_collections()) == 1

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[MockFeatureView(name=collection_to_delete, schema=None)],
            tables_to_keep=[MockFeatureView(name=collection_to_write, schema=schema)],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        assert utility.has_collection(collection_to_write) is True
        assert utility.has_collection(collection_to_delete) is False
        assert len(utility.list_collections()) == 1
