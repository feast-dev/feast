import logging
from typing import List, Optional
from dataclasses import dataclass
import pytest
from pymilvus import (
    Collection, FieldSchema, CollectionSchema, DataType, utility
)
from tests.expediagroup.milvus_online_store_creator import MilvusOnlineStoreCreator
from feast.repo_config import RepoConfig
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.expediagroup.vectordb.milvus_online_store import (
    MilvusOnlineStoreConfig, MilvusOnlineStore
)
from feast.field import Field

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
    def test_milvus_update(self, milvus_online_setup):

        collection_to_delete = "Collection1"
        collection_to_write = "Collection2"
        MilvusOnlineStoreConfig(host=HOST)
        
        # Creating a common schema for collection
        schema = CollectionSchema(fields=[FieldSchema("int64", DataType.INT64, description="int64", is_primary=True), FieldSchema("float_vector", DataType.FLOAT_VECTOR, is_primary=False, dim=128), ])

        # Ensuring no collections exist at the start of the test
        utility.drop_collection(collection_to_delete)
        utility.drop_collection(collection_to_write)

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[MockFeatureView(name=collection_to_delete, schema=schema)],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None
        )

        assert len(utility.list_collections()) == 1

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[MockFeatureView(name=collection_to_delete, schema=None)],
            tables_to_keep=[MockFeatureView(name=collection_to_write, schema=schema)],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None
        )

        logging.info(utility.list_collections())
        assert utility.has_collection(collection_to_write) is True
        assert utility.has_collection(collection_to_delete) is False
        assert len(utility.list_collections()) == 1


def connect_from_connections():

    online_store_creator = MilvusOnlineStoreCreator("milvus")
    online_store_creator.create_online_store()

    # create dummy table to delete
    db_table_delete_name = "Collection2"
    schema2 = CollectionSchema(fields=[FieldSchema("int64", DataType.INT64, description="int64", is_primary=True), FieldSchema("float_vector", DataType.FLOAT_VECTOR, is_primary=False, dim=128), ])

    MilvusOnlineStoreConfig(host=HOST)

    logging.info(utility.list_collections())
    
    MilvusOnlineStore().update(
        config=repo_config,
        tables_to_delete=[],
        tables_to_keep=[MockFeatureView(name="Collection1", schema=schema2)],
        entities_to_delete=None,
        entities_to_keep=None,
        partial=None
    )

    logging.info(utility.list_collections())

    # # logging.info(utility.has_collection("new_collection"))  # Output: False
    # logging.info(utility.list_collections())
    # schema = CollectionSchema(fields=[FieldSchema("int64", DataType.INT64, description="int64", is_primary=True), FieldSchema("float_vector", DataType.FLOAT_VECTOR, is_primary=False, dim=128), ])
    # Collection(name="old_collection", schema=schema)
    # logging.info(utility.list_collections())
    # utility.rename_collection("old_collection", "new_collection")  # Output: True
    
    # utility.drop_collection("new_collection")
    # logging.info(utility.list_collections())
    # logging.info(utility.has_collection("new_collection"))

    MilvusOnlineStore().update(
        config=repo_config,
        tables_to_delete=[MockFeatureView(name="ew", schema=None)],
        tables_to_keep=[MockFeatureView(name="Collection1", schema=schema2)],
        entities_to_delete=None,
        entities_to_keep=None,
        partial=None
    )

    schema3 = CollectionSchema(fields=[FieldSchema("int64", DataType.INT64, description="int is new", is_primary=False), FieldSchema("varchar_vector", DataType.VARCHAR, is_primary=True, dim=128)])

    MilvusOnlineStore().update(
        config=repo_config,
        tables_to_delete=[],
        tables_to_keep=[MockFeatureView(name="Collection1", schema=schema3)],
        entities_to_delete=None,
        entities_to_keep=None,
        partial=None
    )

    online_store_creator.teardown()


connect_from_connections()
