import logging

import pytest
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)

from feast.expediagroup.vectordb.index_type import IndexType
from feast.expediagroup.vectordb.milvus_online_store import (
    MilvusConnectionManager,
    MilvusOnlineStore,
    MilvusOnlineStoreConfig,
)
from feast.expediagroup.vectordb.vector_feature_view import VectorFeatureView
from feast.field import Field
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.repo_config import RepoConfig
from feast.types import Array, Float32, Int64
from tests.expediagroup.milvus_online_store_creator import MilvusOnlineStoreCreator

logging.basicConfig(level=logging.INFO)

REGISTRY = "s3://test_registry/registry.db"
PROJECT = "test_aws"
PROVIDER = "aws"
TABLE_NAME = "milvus_online_store"
REGION = "us-west-2"
HOST = "localhost"
PORT = 19530
ALIAS = "default"
SOURCE = FileSource(path="some path")
VECTOR_FIELD = "feature1"
DIMENSIONS = 10
INDEX_ALGO = IndexType.flat


@pytest.fixture(scope="session")
def repo_config():
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=MilvusOnlineStoreConfig(
            alias=ALIAS, host=HOST, username="abc", password="cde"
        ),
        offline_store=FileOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )


@pytest.fixture(scope="session")
def embedded_milvus():
    # Creating an online store through embedded Milvus for all tests in the class
    online_store_creator = MilvusOnlineStoreCreator("milvus")
    online_store_creator.create_online_store()

    yield online_store_creator

    # Tearing down the Milvus instance after all tests in the class
    online_store_creator.teardown()


class TestMilvusConnectionManager:
    def test_connection_manager(self, repo_config, caplog, mocker):

        mocker.patch("pymilvus.connections.connect")
        with MilvusConnectionManager(repo_config.online_store):
            assert (
                f"Connecting to Milvus with alias {repo_config.online_store.alias} and host {repo_config.online_store.host} and port {repo_config.online_store.port}."
                in caplog.text
            )

        connections.connect.assert_called_once_with(
            alias=repo_config.online_store.alias,
            host=repo_config.online_store.host,
            port=repo_config.online_store.port,
            user=repo_config.online_store.username,
            password=repo_config.online_store.password,
            use_secure=True,
        )

    def test_context_manager_exit(self, repo_config, caplog, mocker):
        # Create a mock for connections.disconnect
        mock_disconnect = mocker.patch("pymilvus.connections.disconnect")

        # Create a mock logger to capture log calls
        mock_logger = mocker.patch(
            "feast.expediagroup.vectordb.milvus_online_store.logger", autospec=True
        )

        with MilvusConnectionManager(repo_config.online_store):
            print("Doing something")

        # Assert that connections.disconnect was called with the expected argument
        mock_disconnect.assert_called_once_with(repo_config.online_store.alias)

        with pytest.raises(Exception):
            with MilvusConnectionManager(repo_config.online_store):
                raise Exception("Test Exception")
        mock_logger.error.assert_called_once()


class TestMilvusOnlineStore:

    collection_to_write = "Collection2"
    collection_to_delete = "Collection1"
    unavailable_collection = "abc"

    @pytest.fixture(autouse=True)
    def setup_method(self, repo_config):
        # Ensuring that the collections created are dropped before the tests are run
        with MilvusConnectionManager(repo_config.online_store):
            # Dropping collections if they exist
            if utility.has_collection(self.collection_to_delete):
                utility.drop_collection(self.collection_to_delete)
            if utility.has_collection(self.collection_to_write):
                utility.drop_collection(self.collection_to_write)
            if utility.has_collection(self.unavailable_collection):
                utility.drop_collection(self.unavailable_collection)
            # Closing the temporary collection to do this

        yield

    def test_milvus_update_add_collection(self, repo_config, caplog, embedded_milvus):
        feast_schema = [
            Field(
                name="feature2",
                dtype=Int64,
                tags={"is_primary": "True", "description": "int64"},
            ),
            Field(
                name="feature1",
                dtype=Array(Float32),
                tags={"is_primary": "False", "description": "float32"},
            ),
        ]

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                VectorFeatureView(
                    name=self.collection_to_write,
                    schema=feast_schema,
                    source=SOURCE,
                    vector_field=VECTOR_FIELD,
                    dimensions=DIMENSIONS,
                    index_algorithm=INDEX_ALGO,
                )
            ],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        # Milvus schema to be checked if the schema from Feast to Milvus was converted accurately
        schema1 = CollectionSchema(
            description="",
            fields=[
                FieldSchema(
                    "feature2", DataType.INT64, description="int64", is_primary=True
                ),
                FieldSchema(
                    "feature1",
                    DataType.FLOAT_VECTOR,
                    description="float32",
                    is_primary=False,
                    dim=10,
                ),
            ],
        )

        schema2 = CollectionSchema(
            description="",
            fields=[
                FieldSchema(
                    "feature1",
                    DataType.FLOAT_VECTOR,
                    description="float32",
                    is_primary=False,
                    dim=10,
                ),
                FieldSchema(
                    "feature2", DataType.INT64, description="int64", is_primary=True
                ),
            ],
        )

        # Here we want to open and check whether the collection was added and then close the connection.
        with MilvusConnectionManager(repo_config.online_store):
            assert utility.has_collection(self.collection_to_write) is True
            assert (
                Collection(self.collection_to_write).schema == schema1
                or Collection(self.collection_to_write).schema == schema2
            )

    def test_milvus_update_add_existing_collection(
        self, repo_config, caplog, embedded_milvus
    ):
        # Creating a common schema for collection
        feast_schema = [
            Field(
                name="feature1",
                dtype=Array(Float32),
                tags={
                    "is_primary": "False",
                    "description": "float32",
                    "dimensions": "128",
                },
            ),
            Field(
                name="feature2",
                dtype=Int64,
                tags={"is_primary": "True", "description": "int64"},
            ),
        ]

        # Creating a common schema for collection to directly add to Milvus
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

        # Here we want to open and add a collection using pymilvus directly and close the connection.
        with MilvusConnectionManager(repo_config.online_store):
            Collection(name=self.collection_to_write, schema=schema)
            assert utility.has_collection(self.collection_to_write) is True
            assert len(utility.list_collections()) == 1

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                VectorFeatureView(
                    name=self.collection_to_write,
                    schema=feast_schema,
                    source=SOURCE,
                    vector_field=VECTOR_FIELD,
                    dimensions=DIMENSIONS,
                    index_algorithm=INDEX_ALGO,
                )
            ],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        # Here we want to open and add a collection using pymilvus directly and close the connection, we need to check if the collection count remains 1 and exists.
        with MilvusConnectionManager(repo_config.online_store):
            assert utility.has_collection(self.collection_to_write) is True
            assert len(utility.list_collections()) == 1

    def test_milvus_update_delete_collection(
        self, repo_config, caplog, embedded_milvus
    ):
        # Creating a common schema for collection which is compatible with FEAST
        feast_schema = [
            Field(
                name="feature1",
                dtype=Array(Float32),
                tags={
                    "is_primary": "False",
                    "description": "float32",
                    "dimensions": "128",
                },
            ),
            Field(
                name="feature2",
                dtype=Int64,
                tags={"is_primary": "True", "description": "int64"},
            ),
        ]

        # Creating a common schema for collection to directly add to Milvus
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

        # Here we want to open and add a collection using pymilvus directly and close the connection
        with MilvusConnectionManager(repo_config.online_store):
            Collection(name=self.collection_to_write, schema=schema)
            assert utility.has_collection(self.collection_to_write) is True

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[
                VectorFeatureView(
                    name=self.collection_to_write,
                    schema=feast_schema,
                    source=SOURCE,
                    vector_field=VECTOR_FIELD,
                    dimensions=DIMENSIONS,
                    index_algorithm=INDEX_ALGO,
                )
            ],
            tables_to_keep=[],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        # Opening and closing the connection and checking if the collection is actually deleted.
        with MilvusConnectionManager(repo_config.online_store):
            assert utility.has_collection(self.collection_to_write) is False

    def test_milvus_update_delete_unavailable_collection(
        self, repo_config, caplog, embedded_milvus
    ):
        feast_schema = [
            Field(
                name="feature1",
                dtype=Array(Float32),
                tags={
                    "is_primary": "False",
                    "description": "float32",
                    "dimensions": "128",
                },
            ),
            Field(
                name="feature2",
                dtype=Int64,
                tags={"is_primary": "True", "description": "int64"},
            ),
        ]

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[
                VectorFeatureView(
                    name=self.unavailable_collection,
                    schema=feast_schema,
                    source=SOURCE,
                    vector_field=VECTOR_FIELD,
                    dimensions=DIMENSIONS,
                    index_algorithm=INDEX_ALGO,
                )
            ],
            tables_to_keep=[],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        with MilvusConnectionManager(repo_config.online_store):
            assert len(utility.list_collections()) == 0
