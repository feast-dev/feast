import json
import logging
import random
from datetime import datetime

import pytest
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)

from feast import FeatureView
from feast.entity import Entity
from feast.expediagroup.vectordb.eg_milvus_online_store import (
    EGMilvusConnectionManager,
    EGMilvusOnlineStore,
    EGMilvusOnlineStoreConfig,
)
from feast.field import Field
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Array, Bytes, Float32, Int64, String
from tests.expediagroup.eg_milvus_online_store_creator import EGMilvusOnlineStoreCreator

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
INDEX_ALGO = "FLAT"


@pytest.fixture(scope="session")
def repo_config(embedded_milvus):
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=EGMilvusOnlineStoreConfig(
            alias=embedded_milvus["alias"],
            host=embedded_milvus["host"],
            port=embedded_milvus["port"],
            username=embedded_milvus["username"],
            password=embedded_milvus["password"],
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )


@pytest.fixture(scope="session")
def embedded_milvus():
    # Creating an online store through embedded Milvus for all tests in the class
    online_store_creator = EGMilvusOnlineStoreCreator("milvus")
    online_store_config = online_store_creator.create_online_store()

    yield online_store_config

    # Tearing down the Milvus instance after all tests in the class
    online_store_creator.teardown()


class TestMilvusConnectionManager:
    def test_connection_manager(self, repo_config, caplog, mocker):
        mocker.patch("pymilvus.connections.connect")
        with EGMilvusConnectionManager(repo_config.online_store):
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
            "feast.expediagroup.vectordb.eg_milvus_online_store.logger", autospec=True
        )

        with EGMilvusConnectionManager(repo_config.online_store):
            print("Doing something")

        # Assert that connections.disconnect was called with the expected argument
        mock_disconnect.assert_called_once_with(repo_config.online_store.alias)

        with pytest.raises(Exception):
            with EGMilvusConnectionManager(repo_config.online_store):
                raise Exception("Test Exception")
        mock_logger.error.assert_called_once()


class TestMilvusOnlineStore:
    collection_to_write = "Collection2"
    collection_to_delete = "Collection1"
    unavailable_collection = "abc"

    @pytest.fixture(autouse=True)
    def setup_method(self, repo_config):
        # Ensuring that the collections created are dropped before the tests are run
        with EGMilvusConnectionManager(repo_config.online_store):
            # Dropping collections if they exist
            if utility.has_collection(self.collection_to_delete):
                utility.drop_collection(self.collection_to_delete)
            if utility.has_collection(self.collection_to_write):
                utility.drop_collection(self.collection_to_write)
            if utility.has_collection(self.unavailable_collection):
                utility.drop_collection(self.unavailable_collection)
            # Closing the temporary collection to do this

        yield

    def create_n_customer_test_samples_milvus_online_read(self, n=10):
        # Utility method to create sample data
        return [
            (
                EntityKeyProto(
                    join_keys=["film_id"],
                    entity_values=[ValueProto(int64_val=i)],
                ),
                {
                    "films": ValueProto(
                        float_list_val=FloatList(
                            val=[random.random() for _ in range(2)]
                        )
                    ),
                    "film_date": ValueProto(int64_val=n),
                    "film_id": ValueProto(int64_val=n),
                },
                datetime.utcnow(),
                None,
            )
            for i in range(n)
        ]

    def _create_n_customer_test_samples_milvus(self, n=10):
        # Utility method to create sample data
        return [
            (
                EntityKeyProto(
                    join_keys=["customer"],
                    entity_values=[ValueProto(string_val=str(i))],
                ),
                {
                    "avg_orders_day": ValueProto(
                        float_list_val=FloatList(val=[1.0, 2.1, 3.3, 4.0, 5.0])
                    ),
                    "name": ValueProto(string_val="John"),
                    "age": ValueProto(int64_val=3),
                },
                datetime.utcnow(),
                None,
            )
            for i in range(n)
        ]

    index_param_list = [
        {
            "metric_type": "L2",
            "index_type": "FLAT",
            "params": {},
        },
        {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 64},
        },
        {
            "metric_type": "COSINE",
            "index_type": "IVF_SQ8",
            "params": {"nlist": 64},
        },
        {
            "metric_type": "IP",
            "index_type": "IVF_PQ",
            "params": {"nlist": 64, "m": 2, "nbits": 8},
        },
        {
            "metric_type": "L2",
            "index_type": "HNSW",
            "params": {"M": 32, "efConstruction": 256},
        },
        {
            "metric_type": "HAMMING",
            "index_type": "BIN_FLAT",
            "params": {},
        },
        {
            "metric_type": "JACCARD",
            "index_type": "BIN_IVF_FLAT",
            "params": {"nlist": 64},
        },
    ]

    @pytest.mark.parametrize("index_params", index_param_list)
    def test_milvus_update_add_collection(self, repo_config, caplog, index_params):
        dimensions = 16
        vector_type = Float32
        if "BIN" in index_params["index_type"]:
            vector_type = Bytes

        vector_tags = {
            "is_primary": "False",
            "description": vector_type.name,
            "dimensions": str(dimensions),
            "index_type": index_params["index_type"],
        }

        if "metric_type" in index_params and index_params["metric_type"]:
            vector_tags["metric_type"] = index_params["metric_type"]

        if "params" in index_params and index_params["params"]:
            vector_tags["index_params"] = json.dumps(index_params["params"])

        entity = Entity(name="feature2")
        feast_schema = [
            Field(
                name="feature2",
                dtype=Int64,
                tags={"description": "int64"},
            ),
            Field(
                name="feature1",
                dtype=Array(vector_type),
                tags=vector_tags,
            ),
        ]

        EGMilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                FeatureView(
                    name=self.collection_to_write,
                    entities=[entity],
                    schema=feast_schema,
                    source=SOURCE,
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
                    (
                        DataType.FLOAT_VECTOR
                        if vector_type == Float32
                        else DataType.BINARY_VECTOR
                    ),
                    description=vector_type.name,
                    is_primary=False,
                    dim=dimensions,
                ),
            ],
        )

        schema2 = CollectionSchema(
            description="",
            fields=[
                FieldSchema(
                    "feature1",
                    (
                        DataType.FLOAT_VECTOR
                        if vector_type == Float32
                        else DataType.BINARY_VECTOR
                    ),
                    description=vector_type.name,
                    is_primary=False,
                    dim=dimensions,
                ),
                FieldSchema(
                    "feature2", DataType.INT64, description="int64", is_primary=True
                ),
            ],
        )

        # Here we want to open and check whether the collection was added and then close the connection.
        with EGMilvusConnectionManager(repo_config.online_store):
            assert utility.has_collection(self.collection_to_write)
            assert (
                Collection(self.collection_to_write).schema == schema1
                or Collection(self.collection_to_write).schema == schema2
            )
            indexes = Collection(self.collection_to_write).indexes
            assert len(indexes) == 1
            assert indexes[0].params == index_params

    def test_milvus_update_add_existing_collection(self, repo_config, caplog):
        entity = Entity(name="feature2")
        # Creating a common schema for collection
        feast_schema = [
            Field(
                name="feature1",
                dtype=Array(Float32),
                tags={
                    "description": "float32",
                    "dimensions": "128",
                    "index_type": "HNSW",
                    "index_params": '{ "M": 32, "efConstruction": 256}',
                },
            ),
            Field(
                name="feature2",
                dtype=Int64,
                tags={"description": "int64"},
            ),
        ]

        self._create_collection_in_milvus(self.collection_to_write, repo_config)

        EGMilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                FeatureView(
                    name=self.collection_to_write,
                    entities=[entity],
                    schema=feast_schema,
                    source=SOURCE,
                )
            ],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        # Here we want to open and add a collection using pymilvus directly and close the connection, we need to check if the collection count remains 1 and exists.
        with EGMilvusConnectionManager(repo_config.online_store):
            assert utility.has_collection(self.collection_to_write) is True
            assert len(utility.list_collections()) == 1

    def test_milvus_update_delete_collection(self, repo_config, caplog):
        entity = Entity(name="feature2")
        # Creating a common schema for collection which is compatible with FEAST
        feast_schema = [
            Field(
                name="feature1",
                dtype=Array(Float32),
                tags={
                    "description": "float32",
                    "dimensions": "128",
                    "index_type": "HNSW",
                    "index_params": '{ "M": 32, "efConstruction": 256}',
                },
            ),
            Field(
                name="feature2",
                dtype=Int64,
                tags={"description": "int64"},
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
        with EGMilvusConnectionManager(repo_config.online_store):
            Collection(name=self.collection_to_write, schema=schema)
            assert utility.has_collection(self.collection_to_write) is True

        EGMilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[
                FeatureView(
                    name=self.collection_to_write,
                    entities=[entity],
                    schema=feast_schema,
                    source=SOURCE,
                )
            ],
            tables_to_keep=[],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        # Opening and closing the connection and checking if the collection is actually deleted.
        with EGMilvusConnectionManager(repo_config.online_store):
            assert utility.has_collection(self.collection_to_write) is False

    def test_milvus_update_delete_unavailable_collection(self, repo_config, caplog):
        entity = Entity(name="feature2")
        feast_schema = [
            Field(
                name="feature1",
                dtype=Array(Float32),
                tags={
                    "description": "float32",
                    "dimensions": "128",
                    "index_type": "HNSW",
                    "index_params": '{ "M": 32, "efConstruction": 256}',
                },
            ),
            Field(
                name="feature2",
                dtype=Int64,
                tags={"description": "int64"},
            ),
        ]

        EGMilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[
                FeatureView(
                    name=self.unavailable_collection,
                    entities=[entity],
                    schema=feast_schema,
                    source=SOURCE,
                )
            ],
            tables_to_keep=[],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        with EGMilvusConnectionManager(repo_config.online_store):
            assert len(utility.list_collections()) == 0

    def test_milvus_online_write_batch(self, repo_config, caplog):
        entity = Entity(name="name")
        feature_view = FeatureView(
            name=self.collection_to_write,
            source=SOURCE,
            entities=[entity],
            schema=[
                Field(
                    name="name",
                    dtype=String,
                ),
                Field(
                    name="age",
                    dtype=Int64,
                ),
                Field(
                    name="avg_orders_day",
                    dtype=Array(Float32),
                    tags={
                        "dimensions": "5",
                        "index_type": "HNSW",
                        "index_params": '{ "M": 32, "efConstruction": 256}',
                    },
                ),
            ],
        )

        total_rows_to_write = 100
        data = self._create_n_customer_test_samples_milvus(n=total_rows_to_write)
        EGMilvusOnlineStore().online_write_batch(
            config=repo_config, table=feature_view, data=data, progress=None
        )

        with EGMilvusConnectionManager(repo_config.online_store):
            collection = Collection(name=self.collection_to_write)
            progress = utility.index_building_progress(collection_name=collection.name)
            assert progress["total_rows"] == total_rows_to_write

    def test_milvus_teardown_with_empty_collection(self, repo_config, caplog):
        self._create_collection_in_milvus(self.collection_to_write, repo_config)

        feature_view = FeatureView(
            name=self.collection_to_write,
            source=SOURCE,
        )

        milvus_online_store = EGMilvusOnlineStore()
        milvus_online_store.teardown(
            config=repo_config, tables=[feature_view], entities=[]
        )

        with EGMilvusConnectionManager(repo_config.online_store):
            assert not utility.has_collection(self.collection_to_write)

    def test_milvus_teardown_with_non_empty_collection(self, repo_config, caplog):
        self._create_collection_in_milvus(self.collection_to_write, repo_config)

        feature_view = FeatureView(
            name=self.collection_to_write,
            source=SOURCE,
        )

        total_rows_to_write = 100
        data = self._create_n_customer_test_samples_milvus(n=total_rows_to_write)
        self._write_data_to_milvus(self.collection_to_write, data, repo_config)

        milvus_online_store = EGMilvusOnlineStore()
        milvus_online_store.teardown(
            config=repo_config, tables=[feature_view], entities=[]
        )

        with EGMilvusConnectionManager(repo_config.online_store):
            assert not utility.has_collection(self.collection_to_write)

    def test_milvus_teardown_with_collection_not_existing(self, repo_config, caplog):
        feature_view = FeatureView(
            name=self.collection_to_write,
            source=SOURCE,
        )

        milvus_online_store = EGMilvusOnlineStore()
        milvus_online_store.teardown(
            config=repo_config, tables=[feature_view], entities=[]
        )

        with EGMilvusConnectionManager(repo_config.online_store):
            assert not utility.has_collection(self.collection_to_write)

    def _create_collection_in_milvus(self, collection_name, repo_config):
        milvus_schema = CollectionSchema(
            fields=[
                FieldSchema(
                    "avg_orders_day", DataType.FLOAT_VECTOR, is_primary=False, dim=5
                ),
                FieldSchema(
                    "name",
                    DataType.VARCHAR,
                    description="string",
                    is_primary=True,
                    max_length=256,
                ),
                FieldSchema("age", DataType.INT64, is_primary=False),
            ]
        )

        with EGMilvusConnectionManager(repo_config.online_store):
            # Create a collection
            collection = Collection(name=self.collection_to_write, schema=milvus_schema)
            # Drop all indexes if any exists
            collection.drop_index()
            # Create a new index
            index_params = {
                "metric_type": "L2",
                "index_type": "IVF_FLAT",
                "params": {"nlist": 1024},
            }
            collection.create_index("avg_orders_day", index_params)

    def _write_data_to_milvus(self, collection_name, data, repo_config):
        with EGMilvusConnectionManager(repo_config.online_store):
            collection_to_load_data = Collection(collection_name)
            rows = EGMilvusOnlineStore()._format_data_for_milvus(
                data, collection_to_load_data
            )
            collection_to_load_data.insert(rows)
            collection_to_load_data.flush()
            collection_to_load_data.load()

    def test_milvus_online_read(self, repo_config, caplog):
        # Generating data to directly load into Milvus
        data_to_load = [
            [i for i in range(10)],
            [i + 2000 for i in range(10)],
            [[random.random() for _ in range(2)] for _ in range(10)],
        ]

        # Creating a common schema for collection to directly add to Milvus
        schema = CollectionSchema(
            [
                FieldSchema("film_id", DataType.INT64, is_primary=True),
                FieldSchema("film_date", DataType.INT64),
                FieldSchema("films", dtype=DataType.FLOAT_VECTOR, dim=2),
            ]
        )

        with EGMilvusConnectionManager(repo_config.online_store):
            # Create a collection
            collection = Collection(name=self.collection_to_write, schema=schema)
            # Drop all indexes if any exists
            collection.drop_index()
            # Create a new index
            index_param = {"index_type": "FLAT", "metric_type": "L2", "params": {}}
            collection.create_index("films", index_param)
            collection.insert(data_to_load)
            collection.load()

        feast_schema = [
            Field(
                name="film_id",
                dtype=Int64,
                tags={"description": "int64"},
            ),
            Field(
                name="film_date",
                dtype=Int64,
                tags={
                    "description": "Int64",
                },
            ),
            Field(
                name="films",
                dtype=Array(Float32),
                tags={
                    "description": "float32",
                    "dimensions": "2",
                    "index_type": "HNSW",
                    "index_params": '{ "M": 32, "efConstruction": 256}',
                },
            ),
        ]

        film_data_with_entity_keys = (
            self.create_n_customer_test_samples_milvus_online_read()
        )
        entity_keys, features, *rest = zip(*film_data_with_entity_keys)
        feature_list = ["film_date", "films", "film_id"]

        result = EGMilvusOnlineStore().online_read(
            config=repo_config,
            table=FeatureView(
                name=self.collection_to_write, schema=feast_schema, source=SOURCE
            ),
            entity_keys=entity_keys,
            requested_features=feature_list,
        )

        assert result is not None
        assert len(result) == 10
        assert result[0][1]["film_id"].int64_val == 0
        assert result[0][1]["film_date"].int64_val == 2000
        assert result[9][1]["film_id"].int64_val == 9
        assert result[9][1]["film_date"].int64_val == 2009
