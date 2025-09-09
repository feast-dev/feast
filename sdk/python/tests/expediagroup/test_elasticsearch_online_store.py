import json
import logging
import random
from datetime import datetime

import pytest

from feast import FeatureView
from feast.entity import Entity
from feast.expediagroup.vectordb.elasticsearch_online_store import (
    ElasticsearchConnectionManager,
    ElasticsearchOnlineStore,
    ElasticsearchOnlineStoreConfig,
)
from feast.field import Field
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import BytesList, FloatList
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import (
    Array,
    Bool,
    Bytes,
    Float32,
    Float64,
    Int32,
    Int64,
    String,
    UnixTimestamp,
)
from tests.expediagroup.elasticsearch_online_store_creator import (
    ElasticsearchOnlineStoreCreator,
)

logging.basicConfig(level=logging.INFO)

REGISTRY = "s3://test_registry/registry.db"
PROJECT = "test_aws"
PROVIDER = "aws"
REGION = "us-west-2"
SOURCE = FileSource(path="some path")

index_param_list = [
    {"index_type": "HNSW", "index_params": {"m": 16, "ef_construction": 100}},
    {"index_type": "HNSW"},
]


@pytest.fixture(scope="session", params=[False, True], ids=["normal", "lazy"])
def repo_config(request, embedded_elasticsearch):
    """Parametrized repo config with lazy_table_creation=False and True"""
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=ElasticsearchOnlineStoreConfig(
            endpoint=f"http://{embedded_elasticsearch['host']}:{embedded_elasticsearch['port']}",
            username=embedded_elasticsearch["username"],
            password=embedded_elasticsearch["password"],
            lazy_table_creation=request.param,  # This will be False, then True
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )


@pytest.fixture(scope="session")
def embedded_elasticsearch():
    online_store_creator = ElasticsearchOnlineStoreCreator(PROJECT)
    online_store_config = online_store_creator.create_online_store()

    yield online_store_config

    online_store_creator.teardown()


class TestElasticsearchOnlineStore:
    @pytest.fixture(autouse=True)
    def setup_method(self, repo_config):
        # Generate unique index names based on lazy_table_creation setting
        lazy_suffix = "_lazy" if repo_config.online_store.lazy_table_creation else ""
        self.index_to_write = f"index_write{lazy_suffix}"
        self.index_to_delete = f"index_delete{lazy_suffix}"
        self.index_to_read = f"index_read{lazy_suffix}"
        self.unavailable_index = f"abc{lazy_suffix}"

        # Ensuring that the indexes created are dropped before the tests are run
        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            # Dropping indexes if they exist
            for index in [
                self.index_to_delete,
                self.index_to_write,
                self.index_to_read,
                self.unavailable_index,
            ]:
                if es.indices.exists(index=index):
                    es.indices.delete(index=index)

        yield

    @pytest.mark.parametrize("index_params", index_param_list)
    def test_elasticsearch_update_add_index(self, repo_config, caplog, index_params):
        dimensions = "16"
        vector_type = Float32
        vector_tags = {
            "is_primary": "False",
            "description": vector_type.name,
            "dimensions": dimensions,
            "index_type": index_params["index_type"],
        }
        if "index_params" in index_params:
            vector_tags["index_params"] = json.dumps(
                index_params.get("index_params", {})
            )
        entity = Entity(name="feature2")
        feast_schema = [
            Field(
                name="feature1",
                dtype=Array(vector_type),
                tags=vector_tags,
            ),
            Field(
                name="feature2",
                dtype=String,
            ),
            Field(name="feature3", dtype=String),
            Field(name="feature4", dtype=Bytes),
            Field(name="feature5", dtype=Int32),
            Field(name="feature6", dtype=Int64),
            Field(name="feature7", dtype=Float32),
            Field(name="feature8", dtype=Float64),
            Field(name="feature9", dtype=Bool),
            Field(name="feature10", dtype=UnixTimestamp),
        ]
        ElasticsearchOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                FeatureView(
                    name=self.index_to_write,
                    entities=[entity],
                    schema=feast_schema,
                    source=SOURCE,
                )
            ],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )

        mapping = {
            "properties": {
                "feature1": {
                    "type": "dense_vector",
                    "dims": 16,
                    "index": True,
                    "similarity": "l2_norm",
                },
                "feature2": {"type": "keyword"},
                "feature3": {"type": "text"},
                "feature4": {"type": "binary"},
                "feature5": {"type": "integer"},
                "feature6": {"type": "long"},
                "feature7": {"type": "float"},
                "feature8": {"type": "double"},
                "feature9": {"type": "boolean"},
                "feature10": {"type": "date_nanos"},
            }
        }
        if "index_params" in index_params:
            mapping["properties"]["feature1"]["index_options"] = {
                "type": index_params["index_type"].lower(),
                **index_params["index_params"],
            }

        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            created_index = es.indices.get(index=self.index_to_write)
            assert created_index.body[self.index_to_write]["mappings"] == mapping

    def test_elasticsearch_update_add_existing_index(self, repo_config, caplog):
        entity = Entity(name="id")
        feast_schema = [
            Field(
                name="vector",
                dtype=Array(Float32),
                tags={
                    "description": "float32",
                    "dimensions": "10",
                    "index_type": "HNSW",
                },
            ),
            Field(
                name="id",
                dtype=String,
            ),
        ]
        self._create_index_in_es(self.index_to_write, repo_config)
        ElasticsearchOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                FeatureView(
                    name=self.index_to_write,
                    entities=[entity],
                    schema=feast_schema,
                    source=SOURCE,
                )
            ],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )

        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            assert es.indices.exists(index=self.index_to_write).body is True

    def test_elasticsearch_update_delete_index(self, repo_config, caplog):
        entity = Entity(name="id")
        feast_schema = [
            Field(
                name="vector",
                dtype=Array(Float32),
                tags={
                    "description": "float32",
                    "dimensions": "10",
                    "index_type": "HNSW",
                },
            ),
            Field(
                name="id",
                dtype=String,
            ),
        ]
        self._create_index_in_es(self.index_to_delete, repo_config)

        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            assert es.indices.exists(index=self.index_to_delete).body is True

        ElasticsearchOnlineStore().update(
            config=repo_config,
            tables_to_delete=[
                FeatureView(
                    name=self.index_to_delete,
                    entities=[entity],
                    schema=feast_schema,
                    source=SOURCE,
                )
            ],
            tables_to_keep=[],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )

        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            assert es.indices.exists(index=self.index_to_delete).body is False

    def test_elasticsearch_update_delete_unavailable_index(self, repo_config, caplog):
        entity = Entity(name="id")
        feast_schema = [
            Field(
                name="vector",
                dtype=Array(Float32),
                tags={
                    "description": "float32",
                    "dimensions": "10",
                    "index_type": "HNSW",
                },
            ),
            Field(
                name="id",
                dtype=String,
            ),
        ]

        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            assert es.indices.exists(index=self.index_to_delete).body is False

        ElasticsearchOnlineStore().update(
            config=repo_config,
            tables_to_delete=[
                FeatureView(
                    name=self.index_to_delete,
                    entities=[entity],
                    schema=feast_schema,
                    source=SOURCE,
                )
            ],
            tables_to_keep=[],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )

        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            assert es.indices.exists(index=self.index_to_delete).body is False

    def test_elasticsearch_online_write_batch(self, repo_config, caplog):
        total_rows_to_write = 10
        (
            feature_view,
            data,
        ) = self._create_n_customer_test_samples_elasticsearch_online_read(
            name=self.index_to_write,
            n=total_rows_to_write,
        )
        ElasticsearchOnlineStore().online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )

        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            es.indices.refresh(index=self.index_to_write)
            res = es.cat.count(index=self.index_to_write, params={"format": "json"})
            assert res[0]["count"] == f"{total_rows_to_write}"
            doc = es.get(index=self.index_to_write, id="0")["_source"]
            for feature in feature_view.schema:
                assert feature.name in doc

    def test_elasticsearch_online_read(self, repo_config, caplog):
        n = 10
        (
            feature_view,
            data,
        ) = self._create_n_customer_test_samples_elasticsearch_online_read(
            name=self.index_to_read, n=n
        )
        ids = [
            EntityKeyProto(
                join_keys=["id"], entity_values=[ValueProto(string_val=str(i))]
            )
            for i in range(n)
        ]
        store = ElasticsearchOnlineStore()
        store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )

        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            es.indices.refresh(index=self.index_to_read)

        result = store.online_read(
            config=repo_config,
            table=feature_view,
            entity_keys=ids,
        )

        assert result is not None
        assert len(result) == n
        for dt, doc in result:
            assert doc is not None
            assert len(doc) == len(feature_view.schema)
            for field in feature_view.schema:
                assert field.name in doc

    def test_elasticsearch_online_read_with_requested_features(
        self, repo_config, caplog
    ):
        n = 10
        requested_features = ["int", "vector", "id"]
        (
            feature_view,
            data,
        ) = self._create_n_customer_test_samples_elasticsearch_online_read(
            name=self.index_to_read, n=n
        )
        ids = [
            EntityKeyProto(
                join_keys=["id"], entity_values=[ValueProto(string_val=str(i))]
            )
            for i in range(n)
        ]
        store = ElasticsearchOnlineStore()
        store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )

        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            es.indices.refresh(index=self.index_to_read)

        result = store.online_read(
            config=repo_config,
            table=feature_view,
            entity_keys=ids,
            requested_features=requested_features,
        )

        assert result is not None
        assert len(result) == n
        for dt, doc in result:
            assert doc is not None
            assert len(doc) == 3
            for field in requested_features:
                assert field in doc

    def _create_index_in_es(self, index_name, repo_config):
        with ElasticsearchConnectionManager(repo_config.online_store) as es:
            mapping = {
                "properties": {
                    "vector": {
                        "type": "dense_vector",
                        "dims": 10,
                        "index": True,
                        "similarity": "l2_norm",
                    },
                    "id": {"type": "keyword"},
                }
            }
            es.indices.create(index=index_name, mappings=mapping)

    def _create_n_customer_test_samples_elasticsearch_online_read(self, name, n=10):
        fv = FeatureView(
            name=name,
            source=SOURCE,
            entities=[Entity(name="id")],
            schema=[
                Field(
                    name="vector",
                    dtype=Array(Float32),
                    tags={
                        "description": "float32",
                        "dimensions": "10",
                        "index_type": "HNSW",
                    },
                ),
                Field(
                    name="id",
                    dtype=String,
                ),
                Field(
                    name="text",
                    dtype=String,
                ),
                Field(
                    name="int",
                    dtype=Int32,
                ),
                Field(
                    name="long",
                    dtype=Int64,
                ),
                Field(
                    name="float",
                    dtype=Float32,
                ),
                Field(
                    name="double",
                    dtype=Float64,
                ),
                Field(
                    name="binary",
                    dtype=Bytes,
                ),
                Field(
                    name="bool",
                    dtype=Bool,
                ),
                Field(
                    name="timestamp",
                    dtype=UnixTimestamp,
                ),
                Field(
                    name="byte_list",
                    dtype=Array(Bytes),
                ),
            ],
        )
        return fv, [
            (
                EntityKeyProto(
                    join_keys=["id"],
                    entity_values=[ValueProto(string_val=str(i))],
                ),
                {
                    "vector": ValueProto(
                        float_list_val=FloatList(
                            val=[random.random() for _ in range(10)]
                        )
                    ),
                    "text": ValueProto(string_val="text"),
                    "int": ValueProto(int32_val=n),
                    "long": ValueProto(int64_val=n),
                    "float": ValueProto(float_val=n * 0.3),
                    "double": ValueProto(double_val=n * 1.2),
                    "binary": ValueProto(bytes_val=b"binary"),
                    "bool": ValueProto(bool_val=True),
                    "timestamp": ValueProto(
                        unix_timestamp_val=int(datetime.utcnow().timestamp() * 1000)
                    ),
                    "byte_list": ValueProto(
                        bytes_list_val=BytesList(val=[b"a", b"b", b"c"])
                    ),
                },
                datetime.utcnow(),
                None,
            )
            for i in range(n)
        ]
