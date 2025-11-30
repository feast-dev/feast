# Copyright 2024 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from types import SimpleNamespace

import pytest

from feast.infra.online_stores.milvus_online_store import milvus as milvus_module
from feast.infra.online_stores.milvus_online_store.milvus import (
    MilvusOnlineStore,
    MilvusOnlineStoreConfig,
    MultiTenancyMode,
    _table_id,
)
from feast.types import PrimitiveFeastType


class FakeIndexParams:
    def __init__(self):
        self.indexes = []

    def add_index(self, **kwargs):
        self.indexes.append(kwargs)


class FakeMilvusClient:
    def __init__(self, existing=None, existing_dbs=None, **_kwargs):
        self.existing = set(existing or [])
        self.created_collections = []
        self.dropped_collections = []
        self.loaded_collections = []
        self.schema = None
        self.existing_dbs = set(existing_dbs or ["default"])
        self.created_databases = []
        self.selected_db = None

    def has_collection(self, collection_name: str) -> bool:
        return collection_name in self.existing

    def create_collection(self, collection_name: str, dimension: int, schema):
        self.created_collections.append(collection_name)
        self.existing.add(collection_name)
        self.schema = schema

    def load_collection(self, collection_name: str):
        self.loaded_collections.append(collection_name)

    def prepare_index_params(self):
        return FakeIndexParams()

    def create_index(self, collection_name: str, index_params):
        self.index_params = (collection_name, index_params)

    def describe_collection(self, collection_name: str):
        return {
            "collection_name": collection_name,
            "fields": [
                {"name": field.name}
                for field in (self.schema.fields if self.schema else [])
            ],
        }

    def drop_collection(self, collection_name: str):
        self.dropped_collections.append(collection_name)
        self.existing.discard(collection_name)

    def list_databases(self):
        return list(self.existing_dbs)

    def create_database(self, db_name: str):
        self.created_databases.append(db_name)
        self.existing_dbs.add(db_name)

    def using_database(self, db_name: str):
        self.selected_db = db_name

    def close(self):
        pass


def _build_test_table(name: str = "user_profile") -> SimpleNamespace:
    entity = SimpleNamespace(
        name="user_id",
        dtype=PrimitiveFeastType.INT64,
        vector_index=False,
    )
    vector = SimpleNamespace(
        name="embedding",
        dtype=PrimitiveFeastType.FLOAT32,
        vector_index=True,
        vector_search_metric="COSINE",
    )
    country = SimpleNamespace(
        name="country",
        dtype=PrimitiveFeastType.STRING,
        vector_index=False,
    )
    return SimpleNamespace(
        name=name,
        schema=[entity, vector, country],
        features=[vector, country],
        entity_columns=[entity],
    )


def _build_config(prefix: str) -> SimpleNamespace:
    return SimpleNamespace(
        project="prefixed_project",
        provider="remote",
        online_store=MilvusOnlineStoreConfig(
            collection_name_prefix=prefix,
            embedding_dim=16,
            metric_type="COSINE",
            index_type="FLAT",
            nlist=8,
        ),
    )


def test_default_config():
    config = MilvusOnlineStoreConfig()

    assert config.type == "milvus"
    assert config.multi_tenancy_mode == MultiTenancyMode.NONE
    assert config.db_name == ""
    assert config.collection_name_prefix == ""


def test_custom_config_database_mode():
    config = MilvusOnlineStoreConfig(
        host="http://milvus-server",
        port=19531,
        index_type="IVF_FLAT",
        metric_type="L2",
        embedding_dim=768,
        username="admin",
        password="secret",
        db_name="marketing_db",
        multi_tenancy_mode=MultiTenancyMode.DATABASE,
    )

    assert config.host == "http://milvus-server"
    assert config.port == 19531
    assert config.index_type == "IVF_FLAT"
    assert config.metric_type == "L2"
    assert config.embedding_dim == 768
    assert config.db_name == "marketing_db"
    assert config.multi_tenancy_mode == MultiTenancyMode.DATABASE


def test_db_name_for_multi_tenancy_inference():
    marketing_config = MilvusOnlineStoreConfig(db_name="marketing_db")
    sales_config = MilvusOnlineStoreConfig(db_name="sales_db")

    assert marketing_config.db_name == "marketing_db"
    assert marketing_config.multi_tenancy_mode == MultiTenancyMode.DATABASE
    assert sales_config.db_name == "sales_db"
    assert sales_config.multi_tenancy_mode == MultiTenancyMode.DATABASE
    assert marketing_config.db_name != sales_config.db_name


def test_collection_prefix_for_multi_tenancy_inference():
    config = MilvusOnlineStoreConfig(collection_name_prefix="tenant_alpha")

    assert config.collection_name_prefix == "tenant_alpha"
    assert config.multi_tenancy_mode == MultiTenancyMode.COLLECTION_PREFIX


def test_local_mode_config():
    config = MilvusOnlineStoreConfig(
        path="data/online_store.db",
        embedding_dim=256,
    )

    assert config.path == "data/online_store.db"
    assert config.embedding_dim == 256
    assert config.multi_tenancy_mode == MultiTenancyMode.NONE


def test_config_serialization():
    config = MilvusOnlineStoreConfig(
        embedding_dim=512,
        collection_name_prefix="tenant123",
        multi_tenancy_mode=MultiTenancyMode.COLLECTION_PREFIX,
    )

    config_dict = config.model_dump()

    assert config_dict["type"] == "milvus"
    assert config_dict["embedding_dim"] == 512
    assert config_dict["collection_name_prefix"] == "tenant123"
    assert config_dict["multi_tenancy_mode"] == MultiTenancyMode.COLLECTION_PREFIX


def test_mode_inference_and_conflicts():
    config = MilvusOnlineStoreConfig()
    assert config.multi_tenancy_mode == MultiTenancyMode.NONE

    config_db = MilvusOnlineStoreConfig(db_name="tenant_db")
    assert config_db.multi_tenancy_mode == MultiTenancyMode.DATABASE

    config_prefix = MilvusOnlineStoreConfig(collection_name_prefix="tenant")
    assert config_prefix.multi_tenancy_mode == MultiTenancyMode.COLLECTION_PREFIX

    with pytest.raises(ValueError):
        MilvusOnlineStoreConfig(
            db_name="tenant_db",
            collection_name_prefix="tenant",
        )


def test_database_mode_requires_db_name():
    with pytest.raises(ValueError):
        MilvusOnlineStoreConfig(multi_tenancy_mode=MultiTenancyMode.DATABASE)


def test_prefix_mode_requires_prefix():
    with pytest.raises(ValueError):
        MilvusOnlineStoreConfig(multi_tenancy_mode=MultiTenancyMode.COLLECTION_PREFIX)


def test_prefix_mode_forbids_db_name():
    with pytest.raises(ValueError):
        MilvusOnlineStoreConfig(
            db_name="tenant_db",
            multi_tenancy_mode=MultiTenancyMode.COLLECTION_PREFIX,
        )


def test_database_mode_forbids_prefix():
    with pytest.raises(ValueError):
        MilvusOnlineStoreConfig(
            collection_name_prefix="tenant",
            multi_tenancy_mode=MultiTenancyMode.DATABASE,
            db_name="tenant_db",
        )


def test_none_mode_forbids_both():
    with pytest.raises(ValueError):
        MilvusOnlineStoreConfig(
            db_name="tenant_db",
            multi_tenancy_mode=MultiTenancyMode.NONE,
        )
    with pytest.raises(ValueError):
        MilvusOnlineStoreConfig(
            collection_name_prefix="tenant",
            multi_tenancy_mode=MultiTenancyMode.NONE,
        )


def test_table_id_logic_per_mode():
    table = SimpleNamespace(name="fv")

    config_none = SimpleNamespace(
        project="project_one",
        online_store=MilvusOnlineStoreConfig(),
    )
    assert _table_id(config_none, table) == "project_one_fv"

    config_db = SimpleNamespace(
        project="project_one",
        online_store=MilvusOnlineStoreConfig(db_name="tenant_db"),
    )
    assert config_db.online_store.multi_tenancy_mode == MultiTenancyMode.DATABASE
    assert _table_id(config_db, table) == "project_one_fv"

    config_prefix = SimpleNamespace(
        project="project_one",
        online_store=MilvusOnlineStoreConfig(collection_name_prefix="tenant_alpha"),
    )
    assert _table_id(config_prefix, table) == "tenant_alpha_project_one_fv"


def test_connection_params_respect_mode():
    store = MilvusOnlineStore()
    config_db = SimpleNamespace(
        online_store=MilvusOnlineStoreConfig(db_name="tenant_db"),
    )
    params = store._get_connection_params(config_db, include_db_name=True)
    assert params["db_name"] == "tenant_db"

    config_prefix = SimpleNamespace(
        online_store=MilvusOnlineStoreConfig(collection_name_prefix="tenant"),
    )
    params_prefix = store._get_connection_params(config_prefix, include_db_name=True)
    assert "db_name" not in params_prefix or params_prefix["db_name"] == ""


def test_database_creation_and_switch(monkeypatch):
    store = MilvusOnlineStore()
    created_clients = []

    def _fake_client_factory(**kwargs):
        client = FakeMilvusClient(existing_dbs={"default"})
        created_clients.append(client)
        return client

    monkeypatch.setattr(milvus_module, "MilvusClient", _fake_client_factory)

    config = SimpleNamespace(
        provider="remote",
        online_store=MilvusOnlineStoreConfig(
            db_name="tenant_db",
            multi_tenancy_mode=MultiTenancyMode.DATABASE,
        ),
    )

    store._ensure_database_exists(config)
    store._ensure_database_exists(config)

    assert created_clients[0].created_databases == ["tenant_db"]
    assert created_clients[0].selected_db == "tenant_db"


def test_prefixed_collection_created(monkeypatch):
    """Milvus client requests should use prefixed collection identifiers."""

    config = _build_config(prefix="tenant_alpha")
    table = _build_test_table()
    store = MilvusOnlineStore()
    fake_client = FakeMilvusClient()
    monkeypatch.setattr(store, "_connect", lambda cfg: fake_client)

    collection = store._get_or_create_collection(config, table)

    expected_name = "tenant_alpha_prefixed_project_user_profile"
    assert fake_client.created_collections == [expected_name]
    assert collection["collection_name"] == expected_name


def test_existing_prefixed_collection_loaded(monkeypatch):
    """Existing collections should be loaded using the prefixed identifier."""

    config = _build_config(prefix="tenant_beta")
    table = _build_test_table()
    expected_name = _table_id(config, table)
    fake_client = FakeMilvusClient(existing={expected_name})
    store = MilvusOnlineStore()
    monkeypatch.setattr(store, "_connect", lambda cfg: fake_client)

    store._get_or_create_collection(config, table)

    assert fake_client.loaded_collections == [expected_name]


def test_prefixed_collection_dropped(monkeypatch):
    """Drop operations should target prefixed collection names to isolate tenants."""

    config = _build_config(prefix="tenant_gamma")
    table = _build_test_table()
    expected_name = _table_id(config, table)
    store = MilvusOnlineStore()
    fake_client = FakeMilvusClient(existing={expected_name})
    store._collections = {expected_name: object()}

    monkeypatch.setattr(store, "_connect", lambda cfg: fake_client)
    monkeypatch.setattr(store, "_ensure_database_exists", lambda cfg: None)

    store.update(
        config,
        tables_to_delete=[table],
        tables_to_keep=[],
        entities_to_delete=(),
        entities_to_keep=(),
        partial=False,
    )

    assert fake_client.dropped_collections == [expected_name]
    assert expected_name not in store._collections
