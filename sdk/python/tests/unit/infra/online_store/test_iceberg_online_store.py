import types

import pytest


pyiceberg = pytest.importorskip("pyiceberg")
pyarrow = pytest.importorskip("pyarrow")

from pyiceberg.transforms import IdentityTransform

from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.infra.online_stores.contrib.iceberg_online_store.iceberg import (
    IcebergOnlineStore,
    IcebergOnlineStoreConfig,
)


def test_iceberg_online_store_config_storage_options_isolated():
    config1 = IcebergOnlineStoreConfig()
    config2 = IcebergOnlineStoreConfig()

    config1.storage_options["k"] = "v"

    assert "k" not in config2.storage_options


def test_iceberg_online_store_partition_spec_entity_hash_identity_transform():
    store = IcebergOnlineStore()
    config = IcebergOnlineStoreConfig(partition_strategy="entity_hash")

    spec = store._build_partition_spec(config)

    assert len(spec.fields) == 1
    assert isinstance(spec.fields[0].transform, IdentityTransform)


def test_iceberg_online_read_applies_selected_fields_projection(monkeypatch):
    store = IcebergOnlineStore()

    online_config = IcebergOnlineStoreConfig(
        catalog_type="sql",
        catalog_name="test_catalog",
        uri="sqlite:///dummy.db",
        warehouse="warehouse",
        namespace="online",
        partition_strategy="entity_hash",
        partition_count=256,
    )

    repo_config = types.SimpleNamespace(
        online_store=online_config,
        project="test_project",
        entity_key_serialization_version=3,
    )

    feature_view = types.SimpleNamespace(
        name="driver_stats",
        features=[
            types.SimpleNamespace(name="conv_rate"),
            types.SimpleNamespace(name="acc_rate"),
        ],
    )

    class DummyScan:
        def __init__(self, selected_fields):
            self.selected_fields = selected_fields

        def to_arrow(self):
            return pyarrow.Table.from_pydict({c: [] for c in self.selected_fields})

    class DummyIcebergTable:
        def __init__(self):
            self.scan_kwargs = None

        def scan(self, **kwargs):
            self.scan_kwargs = kwargs
            return DummyScan(kwargs.get("selected_fields", ("*",)))

    dummy_table = DummyIcebergTable()
    dummy_catalog = types.SimpleNamespace(load_table=lambda identifier: dummy_table)

    monkeypatch.setattr(store, "_load_catalog", lambda cfg: dummy_catalog)
    monkeypatch.setattr(store, "_get_table_identifier", lambda cfg, project, tbl: "online.test")

    entity_hashes = iter([1, 2])
    monkeypatch.setattr(store, "_hash_entity_key", lambda *args, **kwargs: next(entity_hashes))

    monkeypatch.setattr(
        store,
        "_convert_arrow_to_feast",
        lambda *args, **kwargs: [(None, None), (None, None)],
    )

    store.online_read(
        config=repo_config,
        table=feature_view,
        entity_keys=[EntityKeyProto(), EntityKeyProto()],
        requested_features=["conv_rate"],
    )

    assert dummy_table.scan_kwargs is not None
    assert dummy_table.scan_kwargs["row_filter"] == "entity_hash IN (1,2)"
    assert dummy_table.scan_kwargs["selected_fields"] == (
        "entity_key",
        "entity_hash",
        "event_ts",
        "created_ts",
        "conv_rate",
    )
