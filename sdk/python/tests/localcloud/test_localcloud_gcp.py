"""Native Feast round trips against the run project's LocalCloud services."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from google.cloud import bigquery, storage

from feast.infra.offline_stores.bigquery import (
    BigQueryOfflineStoreConfig,
    _get_bigquery_client,
)
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.online_stores.bigtable import (
    BigtableOnlineStore,
    BigtableOnlineStoreConfig,
)
from feast.infra.registry.gcs import GCSRegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig, RepoConfig


def _run_project() -> str:
    return os.environ["GOOGLE_CLOUD_PROJECT"]


def test_gcs_registry_round_trip() -> None:
    assert os.environ["STORAGE_EMULATOR_HOST"].startswith("http://")
    client = storage.Client(project=_run_project())
    bucket = client.create_bucket(f"feast-lc-{uuid4().hex}")
    try:
        store = GCSRegistryStore(
            RegistryConfig(path=f"gs://{bucket.name}/registry.pb"), Path(".")
        )
        original = RegistryProto()
        original.project_metadata.add(project="localcloud", project_uuid="feast")
        store.update_registry_proto(original)
        readback = store.get_registry_proto()
        assert readback.project_metadata[0].project_uuid == "feast"
        assert (
            bucket.blob("registry.pb").download_as_bytes()
            == readback.SerializeToString()
        )
        store.teardown()
    finally:
        bucket.delete(force=True)
        client.close()


def test_bigtable_online_store_round_trip() -> None:
    assert os.environ["BIGTABLE_EMULATOR_HOST"]
    instance_id = f"feast-lc-{uuid4().hex[:12]}"
    config = RepoConfig(
        registry="/tmp/feast-localcloud-registry.pb",
        project="localcloud",
        provider="local",
        online_store=BigtableOnlineStoreConfig(
            instance=instance_id, project_id=_run_project()
        ),
        offline_store=DaskOfflineStoreConfig(),
    )
    store = BigtableOnlineStore()
    client = store._get_client(config.online_store, admin=True)
    instance = client.instance(instance_id)
    instance.create(
        clusters=[instance.cluster("local", location_id="us-central1-a", serve_nodes=1)]
    ).result(timeout=60)
    try:
        feature_view = SimpleNamespace(name="localcloud_fv", features=[object()])
        table_name = "feast_test"
        table = instance.table(table_name)
        table.create(column_families={store.feature_column_family: None})
        store._get_table_name = lambda **_: table_name
        key = EntityKeyProto(join_keys=["id"], entity_values=[ValueProto(int64_val=7)])
        value = ValueProto(string_val="hello LocalCloud")
        event_time = datetime.now(timezone.utc)
        store.online_write_batch(
            config, feature_view, [(key, {"message": value}, event_time, None)], None
        )
        written_row = table.read_row(
            store._compute_row_key(key, feature_view.name, config)
        )
        assert (
            written_row.cells[store.feature_column_family][b"message"][0].value
            == value.SerializeToString()
        )
        rows = store.online_read(config, feature_view, [key], ["message"])
        assert rows[0][1]["message"].string_val == "hello LocalCloud"
        table.delete()
    finally:
        instance.delete()
        client.close()


def test_bigquery_offline_store_client_round_trip() -> None:
    assert os.environ["BIGQUERY_EMULATOR_HOST"].startswith("http://")
    client = _get_bigquery_client(project=_run_project())
    dataset = client.create_dataset(f"{_run_project()}.feast_lc_{uuid4().hex[:12]}")
    try:
        table = client.create_table(
            bigquery.Table(
                dataset.reference.table("messages"),
                schema=[bigquery.SchemaField("message", "STRING")],
            )
        )
        assert client.insert_rows_json(table, [{"message": "hello LocalCloud"}]) == []
        rows = list(
            client.query(
                f"SELECT message FROM `{table.project}.{table.dataset_id}.{table.table_id}`"
            ).result(timeout=30)
        )
        assert [row.message for row in rows] == ["hello LocalCloud"]
        source = BigQuerySource(table=table.full_table_id.replace(":", "."))
        config = RepoConfig(
            registry="/tmp/feast-localcloud-registry.pb",
            project="localcloud",
            provider="gcp",
            online_store=BigtableOnlineStoreConfig(
                instance="unused", project_id=_run_project()
            ),
            offline_store=BigQueryOfflineStoreConfig(
                project_id=_run_project(), dataset=dataset.dataset_id
            ),
        )
        source.validate(config)
        assert source.get_table_column_names_and_types(config) == [
            ("message", "STRING")
        ]
    finally:
        client.delete_dataset(dataset, delete_contents=True)
        client.close()
