import os
from datetime import datetime, timedelta

import pandas as pd
import pytest

from feast import Entity, FeatureStore, FeatureView, Field, FileSource
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.registry_server import RegistryServer
from feast.repo_config import RepoConfig
from feast.types import Float32

PROJECT = "dest_confinement"


@pytest.fixture
def registry_server(tmp_path, monkeypatch):
    monkeypatch.setattr("feast.registry_server.is_auth_necessary", lambda sm: True)

    data_dir = tmp_path / "repo"
    data_dir.mkdir()
    (data_dir / "data").mkdir()

    pd.DataFrame(
        {
            "driver_id": [1001, 1002],
            "event_timestamp": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            "conv_rate": [0.1, 0.2],
        }
    ).to_parquet(str(data_dir / "data" / "driver_stats.parquet"))

    store = FeatureStore(
        config=RepoConfig(
            project=PROJECT,
            registry=str(data_dir / "registry.db"),
            provider="local",
            repo_path=str(data_dir),
            entity_key_serialization_version=3,
        )
    )

    driver = Entity(name="driver", join_keys=["driver_id"])
    file_source = FileSource(
        name="driver_stats_source",
        path="data/driver_stats.parquet",
        timestamp_field="event_timestamp",
    )
    bq_source = BigQuerySource(
        name="driver_stats_bq",
        table="my-project.ds.driver_stats",
        timestamp_field="event_timestamp",
    )
    fv = FeatureView(
        name="driver_stats",
        entities=[driver],
        ttl=timedelta(days=1),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=file_source,
    )
    store.apply([driver, file_source, bq_source, fv])

    return RegistryServer(store.registry, store=store), str(data_dir)


def _validate(rs, storage_type, path):
    return rs._validate_saved_dataset_destination(
        storage_type=storage_type, storage_path=path, project=PROJECT
    )


def test_in_repo_path_is_allowed(registry_server):
    rs, repo = registry_server
    resolved = _validate(rs, "file", "data/my_dataset.parquet")
    assert os.path.realpath(resolved).startswith(os.path.realpath(repo))
    assert resolved.endswith("my_dataset.parquet")


def test_absolute_escape_is_rejected(registry_server):
    rs, _ = registry_server
    with pytest.raises(ValueError):
        _validate(rs, "file", "/etc/written-by-attacker.parquet")


def test_parent_traversal_is_rejected(registry_server):
    rs, _ = registry_server
    with pytest.raises(ValueError):
        _validate(rs, "file", "../../etc/x.parquet")


def test_overwriting_registered_file_source_is_rejected(registry_server):
    rs, _ = registry_server
    with pytest.raises(ValueError):
        _validate(rs, "file", "data/driver_stats.parquet")


def test_overwriting_registered_table_source_is_rejected(registry_server):
    rs, _ = registry_server
    with pytest.raises(ValueError):
        _validate(rs, "bigquery", "my-project.ds.driver_stats")


def test_new_table_is_allowed(registry_server):
    rs, _ = registry_server
    assert _validate(rs, "bigquery", "my-project.ds.new_dataset") == (
        "my-project.ds.new_dataset"
    )


def test_no_auth_keeps_raw_path(tmp_path, monkeypatch):
    monkeypatch.setattr("feast.registry_server.is_auth_necessary", lambda sm: False)
    data_dir = tmp_path / "repo"
    data_dir.mkdir()
    store = FeatureStore(
        config=RepoConfig(
            project=PROJECT,
            registry=str(data_dir / "registry.db"),
            provider="local",
            repo_path=str(data_dir),
            entity_key_serialization_version=3,
        )
    )
    rs = RegistryServer(store.registry, store=store)
    assert _validate(rs, "file", "/tmp/anywhere.parquet") == "/tmp/anywhere.parquet"
