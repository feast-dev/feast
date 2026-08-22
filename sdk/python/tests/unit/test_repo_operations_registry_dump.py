from __future__ import annotations

import json

from feast import Entity, FeatureStore
from feast.infra.registry.sql import SqlRegistryConfig
from feast.repo_config import RepoConfig
from feast.repo_operations import registry_dump
from feast.value_type import ValueType


def test_registry_dump_uses_configured_sql_registry(tmp_path):
    project = "test_project"
    config = RepoConfig(
        project=project,
        registry=SqlRegistryConfig(path=f"sqlite:///{tmp_path / 'registry.db'}"),
        provider="local",
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
        offline_store={"type": "file"},
    )
    store = FeatureStore(repo_path=tmp_path, config=config)
    store.registry.apply_entity(
        Entity(
            name="driver",
            join_keys=["driver_id"],
            value_type=ValueType.INT64,
        ),
        project,
    )

    dumped_registry = json.loads(registry_dump(config, tmp_path))

    assert dumped_registry["project"] == project
    assert [entity["spec"]["name"] for entity in dumped_registry["entities"]] == [
        "driver"
    ]
