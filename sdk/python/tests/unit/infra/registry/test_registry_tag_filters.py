from __future__ import annotations

from pathlib import Path

import pytest

from feast import Entity, FeatureStore, FeatureView, Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.registry import Registry
from feast.infra.registry.sql import SqlRegistry
from feast.repo_config import RepoConfig
from feast.types import Int64
from feast.value_type import ValueType


@pytest.mark.parametrize("registry_type", ["file", "sql"])
@pytest.mark.parametrize("resource_type", ["entities", "feature_views"])
@pytest.mark.parametrize(
    "initial_tags,updated_tags,initial_names,updated_names",
    [
        (
            {"environment": "production"},
            {"environment": "staging"},
            {"production"},
            {"staging"},
        ),
        ({}, {"environment": "production"}, {"production", "staging"}, {"production"}),
        ({"environment": "production"}, {}, {"production"}, {"production", "staging"}),
    ],
    ids=["change-value", "add-tag", "remove-tag"],
)
def test_cached_list_uses_current_tag_filter(
    tmp_path: Path,
    registry_type: str,
    resource_type: str,
    initial_tags: dict[str, str],
    updated_tags: dict[str, str],
    initial_names: set[str],
    updated_names: set[str],
) -> None:
    """Reusing a tag dictionary must not return results for its previous contents."""
    registry_path = tmp_path / "registry.db"
    registry_config = (
        {"path": str(registry_path), "cache_ttl_seconds": 0}
        if registry_type == "file"
        else {
            "registry_type": "sql",
            "path": f"sqlite:///{registry_path}",
            "cache_ttl_seconds": 0,
        }
    )
    store = FeatureStore(
        config=RepoConfig(
            project="test_project", registry=registry_config, provider="local"
        )
    )
    try:
        for environment in ("production", "staging"):
            tags = {"environment": environment}
            entity = Entity(name=environment, value_type=ValueType.INT64, tags=tags)
            store.registry.apply_entity(entity, store.project)
            store.registry.apply_feature_view(
                FeatureView(
                    name=environment,
                    entities=[entity],
                    schema=[
                        Field(name=environment, dtype=Int64),
                        Field(name="value", dtype=Int64),
                    ],
                    source=FileSource(
                        path="unused.parquet", timestamp_field="event_timestamp"
                    ),
                    tags=tags,
                ),
                store.project,
            )
        store.refresh_registry()

        def list_names(tags: dict[str, str]) -> set[str]:
            if resource_type == "entities":
                return {
                    obj.name for obj in store.list_entities(allow_cache=True, tags=tags)
                }
            return {
                obj.name
                for obj in store.list_feature_views(allow_cache=True, tags=tags)
            }

        query_tags = dict(initial_tags)
        assert list_names(query_tags) == initial_names

        query_tags.clear()
        query_tags.update(updated_tags)
        assert list_names(query_tags) == updated_names
        # A separate caller with an equivalent filter must also see correct results.
        assert list_names(dict(updated_tags)) == updated_names
    finally:
        registry = store.registry
        assert isinstance(registry, (Registry, SqlRegistry))
        registry.teardown()
