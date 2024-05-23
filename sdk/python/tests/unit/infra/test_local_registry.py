# Copyright 2022 The Feast Authors
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
from tempfile import mkstemp

import pytest

from feast.entity import Entity
from feast.infra.registry.registry import Registry
from feast.repo_config import RegistryConfig


def test_commit():
    fd, registry_path = mkstemp()
    registry_config = RegistryConfig(path=registry_path, cache_ttl_seconds=600)
    test_registry = Registry("project", registry_config, None)

    entity = Entity(
        name="driver_car_id",
        description="Car driver id",
        tags={"team": "matchmaking"},
    )

    project = "project"

    # Register Entity without commiting
    test_registry.apply_entity(entity, project, commit=False)
    assert test_registry.cached_registry_proto
    assert len(test_registry.cached_registry_proto.project_metadata) == 1
    project_metadata = test_registry.cached_registry_proto.project_metadata[0]
    project_uuid = project_metadata.project_uuid
    assert len(project_uuid) == 36
    validate_project_uuid(project_uuid, test_registry)

    # Retrieving the entity should still succeed
    entities = test_registry.list_entities(project, allow_cache=True)
    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )
    validate_project_uuid(project_uuid, test_registry)

    entity = test_registry.get_entity("driver_car_id", project, allow_cache=True)
    assert (
        entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )
    validate_project_uuid(project_uuid, test_registry)

    # Create new registry that points to the same store
    registry_with_same_store = Registry("project", registry_config, None)

    # Retrieving the entity should fail since the store is empty
    entities = registry_with_same_store.list_entities(project)
    assert len(entities) == 0
    validate_project_uuid(project_uuid, registry_with_same_store)

    # commit from the original registry
    test_registry.commit()

    # Reconstruct the new registry in order to read the newly written store
    registry_with_same_store = Registry("project", registry_config, None)

    # Retrieving the entity should now succeed
    entities = registry_with_same_store.list_entities(project)
    entity = entities[0]
    assert (
        len(entities) == 1
        and entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )
    validate_project_uuid(project_uuid, registry_with_same_store)

    entity = test_registry.get_entity("driver_car_id", project)
    assert (
        entity.name == "driver_car_id"
        and entity.description == "Car driver id"
        and "team" in entity.tags
        and entity.tags["team"] == "matchmaking"
    )

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto(project=project)


def validate_project_uuid(project_uuid, test_registry):
    assert len(test_registry.cached_registry_proto.project_metadata) == 1
    project_metadata = test_registry.cached_registry_proto.project_metadata[0]
    assert project_metadata.project_uuid == project_uuid
