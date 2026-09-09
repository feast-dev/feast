"""Unit tests for the ``RegistryServer.Proto`` RPC (issue #6558).

The RPC must build the ``RegistryProto`` from individually RBAC-filtered list calls
rather than returning ``proxied_registry.proto()`` directly (which would bypass
permissions). Under ``NoAuthConfig`` filtering is a no-op, so the full registry is
returned; with auth enabled only ``DESCRIBE``-permitted objects are included.
"""

from datetime import datetime, timezone
from unittest.mock import patch

from google.protobuf.empty_pb2 import Empty

from feast.data_source import DataSource
from feast.entity import Entity
from feast.feast_object import FeastObject
from feast.project import Project
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.registry_server import RegistryServer
from feast.value_type import ValueType

# The registry's authentic metadata, returned by _FakeRegistry.proto(). Proto() must carry these
# through rather than stamping "now" (issue #6558 review feedback).
_REGISTRY_LAST_UPDATED = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
_REGISTRY_VERSION_ID = "test-version-id"


class _FakeRegistry:
    """Minimal BaseRegistry stand-in exposing only the list_* calls Proto uses."""

    def __init__(self, projects, entities_by_project, data_sources_by_project):
        self._projects = projects
        self._entities = entities_by_project
        self._data_sources = data_sources_by_project

    def proto(self) -> RegistryProto:
        # Source of the authentic last_updated / version_id metadata. Proto() reads only these
        # scalar fields from here (no objects), so RBAC filtering is unaffected.
        proto = RegistryProto()
        proto.version_id = _REGISTRY_VERSION_ID
        proto.last_updated.FromDatetime(_REGISTRY_LAST_UPDATED)
        return proto

    def list_projects(self, allow_cache: bool = False, tags=None):
        return self._projects

    def list_entities(self, project: str, allow_cache: bool = False, tags=None):
        return self._entities.get(project, [])

    def list_data_sources(self, project: str, allow_cache: bool = False, tags=None):
        return self._data_sources.get(project, [])

    # Every other object type is empty for this fixture.
    def _empty(self, *args, **kwargs):
        return []

    list_feature_views = _empty
    list_stream_feature_views = _empty
    list_on_demand_feature_views = _empty
    list_label_views = _empty
    list_feature_services = _empty
    list_saved_datasets = _empty
    list_validation_references = _empty
    list_permissions = _empty


def _entity(name: str) -> Entity:
    return Entity(name=name, value_type=ValueType.STRING)


def _data_source(name: str) -> DataSource:
    from feast.infra.offline_stores.file_source import FileSource

    return FileSource(name=name, path=f"/tmp/{name}.parquet", timestamp_field="ts")


def _build_server() -> tuple[RegistryServer, _FakeRegistry]:
    registry = _FakeRegistry(
        projects=[Project(name="proj_a"), Project(name="proj_b")],
        entities_by_project={
            "proj_a": [_entity("driver"), _entity("customer")],
            "proj_b": [_entity("merchant")],
        },
        data_sources_by_project={"proj_a": [_data_source("src_a")]},
    )
    return RegistryServer(registry), registry  # type: ignore[arg-type]


def test_proto_returns_full_registry_when_no_auth():
    """NoAuthConfig (no security manager) -> every object across all projects."""
    server, _ = _build_server()

    result = server.Proto(Empty(), context=None)

    assert {p.spec.name for p in result.projects} == {"proj_a", "proj_b"}
    assert {e.spec.name for e in result.entities} == {"driver", "customer", "merchant"}
    assert {d.name for d in result.data_sources} == {"src_a"}
    # last_updated / version_id are carried from the registry's real proto (not stamped "now"),
    # so cache consumers see the registry's authentic freshness metadata.
    assert result.version_id == _REGISTRY_VERSION_ID
    assert result.last_updated.ToDatetime(tzinfo=timezone.utc) == _REGISTRY_LAST_UPDATED


def test_proto_filters_by_describe_permission():
    """With RBAC, only DESCRIBE-permitted objects are included."""
    server, _ = _build_server()

    # Simulate a security manager that permits everything except the "customer"
    # entity, regardless of object type (filters by DESCRIBE).
    def fake_permitted(resources: list[FeastObject], actions):
        return [r for r in resources if getattr(r, "name", None) != "customer"]

    with patch(
        "feast.registry_server.permitted_resources", side_effect=fake_permitted
    ) as mocked:
        result = server.Proto(Empty(), context=None)

    assert mocked.called
    # "customer" is filtered out; everything else survives.
    assert {e.spec.name for e in result.entities} == {"driver", "merchant"}
    assert {p.spec.name for p in result.projects} == {"proj_a", "proj_b"}
    assert {d.name for d in result.data_sources} == {"src_a"}


def test_proto_empty_registry():
    """No projects -> empty (but valid) RegistryProto, not an error."""
    server = RegistryServer(_FakeRegistry([], {}, {}))  # type: ignore[arg-type]

    result = server.Proto(Empty(), context=None)

    assert len(result.projects) == 0
    assert len(result.entities) == 0
