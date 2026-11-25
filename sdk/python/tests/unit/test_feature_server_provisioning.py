"""Authorization and infrastructure scope of remote provisioning endpoints."""

from __future__ import annotations

import base64
from unittest.mock import Mock

import httpx
import pytest

from feast import (
    Entity,
    FeatureStore,
    FeatureView,
    Field,
    FileSource,
    Project,
    RepoConfig,
    data_source,
)
from feast.constants import PROTECTED_PROJECT_TAG
from feast.feature_server import get_app
from feast.infra.online_stores.remote import encode_infra_object
from feast.permissions import security_manager
from feast.permissions.action import AuthzedAction
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.server.rest import inject_user_details
from feast.permissions.user import User
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.types import Int64
from feast.value_type import ValueType


@pytest.fixture
async def infra_client(tmp_path, monkeypatch):
    entity = Entity(name="id", value_type=ValueType.INT64)
    view = FeatureView(
        name="victim",
        entities=[entity],
        schema=[Field(name="id", dtype=Int64), Field(name="value", dtype=Int64)],
        source=FileSource(path="unused.parquet", timestamp_field="ts"),
    )
    store = FeatureStore(
        config=RepoConfig(
            project="test",
            provider="local",
            registry=str(tmp_path / "registry.db"),
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
        )
    )
    monkeypatch.setattr(security_manager, "_sm", None)
    store.apply([entity, view])
    registry = Mock(wraps=store.registry)
    registry.list_permissions.return_value = []
    sm = security_manager.SecurityManager(project=store.project, registry=registry)
    monkeypatch.setattr(security_manager, "_sm", sm)
    app = get_app(store)

    async def identify():
        sm.set_current_user(User("tester", roles=["test"]))

    app.dependency_overrides[inject_user_details] = identify
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        yield client, store, registry, view
    await store.close()


def grant(registry, resource_type, action):
    registry.list_permissions.return_value = [
        Permission(
            name="test",
            types=resource_type,
            actions=[action],
            policy=RoleBasedPolicy(roles=["test"]),
        )
    ]


@pytest.mark.parametrize("body", [{}, {"tables": [{"type": "Entity", "proto": ""}]}])
async def test_teardown_requires_project_delete(infra_client, body):
    client, store, registry, _ = infra_client
    grant(registry, Entity, AuthzedAction.DELETE)
    response = await client.post("/teardown-infra", json=body)
    assert response.status_code == 403
    assert (
        store._get_provider()
        .online_store._get_conn(store.config)
        .execute("SELECT name FROM sqlite_master WHERE name='test_victim'")
        .fetchone()
    )


async def test_table_list_rejects_entities(infra_client):
    client, store, registry, view = infra_client
    grant(registry, Entity, AuthzedAction.DELETE)
    response = await client.post(
        "/update-infra",
        json={
            "tables_to_delete": [
                encode_infra_object(Entity(name=view.name, value_type=ValueType.INT64))
            ]
        },
    )
    assert response.status_code == 400
    assert (
        store._get_provider()
        .online_store._get_conn(store.config)
        .execute("SELECT name FROM sqlite_master WHERE name='test_victim'")
        .fetchone()
    )


@pytest.mark.parametrize("operation", ["update", "teardown"])
async def test_provisioning_only_calls_online_store(
    infra_client, operation, monkeypatch
):
    client, store, registry, view = infra_client
    grant(
        registry,
        [FeatureView, Entity, Project],
        AuthzedAction.CREATE if operation == "update" else AuthzedAction.DELETE,
    )
    provider = store._get_provider()
    online_call = Mock()
    batch = Mock()
    monkeypatch.setattr(provider.online_store, operation, online_call)
    monkeypatch.setattr(provider, "_batch_engine", batch)
    body = {
        "tables_to_keep" if operation == "update" else "tables": [
            encode_infra_object(view)
        ]
    }
    response = await client.post(f"/{operation}-infra", json=body)
    assert response.status_code == 200, response.text
    online_call.assert_called_once()
    assert not batch.mock_calls


@pytest.mark.parametrize(
    "action, field",
    [
        (AuthzedAction.CREATE, "tables_to_keep"),
        (AuthzedAction.DELETE, "tables_to_delete"),
    ],
)
async def test_checks_registry_resource_type(infra_client, action, field):
    client, store, registry, view = infra_client
    from feast.labeling.label_view import LabelView

    grant(registry, LabelView, action)
    forged = LabelView(
        name=view.name,
        entities=[],
        source=view.batch_source,
        schema=[Field(name="value", dtype=Int64)],
    )
    response = await client.post(
        "/update-infra", json={field: [encode_infra_object(forged)]}
    )
    assert response.status_code == 403


async def test_delete_uses_server_owned_metadata(infra_client, monkeypatch):
    client, store, registry, view = infra_client
    grant(registry, FeatureView, AuthzedAction.DELETE)
    forged = view.__copy__()
    forged.entities = ["victim_entity_group"]
    update = Mock()
    monkeypatch.setattr(store._get_provider().online_store, "update", update)
    response = await client.post(
        "/update-infra", json={"tables_to_delete": [encode_infra_object(forged)]}
    )
    assert response.status_code == 200
    assert update.call_args.kwargs["tables_to_delete"][0].entities == view.entities


@pytest.mark.parametrize("scope, expected", [(FeatureView, 403), (Project, 200)])
async def test_missing_delete_target_requires_project_permission(
    infra_client, scope, expected
):
    client, store, registry, view = infra_client
    store.registry.delete_feature_view(view.name, project=store.project)
    grant(registry, scope, AuthzedAction.DELETE)
    response = await client.post(
        "/update-infra", json={"tables_to_delete": [encode_infra_object(view)]}
    )
    assert response.status_code == expected


async def test_protected_project_cannot_be_torn_down(infra_client):
    client, store, registry, _ = infra_client
    store.registry.apply_project(
        Project(name=store.project, tags={PROTECTED_PROJECT_TAG: "true"})
    )
    grant(registry, Project, AuthzedAction.DELETE)
    response = await client.post("/teardown-infra", json={})
    assert response.status_code == 403


@pytest.mark.parametrize("endpoint", ["update", "teardown"])
async def test_provisioning_rejects_other_project(infra_client, endpoint, monkeypatch):
    client, store, registry, _ = infra_client
    grant(registry, [FeatureView, Entity, Project], AuthzedAction.DELETE)
    online = store._get_provider().online_store
    operation = Mock()
    monkeypatch.setattr(online, endpoint, operation)
    response = await client.post(f"/{endpoint}-infra?project=other", json={})
    assert response.status_code == 400
    operation.assert_not_called()


@pytest.mark.parametrize("allowed", [False, True])
async def test_first_creation_authorizes_before_source_import(
    infra_client, allowed, monkeypatch
):
    client, store, registry, template = infra_client
    view = template.__copy__()
    view.name = "first_authenticated_creation"
    proto = view.to_proto()
    proto.spec.batch_source.type = DataSourceProto.CUSTOM_SOURCE
    source_type = (
        f"{FileSource.__module__}.{FileSource.__name__}"
        if allowed
        else "untrusted_module.CustomSource"
    )
    proto.spec.batch_source.data_source_class_type = source_type
    load_source = Mock(wraps=data_source.get_data_source_class_from_type)
    monkeypatch.setattr(data_source, "get_data_source_class_from_type", load_source)
    grant(
        registry,
        FeatureView if allowed else Entity,
        AuthzedAction.CREATE,
    )
    response = await client.post(
        "/update-infra?project=test",
        json={
            "tables_to_keep": [
                {
                    "type": "FeatureView",
                    "proto": base64.b64encode(proto.SerializeToString()).decode(
                        "ascii"
                    ),
                }
            ]
        },
    )
    assert response.status_code == (200 if allowed else 403), response.text
    row = (
        store._get_provider()
        .online_store._get_conn(store.config)
        .execute(
            "SELECT name FROM sqlite_master WHERE name='test_first_authenticated_creation'"
        )
        .fetchone()
    )
    assert bool(row) == allowed
    if allowed:
        load_source.assert_any_call(source_type)
    else:
        load_source.assert_not_called()
