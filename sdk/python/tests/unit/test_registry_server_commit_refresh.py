from unittest.mock import Mock

import pytest
from google.protobuf.empty_pb2 import Empty

from feast.errors import FeastPermissionError, ProjectObjectNotFoundException
from feast.infra.infra_object import Infra
from feast.permissions.permission import AuthzedAction, Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.security_manager import (
    SecurityManager,
    no_security_manager,
    set_security_manager,
)
from feast.permissions.user import User
from feast.project import Project
from feast.protos.feast.registry import RegistryServer_pb2
from feast.registry_server import RegistryServer


@pytest.fixture
def project_permissions():
    return [
        Permission(
            name="project_updater",
            types=Project,
            name_patterns="team-a.*",
            policy=RoleBasedPolicy(roles=["team-a"]),
            actions=[AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
        ),
        Permission(
            name="project_creator",
            types=Project,
            name_patterns="team-a.*",
            policy=RoleBasedPolicy(roles=["team-a"]),
            actions=[AuthzedAction.CREATE],
        ),
        Permission(
            name="project_a_create_only",
            types=Project,
            name_patterns="team-a.*",
            policy=RoleBasedPolicy(roles=["team-a-creator"]),
            actions=[AuthzedAction.CREATE],
        ),
        Permission(
            name="project_b_updater",
            types=Project,
            name_patterns="team-b.*",
            policy=RoleBasedPolicy(roles=["team-b"]),
            actions=[AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
        ),
    ]


@pytest.fixture
def auth_security_manager(project_permissions):
    registry = Mock()
    registry.list_permissions = Mock(return_value=project_permissions)
    sm = SecurityManager(project="any", registry=registry)
    set_security_manager(sm)
    yield sm
    no_security_manager()


def _server_with_projects(*project_names: str) -> tuple[RegistryServer, Mock]:
    registry = Mock()
    projects = [Project(name=name) for name in project_names]
    registry.list_projects = Mock(return_value=projects)

    def get_project(name: str, allow_cache: bool = False):
        for project in projects:
            if project.name == name:
                return project
        raise ProjectObjectNotFoundException(name=name)

    registry.get_project = Mock(side_effect=get_project)
    registry.refresh = Mock()
    registry.commit = Mock()
    registry.get_infra = Mock(return_value=Infra())
    registry.update_infra = Mock()
    return RegistryServer(registry=registry), registry


def test_refresh_allows_missing_project_without_auth():
    no_security_manager()
    server, registry = _server_with_projects("existing")

    response = server.Refresh(
        RegistryServer_pb2.RefreshRequest(project="brand-new"), None
    )

    assert response == Empty()
    registry.refresh.assert_called_once_with("brand-new")


def test_refresh_allows_missing_project_with_create_permission(auth_security_manager):
    auth_security_manager.set_current_user(User("alice", ["team-a"]))
    server, registry = _server_with_projects("team-b-project")

    response = server.Refresh(
        RegistryServer_pb2.RefreshRequest(project="team-a-new"), None
    )

    assert response == Empty()
    registry.refresh.assert_called_once_with("team-a-new")


def test_refresh_denies_missing_project_without_create_permission(
    auth_security_manager,
):
    auth_security_manager.set_current_user(User("bob", ["team-b"]))
    server, _ = _server_with_projects("team-b-project")

    with pytest.raises(FeastPermissionError):
        server.Refresh(RegistryServer_pb2.RefreshRequest(project="team-a-new"), None)


def test_refresh_requires_update_for_existing_project(auth_security_manager):
    auth_security_manager.set_current_user(User("alice", ["team-a"]))
    server, registry = _server_with_projects("team-a-project", "team-b-project")

    response = server.Refresh(
        RegistryServer_pb2.RefreshRequest(project="team-a-project"), None
    )
    assert response == Empty()
    registry.refresh.assert_called_once_with("team-a-project")

    with pytest.raises(FeastPermissionError):
        server.Refresh(
            RegistryServer_pb2.RefreshRequest(project="team-b-project"), None
        )


def test_commit_allows_when_caller_can_update_one_shared_project(auth_security_manager):
    auth_security_manager.set_current_user(User("alice", ["team-a"]))
    server, registry = _server_with_projects("team-a-project", "team-b-project")

    response = server.Commit(Empty(), None)

    assert response == Empty()
    registry.commit.assert_called_once()


def test_commit_denies_when_caller_cannot_create_or_update_any_project(
    auth_security_manager,
):
    auth_security_manager.set_current_user(User("nobody", ["other"]))
    server, _ = _server_with_projects("team-a-project", "team-b-project")

    with pytest.raises(FeastPermissionError):
        server.Commit(Empty(), None)


def test_commit_allows_without_auth():
    no_security_manager()
    server, registry = _server_with_projects("a", "b")

    response = server.Commit(Empty(), None)

    assert response == Empty()
    registry.commit.assert_called_once()


def test_commit_allows_empty_registry_with_auth(auth_security_manager):
    # No projects yet — same as old behavior (empty permission loop).
    auth_security_manager.set_current_user(User("nobody", ["other"]))
    server, registry = _server_with_projects()

    response = server.Commit(Empty(), None)

    assert response == Empty()
    registry.commit.assert_called_once()


def test_refresh_existing_project_without_auth():
    no_security_manager()
    server, registry = _server_with_projects("existing")

    response = server.Refresh(
        RegistryServer_pb2.RefreshRequest(project="existing"), None
    )

    assert response == Empty()
    registry.refresh.assert_called_once_with("existing")


def test_commit_allows_with_only_create_permission_on_new_project(
    auth_security_manager,
):
    # After ApplyProject(commit=False), project exists in cache but caller may
    # only have CREATE (not UPDATE) for that first apply transaction.
    auth_security_manager.set_current_user(User("creator", ["team-a-creator"]))
    server, registry = _server_with_projects("team-a-new")

    response = server.Commit(Empty(), None)

    assert response == Empty()
    registry.commit.assert_called_once()


def test_get_infra_allows_missing_project_without_auth():
    no_security_manager()
    server, registry = _server_with_projects("existing")

    response = server.GetInfra(
        RegistryServer_pb2.GetInfraRequest(project="brand-new", allow_cache=True),
        None,
    )

    assert response == Infra().to_proto()
    registry.get_infra.assert_called_once_with(project="brand-new", allow_cache=True)


def test_get_infra_allows_missing_project_with_create_permission(auth_security_manager):
    auth_security_manager.set_current_user(User("creator", ["team-a-creator"]))
    server, registry = _server_with_projects("team-b-project")

    response = server.GetInfra(
        RegistryServer_pb2.GetInfraRequest(project="team-a-new", allow_cache=True),
        None,
    )

    assert response == Infra().to_proto()
    registry.get_infra.assert_called_once_with(project="team-a-new", allow_cache=True)


def test_get_infra_denies_missing_project_without_create_permission(
    auth_security_manager,
):
    auth_security_manager.set_current_user(User("bob", ["team-b"]))
    server, _ = _server_with_projects("team-b-project")

    with pytest.raises(FeastPermissionError):
        server.GetInfra(
            RegistryServer_pb2.GetInfraRequest(project="team-a-new", allow_cache=True),
            None,
        )


def test_get_infra_requires_describe_for_existing_project(auth_security_manager):
    auth_security_manager.set_current_user(User("alice", ["team-a"]))
    server, registry = _server_with_projects("team-a-project", "team-b-project")

    response = server.GetInfra(
        RegistryServer_pb2.GetInfraRequest(project="team-a-project", allow_cache=True),
        None,
    )
    assert response == Infra().to_proto()
    registry.get_infra.assert_called_once_with(
        project="team-a-project", allow_cache=True
    )

    with pytest.raises(FeastPermissionError):
        server.GetInfra(
            RegistryServer_pb2.GetInfraRequest(
                project="team-b-project", allow_cache=True
            ),
            None,
        )


def test_update_infra_allows_missing_project_with_create_permission(
    auth_security_manager,
):
    auth_security_manager.set_current_user(User("creator", ["team-a-creator"]))
    server, registry = _server_with_projects("team-b-project")

    response = server.UpdateInfra(
        RegistryServer_pb2.UpdateInfraRequest(
            infra=Infra().to_proto(), project="team-a-new", commit=True
        ),
        None,
    )

    assert response == Empty()
    registry.update_infra.assert_called_once()


def test_registry_server_writes_through_for_file_registry(tmp_path):
    """Issue #6663: File Registry must persist each RPC immediately even when commit=False."""
    from feast import FileSource
    from feast.entity import Entity
    from feast.infra.registry.registry import Registry
    from feast.repo_config import RegistryConfig
    from feast.value_type import ValueType

    no_security_manager()
    registry_path = tmp_path / "registry.db"
    config = RegistryConfig(path=str(registry_path), cache_ttl_seconds=60)
    registry = Registry("test_project", config, None)
    registry.apply_entity(
        Entity(name="stale_entity", join_keys=["id"], value_type=ValueType.INT64),
        project="test_project",
        commit=True,
    )

    server = RegistryServer(registry=registry)

    new_entity = Entity(
        name="fresh_entity", join_keys=["id"], value_type=ValueType.INT64
    )
    server.ApplyEntity(
        RegistryServer_pb2.ApplyEntityRequest(
            entity=new_entity.to_proto(), project="test_project", commit=False
        ),
        None,
    )
    # Delete* performs an uncached read (allow_cache=False) that previously discarded
    # uncommitted commit=False applies still sitting only in cached_registry_proto.
    server.DeleteEntity(
        RegistryServer_pb2.DeleteEntityRequest(
            name="stale_entity", project="test_project", commit=False
        ),
        None,
    )

    source = FileSource(
        name="fresh_source",
        path="data.parquet",
    )
    server.ApplyDataSource(
        RegistryServer_pb2.ApplyDataSourceRequest(
            data_source=source.to_proto(), project="test_project", commit=False
        ),
        None,
    )

    reloaded = Registry("test_project", config, None)
    assert [
        e.name
        for e in reloaded.list_entities(project="test_project", allow_cache=False)
    ] == ["fresh_entity"]
    assert [
        s.name
        for s in reloaded.list_data_sources(project="test_project", allow_cache=False)
    ] == ["fresh_source"]


def test_registry_server_writes_through_all_delete_handlers():
    """Every Delete* handler must force commit=True when proxying a file Registry."""
    from unittest.mock import patch

    from feast.infra.registry.registry import Registry
    from feast.repo_config import RegistryConfig

    no_security_manager()
    registry = object.__new__(Registry)
    registry.config = RegistryConfig(path="registry.db")

    server = RegistryServer(registry=registry)

    with (
        patch.object(registry, "get_entity", return_value=Mock()),
        patch.object(registry, "delete_entity") as mock_del_entity,
        patch.object(registry, "get_data_source", return_value=Mock()),
        patch.object(registry, "delete_data_source") as mock_del_ds,
        patch.object(registry, "get_any_feature_view", return_value=Mock()),
        patch.object(registry, "delete_feature_view") as mock_del_fv,
        patch.object(registry, "get_feature_service", return_value=Mock()),
        patch.object(registry, "delete_feature_service") as mock_del_fs,
        patch.object(registry, "get_saved_dataset", return_value=Mock()),
        patch.object(registry, "delete_saved_dataset") as mock_del_sd,
        patch.object(registry, "get_validation_reference", return_value=Mock()),
        patch.object(registry, "delete_validation_reference") as mock_del_vr,
        patch.object(registry, "get_permission", return_value=Mock()),
        patch.object(registry, "delete_permission") as mock_del_perm,
        patch.object(registry, "get_project", return_value=Mock()),
        patch.object(registry, "delete_project") as mock_del_proj,
    ):
        server.DeleteEntity(
            RegistryServer_pb2.DeleteEntityRequest(name="e", project="p", commit=False),
            None,
        )
        server.DeleteDataSource(
            RegistryServer_pb2.DeleteDataSourceRequest(
                name="ds", project="p", commit=False
            ),
            None,
        )
        server.DeleteFeatureView(
            RegistryServer_pb2.DeleteFeatureViewRequest(
                name="fv", project="p", commit=False
            ),
            None,
        )
        server.DeleteFeatureService(
            RegistryServer_pb2.DeleteFeatureServiceRequest(
                name="fs", project="p", commit=False
            ),
            None,
        )
        server.DeleteSavedDataset(
            RegistryServer_pb2.DeleteSavedDatasetRequest(
                name="sd", project="p", commit=False
            ),
            None,
        )
        server.DeleteValidationReference(
            RegistryServer_pb2.DeleteValidationReferenceRequest(
                name="vr", project="p", commit=False
            ),
            None,
        )
        server.DeletePermission(
            RegistryServer_pb2.DeletePermissionRequest(
                name="perm", project="p", commit=False
            ),
            None,
        )
        server.DeleteProject(
            RegistryServer_pb2.DeleteProjectRequest(name="proj", commit=False), None
        )

    for mock_del in (
        mock_del_entity,
        mock_del_ds,
        mock_del_fv,
        mock_del_fs,
        mock_del_sd,
        mock_del_vr,
        mock_del_perm,
        mock_del_proj,
    ):
        assert mock_del.call_args.kwargs["commit"] is True


def test_registry_server_preserves_commit_flag_for_non_file_registry():
    """Non-Registry backends (e.g., SqlRegistry/SnowflakeRegistry) receive request.commit unchanged."""
    from unittest.mock import patch

    from feast.entity import Entity
    from feast.infra.registry.sql import SqlRegistry
    from feast.value_type import ValueType

    no_security_manager()
    sql_registry = object.__new__(SqlRegistry)
    server = RegistryServer(registry=sql_registry)

    entity = Entity(name="my_entity", join_keys=["id"], value_type=ValueType.INT64)
    with (
        patch.object(sql_registry, "apply_entity") as mock_apply,
        patch.object(sql_registry, "get_entity", return_value=entity),
        patch.object(sql_registry, "delete_entity") as mock_delete,
    ):
        server.ApplyEntity(
            RegistryServer_pb2.ApplyEntityRequest(
                entity=entity.to_proto(), project="p", commit=False
            ),
            None,
        )
        server.DeleteEntity(
            RegistryServer_pb2.DeleteEntityRequest(
                name="my_entity", project="p", commit=False
            ),
            None,
        )

    assert mock_apply.call_args.kwargs["commit"] is False
    assert mock_delete.call_args.kwargs["commit"] is False
