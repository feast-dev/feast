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
