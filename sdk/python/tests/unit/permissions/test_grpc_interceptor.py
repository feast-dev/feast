from unittest.mock import AsyncMock, MagicMock, Mock, patch

import grpc
import pytest

from feast.errors import FeastPermissionError, ProjectObjectNotFoundException
from feast.infra.registry.base_registry import BaseRegistry
from feast.permissions.action import ALL_ACTIONS
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.security_manager import (
    SecurityManager,
    assert_permissions_to_update,
    no_security_manager,
    set_security_manager,
)
from feast.permissions.server.grpc import AuthInterceptor, _request_project
from feast.permissions.user import User
from feast.project import Project
from feast.protos.feast.registry import RegistryServer_pb2

PROJECT = "my_project"


@pytest.mark.parametrize(
    "request_message, expected_project",
    [
        (
            RegistryServer_pb2.ApplyProjectRequest(
                project=Project(name=PROJECT).to_proto()
            ),
            PROJECT,
        ),
        # An unset message field reads as an empty Project: no name.
        (RegistryServer_pb2.ApplyProjectRequest(), ""),
        (
            RegistryServer_pb2.GetEntityRequest(name="e", project=PROJECT),
            PROJECT,
        ),
        (RegistryServer_pb2.ListProjectsRequest(), None),
    ],
)
def test_request_project(request_message, expected_project):
    assert _request_project(request_message) == expected_project


@pytest.fixture
def registry():
    """A registry that holds one permission, in the project PROJECT.

    Like the real registries, list_permissions returns only the permissions
    of the project it is called with.
    """
    permission = Permission(
        name="project_admin",
        types=Project,
        policy=RoleBasedPolicy(roles=["admin"]),
        actions=ALL_ACTIONS,
    )
    registry = Mock(spec=BaseRegistry)
    registry.list_permissions = Mock(
        side_effect=lambda project, **_: [permission] if project == PROJECT else []
    )
    registry.get_project = Mock(
        side_effect=ProjectObjectNotFoundException(name=PROJECT)
    )
    return registry


@pytest.fixture
def security_manager(registry):
    # The server runs for another project: the request must choose PROJECT.
    sm = SecurityManager(project="server_project", registry=registry)
    set_security_manager(sm)
    yield sm
    no_security_manager()


def _call(security_manager, user, behavior, request):
    """Send request through AuthInterceptor to behavior, as user."""
    auth_manager = MagicMock()
    auth_manager.token_extractor.extract_access_token.return_value = "token"
    auth_manager.token_parser.user_details_from_access_token = AsyncMock(
        return_value=user
    )
    handler = grpc.unary_unary_rpc_method_handler(behavior)
    with (
        patch(
            "feast.permissions.server.grpc.get_security_manager",
            return_value=security_manager,
        ),
        patch(
            "feast.permissions.server.grpc.get_auth_manager",
            return_value=auth_manager,
        ),
    ):
        wrapped = AuthInterceptor().intercept_service(
            lambda details: handler,
            MagicMock(invocation_metadata=[("authorization", "Bearer token")]),
        )
        return wrapped.unary_unary(request, MagicMock())


def _apply_project(registry):
    """The permission check of RegistryServer.ApplyProject."""

    def behavior(request, context):
        project = Project.from_proto(request.project)
        return assert_permissions_to_update(
            resource=project, getter=registry.get_project, project=project.name
        )

    return behavior


def _apply_project_request():
    return RegistryServer_pb2.ApplyProjectRequest(
        project=Project(name=PROJECT).to_proto()
    )


def test_apply_project_reads_permissions_of_the_project_name(
    security_manager, registry
):
    _call(
        security_manager,
        User("admin", ["admin"]),
        lambda request, context: security_manager.permissions,
        _apply_project_request(),
    )

    # With the Project message here, the SQL registry fails with
    # "cannot adapt type 'Project'".
    registry.list_permissions.assert_called_once_with(project=PROJECT)


def test_apply_project_permitted_by_a_permission_of_the_project(
    security_manager, registry
):
    project = _call(
        security_manager,
        User("admin", ["admin"]),
        _apply_project(registry),
        _apply_project_request(),
    )

    assert project.name == PROJECT


def test_apply_project_denied_without_a_permission_of_the_project(
    security_manager, registry
):
    # With the Project message as the project, the registry finds no
    # permission, and a request without permissions is allowed.
    with pytest.raises(FeastPermissionError):
        _call(
            security_manager,
            User("reader", ["reader"]),
            _apply_project(registry),
            _apply_project_request(),
        )
