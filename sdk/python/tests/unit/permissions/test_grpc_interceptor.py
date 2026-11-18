from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from feast.permissions.server.grpc import AuthInterceptor, _request_project
from feast.project import Project
from feast.protos.feast.registry import RegistryServer_pb2


@pytest.mark.parametrize(
    "request_message, expected_project",
    [
        (
            RegistryServer_pb2.ApplyProjectRequest(
                project=Project(name="my_project").to_proto()
            ),
            "my_project",
        ),
        (
            RegistryServer_pb2.GetEntityRequest(name="e", project="my_project"),
            "my_project",
        ),
        (RegistryServer_pb2.ListProjectsRequest(), None),
    ],
)
def test_request_project(request_message, expected_project):
    assert _request_project(request_message) == expected_project


def test_interceptor_sets_project_name_for_apply_project():
    sm = MagicMock()
    auth_manager = MagicMock()
    auth_manager.token_extractor.extract_access_token.return_value = "token"
    auth_manager.token_parser.user_details_from_access_token = AsyncMock(
        return_value=MagicMock()
    )
    seen_projects = []

    def behavior(request, context):
        seen_projects.append(sm.set_current_project.call_args.args[0])
        return "response"

    handler = grpc.unary_unary_rpc_method_handler(behavior)
    handler_call_details = MagicMock(
        invocation_metadata=[("authorization", "Bearer token")]
    )

    with (
        patch("feast.permissions.server.grpc.get_security_manager", return_value=sm),
        patch(
            "feast.permissions.server.grpc.get_auth_manager", return_value=auth_manager
        ),
    ):
        wrapped = AuthInterceptor().intercept_service(
            lambda details: handler, handler_call_details
        )
        request = RegistryServer_pb2.ApplyProjectRequest(
            project=Project(name="my_project").to_proto()
        )
        assert wrapped.unary_unary(request, MagicMock()) == "response"

    assert seen_projects == ["my_project"]
    sm.reset_current_project.assert_called_once()
