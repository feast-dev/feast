from unittest.mock import MagicMock, Mock

from requests import Response

PROJECT_NAME = "test_registry_server"


def _mock_oidc(request, monkeypatch, client_id):
    async def mock_oath2(self, request):
        return "OK"

    monkeypatch.setattr(
        "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__",
        mock_oath2,
    )
    signing_key = MagicMock()
    signing_key.key = "a-key"
    monkeypatch.setattr(
        "feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt",
        lambda self, access_token: signing_key,
    )
    user_data = {
        "preferred_username": "my-name",
        "resource_access": {client_id: {"roles": ["reader", "writer"]}},
    }
    monkeypatch.setattr(
        "feast.permissions.auth.oidc_token_parser.jwt.decode",
        lambda self, *args, **kwargs: user_data,
    )
    discovery_response = Mock(spec=Response)
    discovery_response.status_code = 200
    discovery_response.json.return_value = {
        "token_endpoint": "http://localhost:8080/realms/master/protocol/openid-connect/token"
    }
    monkeypatch.setattr(
        "feast.permissions.client.oidc_authentication_client_manager.requests.get",
        lambda url: discovery_response,
    )
    token_response = Mock(spec=Response)
    token_response.status_code = 200
    token_response.json.return_value = {"access_token": "my-token"}
    monkeypatch.setattr(
        "feast.permissions.client.oidc_authentication_client_manager.requests.post",
        lambda url, data, headers: token_response,
    )


def _mock_kubernetes(request, monkeypatch):
    sa_name = request.getfixturevalue("sa_name")
    namespace = request.getfixturevalue("namespace")
    subject = f"system:serviceaccount:{namespace}:{sa_name}"
    rolebindings = request.getfixturevalue("rolebindings")
    clusterrolebindings = request.getfixturevalue("clusterrolebindings")

    monkeypatch.setattr(
        "feast.permissions.auth.kubernetes_token_parser.config.load_incluster_config",
        lambda: None,
    )
    monkeypatch.setattr(
        "feast.permissions.auth.kubernetes_token_parser.jwt.decode",
        lambda *args, **kwargs: {"sub": subject},
    )
    monkeypatch.setattr(
        "feast.permissions.auth.kubernetes_token_parser.client.RbacAuthorizationV1Api.list_namespaced_role_binding",
        lambda *args, **kwargs: rolebindings["items"],
    )
    monkeypatch.setattr(
        "feast.permissions.auth.kubernetes_token_parser.client.RbacAuthorizationV1Api.list_cluster_role_binding",
        lambda *args, **kwargs: clusterrolebindings["items"],
    )
    monkeypatch.setattr(
        "feast.permissions.client.kubernetes_auth_client_manager.KubernetesAuthClientManager.get_token",
        lambda self: "my-token",
    )
