"""Authorization tests for the standalone ``feast mcp`` server.

The MCP server enforces no RBAC of its own: it forwards the caller's bearer
token to Feast, which validates it and applies its permission model. Two
things therefore have to hold, and both are tested here end to end over a
real HTTP cycle (via ASGI, no socket):

1. the caller's token actually reaches the request Feast receives, and
2. a denial from Feast comes back as an MCP *error*, not as tool content.

These cannot be tested through FastMCP's in-memory client: it refuses an
``Authorization`` header outright, and with no auth provider configured the
access-token context is never populated -- so a mocked-out version of this
file would pass while the server silently dropped every token.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import patch

import pytest

# The standalone MCP server ships behind the optional feast[mcp-server] extra;
# py-key-value-aio (used by feast.mcp.auth) arrives with it transitively.
pytest.importorskip("fastmcp", reason="feast[mcp-server] extra not installed")
pytest.importorskip("key_value", reason="feast[mcp-server] extra not installed")

from fastmcp.exceptions import ToolError  # noqa: E402
from fastmcp.server.auth.providers.debug import DebugTokenVerifier  # noqa: E402

from feast.mcp import auth as auth_mod  # noqa: E402
from tests.unit.mcp.mcp_utils import build_mcp, mcp_over_http  # noqa: E402

VALID_TOKEN = "valid-user-token"

#: The epic's minimal tool matrix: one feature-server tool, one registry tool.
MATRIX_TOOLS = [
    (
        "features_get_online_features",
        {
            "features": ["driver_hourly_stats:conv_rate"],
            "entities": {"driver_id": [1001]},
        },
    ),
    ("registry_list_feature_views", {"project": "demo"}),
]
MATRIX_IDS = [name for name, _ in MATRIX_TOOLS]


class TestPassthroughForwarding:
    """Default mode: no auth provider, token relayed verbatim."""

    @pytest.mark.parametrize("tool,args", MATRIX_TOOLS, ids=MATRIX_IDS)
    async def test_caller_token_reaches_feast(self, feast_backend, tool, args):
        # Regression test: with no auth provider FastMCP never populates the
        # access-token context, so reading it alone drops the token entirely.
        async with mcp_over_http(
            build_mcp(),
            request_headers={"Authorization": f"Bearer {VALID_TOKEN}"},
        ) as client:
            await client.call_tool(tool, args)
        assert feast_backend.last.bearer == VALID_TOKEN

    @pytest.mark.parametrize("tool,args", MATRIX_TOOLS, ids=MATRIX_IDS)
    async def test_no_token_sends_no_authorization_header(
        self, feast_backend, tool, args
    ):
        async with mcp_over_http(build_mcp()) as client:
            await client.call_tool(tool, args)
        assert feast_backend.last.authorization is None

    async def test_non_bearer_scheme_is_not_forwarded(self, feast_backend):
        async with mcp_over_http(
            build_mcp(), request_headers={"Authorization": "Basic dXNlcjpwdw=="}
        ) as client:
            await client.call_tool("registry_list_feature_views", {"project": "demo"})
        assert feast_backend.last.bearer is None


class TestProviderConfigured:
    """With an auth provider set, FastMCP validates before the tool runs."""

    @staticmethod
    def _server():
        return build_mcp(
            auth=DebugTokenVerifier(validate=lambda token: token == VALID_TOKEN)
        )

    async def test_valid_token_is_forwarded(self, feast_backend):
        async with mcp_over_http(
            self._server(), request_headers={"Authorization": f"Bearer {VALID_TOKEN}"}
        ) as client:
            await client.call_tool("registry_list_feature_views", {"project": "demo"})
        assert feast_backend.last.bearer == VALID_TOKEN

    async def test_invalid_token_never_reaches_the_tool(self, feast_backend):
        with pytest.raises(Exception):
            async with mcp_over_http(
                self._server(),
                request_headers={"Authorization": "Bearer forged-token"},
            ) as client:
                await client.call_tool(
                    "registry_list_feature_views", {"project": "demo"}
                )
        assert feast_backend.call_count == 0

    async def test_missing_token_never_reaches_the_tool(self, feast_backend):
        with pytest.raises(Exception):
            async with mcp_over_http(self._server()) as client:
                await client.call_tool(
                    "registry_list_feature_views", {"project": "demo"}
                )
        assert feast_backend.call_count == 0


class TestKubernetesTokenVerifier:
    """`--auth-mode kubernetes` uses Feast's own Kubernetes auth.

    These tests check that nothing is rewritten here. The verifier gives the
    token to the same ``KubernetesTokenParser`` the Feast feature server and
    registry server use, then turns the ``User`` it gets back into what
    FastMCP expects. A fake parser stands in for the Token Access Review API,
    so these tests do not need a real cluster.
    """

    @staticmethod
    def _verifier(user=None, error=None):
        from feast.permissions.user import User

        class FakeKubernetesParser:
            """Stands in for Feast's KubernetesTokenParser."""

            def __init__(self):
                self.seen: list[str] = []

            async def user_details_from_access_token(self, access_token: str):
                self.seen.append(access_token)
                if error is not None:
                    raise error
                return user or User(
                    username="default:feast-sa",
                    roles=["feast-reader"],
                    groups=["system:serviceaccounts"],
                    namespaces=["default"],
                )

        parser = FakeKubernetesParser()
        return auth_mod.KubernetesTokenVerifier(parser=parser), parser

    async def test_valid_token_is_resolved_through_the_feast_parser(self):
        verifier, parser = self._verifier()

        access = await verifier.verify_token(VALID_TOKEN)

        assert parser.seen == [VALID_TOKEN], "the parser must see the raw token"
        assert access is not None
        assert access.subject == "default:feast-sa"
        assert access.claims["roles"] == ["feast-reader"]
        assert access.claims["groups"] == ["system:serviceaccounts"]
        assert access.claims["namespaces"] == ["default"]

    async def test_the_verified_token_is_relayed_verbatim(self):
        # Feast must still receive the caller's own token, so that it can
        # apply the permission model to the real user.
        verifier, _ = self._verifier()

        access = await verifier.verify_token(VALID_TOKEN)

        assert access is not None
        assert access.token == VALID_TOKEN
        assert auth_mod.get_auth_token.__doc__  # choke point still in play

    async def test_rejected_token_returns_none(self):
        from starlette.authentication import AuthenticationError

        verifier, _ = self._verifier(error=AuthenticationError("bad token"))

        assert await verifier.verify_token("forged-token") is None

    async def test_token_review_failure_is_not_an_accepted_token(self):
        # If the API server cannot answer, the request must be rejected,
        # not let through. Feast's own REST path does the same.
        verifier, _ = self._verifier(error=RuntimeError("apiserver unreachable"))

        assert await verifier.verify_token(VALID_TOKEN) is None

    async def test_client_id_records_the_auth_mode(self):
        verifier, _ = self._verifier()

        access = await verifier.verify_token(VALID_TOKEN)

        assert access is not None
        assert access.client_id == "kubernetes"
        assert "kubernetes" in auth_mod._describe_user(access)

    def test_default_parser_reports_a_missing_kubernetes_client(self):
        # feast[k8s] is optional, so the import itself can fail. The error
        # should name the missing extra, not just say ModuleNotFoundError.
        with patch.dict(
            sys.modules, {"feast.permissions.auth.kubernetes_token_parser": None}
        ):
            with pytest.raises(RuntimeError) as excinfo:
                auth_mod.KubernetesTokenVerifier()

        message = str(excinfo.value)
        assert "--auth-mode kubernetes" in message
        assert "feast[k8s]" in message

    def test_default_parser_explains_itself_outside_a_cluster(self):
        # The parser is built at startup. If the in-cluster config is
        # missing, the error must say so, instead of showing up later as a
        # plain 401 on the first tool call.
        module = types.ModuleType("feast.permissions.auth.kubernetes_token_parser")

        class NeedsACluster:
            def __init__(self):
                raise RuntimeError("Service host/port is not set")

        module.KubernetesTokenParser = NeedsACluster  # type: ignore[attr-defined]
        with patch.dict(
            sys.modules, {"feast.permissions.auth.kubernetes_token_parser": module}
        ):
            with pytest.raises(RuntimeError) as excinfo:
                auth_mod.KubernetesTokenVerifier()

        message = str(excinfo.value)
        assert "--auth-mode kubernetes" in message
        assert "inside a Kubernetes cluster" in message
        assert "Service host/port is not set" in message

    def test_the_cli_exposes_the_mode(self):
        # Checks the other half of the wiring. If the config knows the mode
        # but the CLI rejects it, nobody can use it.
        from feast.mcp.server import mcp_cli

        auth_mode = next(p for p in mcp_cli.params if p.name == "auth_mode")
        assert "kubernetes" in auth_mode.type.choices  # type: ignore[attr-defined]

    async def test_valid_token_reaches_feast_end_to_end(self, feast_backend):
        verifier, _ = self._verifier()
        async with mcp_over_http(
            build_mcp(auth=verifier),
            request_headers={"Authorization": f"Bearer {VALID_TOKEN}"},
        ) as client:
            await client.call_tool("registry_list_feature_views", {"project": "demo"})

        assert feast_backend.last.bearer == VALID_TOKEN

    async def test_rejected_token_never_reaches_feast_end_to_end(self, feast_backend):
        from starlette.authentication import AuthenticationError

        verifier, _ = self._verifier(error=AuthenticationError("bad token"))
        with pytest.raises(Exception):
            async with mcp_over_http(
                build_mcp(auth=verifier),
                request_headers={"Authorization": "Bearer forged-token"},
            ) as client:
                await client.call_tool(
                    "registry_list_feature_views", {"project": "demo"}
                )

        assert feast_backend.call_count == 0


class TestFeastDenialRelay:
    """Feast's 401/403 must arrive as an MCP error, not as feature data."""

    @pytest.mark.parametrize("status", [401, 403])
    @pytest.mark.parametrize("tool,args", MATRIX_TOOLS, ids=MATRIX_IDS)
    async def test_denial_becomes_a_tool_error(self, feast_backend, status, tool, args):
        feast_backend.queue(status, {"detail": "permission denied"})
        async with mcp_over_http(
            build_mcp(), request_headers={"Authorization": f"Bearer {VALID_TOKEN}"}
        ) as client:
            with pytest.raises(ToolError) as excinfo:
                await client.call_tool(tool, args)
        message = str(excinfo.value)
        assert str(status) in message
        assert "permission denied" in message

    async def test_denial_is_not_returned_as_successful_content(self, feast_backend):
        feast_backend.queue(401, {"detail": "missing token"})
        async with mcp_over_http(build_mcp()) as client:
            with pytest.raises(ToolError):
                result = await client.call_tool(
                    "features_get_online_features",
                    {"features": ["a:b"], "entities": {"driver_id": [1]}},
                )
                pytest.fail(f"denial returned as success: {result}")


class TestRawBearerToken:
    """Unit coverage for the header parser behind passthrough forwarding."""

    @staticmethod
    def _request(headers: dict):
        # A real Starlette request, because Feast's RestTokenExtractor
        # checks the type of what it is given.
        from starlette.requests import Request

        return Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/mcp",
                "query_string": b"",
                "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
            }
        )

    @pytest.mark.parametrize(
        "header,expected",
        [
            ("Bearer abc123", "abc123"),
            ("bearer abc123", "abc123"),
            ("BEARER abc123", "abc123"),
            ("Bearer   abc123  ", "abc123"),
            ("Basic dXNlcjpwdw==", None),
            ("abc123", None),
            ("Bearer ", None),
            ("", None),
        ],
    )
    def test_header_parsing(self, header, expected):
        request = self._request({"authorization": header} if header else {})
        with patch.object(auth_mod, "get_http_request", return_value=request):
            assert auth_mod._raw_bearer_token() == expected

    def test_returns_none_outside_an_http_request(self):
        # stdio transport: there is no request to read a header from.
        with patch.object(
            auth_mod, "get_http_request", side_effect=RuntimeError("no request")
        ):
            assert auth_mod._raw_bearer_token() is None


class TestGetAuthToken:
    """The choke point every tool calls."""

    class _Token:
        token = "token-from-provider"
        claims = {"preferred_username": "alice"}
        client_id = "feast-mcp"

    def test_prefers_the_validated_access_token(self):
        with (
            patch.object(auth_mod, "get_access_token", return_value=self._Token()),
            patch.object(auth_mod, "get_http_request", side_effect=RuntimeError),
        ):
            assert auth_mod.get_auth_token() == "token-from-provider"

    def test_falls_back_to_the_raw_header(self):
        request = TestRawBearerToken._request({"authorization": "Bearer raw-token"})
        with (
            patch.object(auth_mod, "get_access_token", return_value=None),
            patch.object(auth_mod, "get_http_request", return_value=request),
        ):
            assert auth_mod.get_auth_token() == "raw-token"

    def test_returns_none_when_there_is_no_token_anywhere(self):
        with (
            patch.object(auth_mod, "get_access_token", return_value=None),
            patch.object(auth_mod, "get_http_request", side_effect=RuntimeError),
        ):
            assert auth_mod.get_auth_token() is None

    def test_does_not_log_the_token_itself(self, caplog):
        request = TestRawBearerToken._request({"authorization": "Bearer s3cr3t"})
        with (
            patch.object(auth_mod, "get_access_token", return_value=None),
            patch.object(auth_mod, "get_http_request", return_value=request),
        ):
            with caplog.at_level("INFO"):
                auth_mod.get_auth_token()
        assert "s3cr3t" not in caplog.text


class TestDescribeUser:
    """Identity used in the audit log line, in claim-precedence order."""

    @staticmethod
    def _token(claims, client_id=None):
        class FakeToken:
            pass

        token = FakeToken()
        token.claims = claims
        token.client_id = client_id
        return token

    def test_prefers_preferred_username(self):
        described = auth_mod._describe_user(
            self._token({"preferred_username": "alice", "email": "a@x", "sub": "s"})
        )
        assert "alice" in described

    def test_falls_back_to_email_then_sub(self):
        assert "a@x" in auth_mod._describe_user(
            self._token({"email": "a@x", "sub": "s"})
        )
        assert "s" in auth_mod._describe_user(self._token({"sub": "s"}))

    def test_unknown_when_no_claims(self):
        assert auth_mod._describe_user(self._token({})) == "unknown"

    def test_includes_client_id_when_present(self):
        described = auth_mod._describe_user(
            self._token({"sub": "s"}, client_id="feast-mcp")
        )
        assert "client_id=feast-mcp" in described


class TestRequestContext:
    """Client IP resolution for the audit log: the socket peer only."""

    @staticmethod
    def _request(headers, client_host="10.0.0.9", method="POST", path="/mcp"):
        class FakeClient:
            host = client_host

        class FakeURL:
            pass

        url = FakeURL()
        url.path = path

        class FakeRequest:
            pass

        request = FakeRequest()
        request.headers = headers
        request.client = FakeClient()
        request.method = method
        request.url = url
        return request

    def test_forwarding_headers_are_ignored(self):
        # Any caller can set these, so they must not reach the audit log.
        request = self._request(
            {"x-forwarded-for": "203.0.113.5, 70.41.3.18", "x-real-ip": "203.0.113.7"}
        )
        with patch.object(auth_mod, "get_http_request", return_value=request):
            ip, where = auth_mod._request_context()
        assert ip == "10.0.0.9"
        assert where == "POST /mcp"

    def test_socket_peer_is_the_client_ip(self):
        with patch.object(auth_mod, "get_http_request", return_value=self._request({})):
            ip, _ = auth_mod._request_context()
        assert ip == "10.0.0.9"

    def test_returns_nones_outside_an_http_request(self):
        with patch.object(
            auth_mod, "get_http_request", side_effect=RuntimeError("stdio")
        ):
            assert auth_mod._request_context() == (None, None)
