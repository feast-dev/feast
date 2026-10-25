"""Tests for :mod:`feast.mcp.client` -- the HTTP hop to Feast.

The status-code handling here is security-relevant: a 401 or 403 from Feast
returned as a *successful* tool result hands the caller (and the LLM behind
it) a denial payload dressed up as feature data.
"""

from __future__ import annotations

import httpx
import pytest

# The standalone MCP server ships behind the optional feast[mcp-server] extra.
pytest.importorskip("fastmcp", reason="feast[mcp-server] extra not installed")

from fastmcp.exceptions import ToolError  # noqa: E402

from feast.mcp.client import FeastClient  # noqa: E402


class TestHeaders:
    def test_token_is_sent_as_bearer(self, feast_backend):
        client = FeastClient("http://feast.invalid")
        assert client._headers("abc")["Authorization"] == "Bearer abc"

    def test_no_authorization_header_without_a_token(self, feast_backend):
        client = FeastClient("http://feast.invalid")
        assert "Authorization" not in client._headers(None)

    def test_content_type_is_always_json(self, feast_backend):
        client = FeastClient("http://feast.invalid")
        assert client._headers()["Content-Type"] == "application/json"

    def test_trailing_slash_is_stripped_from_base_url(self):
        assert FeastClient("http://feast.invalid/")._base_url == "http://feast.invalid"


class TestRequest:
    async def test_forwards_method_path_body_and_params(self, feast_backend):
        client = FeastClient("http://feast.invalid")
        await client.request(
            "POST",
            "/get-online-features",
            token="tok",
            json={"features": ["a:b"]},
            params={"project": "demo"},
        )
        call = feast_backend.last
        assert call.method == "POST"
        assert call.path == "/get-online-features"
        assert call.body == {"features": ["a:b"]}
        assert call.params == {"project": "demo"}
        assert call.bearer == "tok"

    async def test_returns_parsed_json(self, feast_backend):
        feast_backend.queue(200, {"results": [{"values": [0.5]}]})
        client = FeastClient("http://feast.invalid")
        assert await client.request("GET", "/x") == {"results": [{"values": [0.5]}]}

    async def test_returns_raw_text_for_non_json(self, feast_backend):
        feast_backend.queue(200, "pong", content_type="text/plain")
        client = FeastClient("http://feast.invalid")
        assert await client.request("GET", "/health") == "pong"


class TestErrorStatuses:
    """Every 4xx/5xx must surface as an MCP error, never as content."""

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 422, 429, 500, 503])
    async def test_error_status_raises(self, feast_backend, status):
        feast_backend.queue(status, {"detail": "nope"})
        client = FeastClient("http://feast.invalid")
        with pytest.raises(ToolError) as excinfo:
            await client.request("POST", "/get-online-features")
        assert str(status) in str(excinfo.value)

    async def test_message_carries_status_method_path_and_detail(self, feast_backend):
        feast_backend.queue(403, {"detail": "user lacks the reader role"})
        client = FeastClient("http://feast.invalid")
        with pytest.raises(ToolError) as excinfo:
            await client.request("POST", "/get-online-features")
        message = str(excinfo.value)
        assert "403" in message
        assert "POST /get-online-features" in message
        assert "user lacks the reader role" in message

    @pytest.mark.parametrize("key", ["detail", "message", "error"])
    async def test_detail_is_pulled_from_common_body_keys(self, feast_backend, key):
        feast_backend.queue(401, {key: "token expired"})
        client = FeastClient("http://feast.invalid")
        with pytest.raises(ToolError, match="token expired"):
            await client.request("GET", "/x")

    async def test_non_json_error_body_falls_back_to_text(self, feast_backend):
        feast_backend.queue(502, "feast is down", content_type="text/plain")
        client = FeastClient("http://feast.invalid")
        with pytest.raises(ToolError, match="feast is down"):
            await client.request("GET", "/x")

    async def test_oversized_error_body_is_truncated(self, feast_backend):
        feast_backend.queue(400, {"detail": "x" * 5000})
        client = FeastClient("http://feast.invalid")
        with pytest.raises(ToolError) as excinfo:
            await client.request("GET", "/x")
        # 500-char cap on the detail, plus the short prefix around it.
        assert len(str(excinfo.value)) < 700

    async def test_success_statuses_do_not_raise(self, feast_backend):
        for status in (200, 201, 202, 204):
            feast_backend.queue(status, {"ok": True})
            client = FeastClient("http://feast.invalid")
            await client.request("GET", "/x")


class TestTransportFailures:
    """Fault injection: Feast never answers."""

    async def test_connection_error_propagates(self, feast_backend):
        feast_backend.fail_with(
            httpx.ConnectError("[Errno 61] Connection refused", request=None)
        )
        client = FeastClient("http://feast.invalid", timeout=1.0)
        with pytest.raises(httpx.ConnectError):
            await client.request("GET", "/x")

    async def test_read_timeout_propagates(self, feast_backend):
        feast_backend.fail_with(httpx.ReadTimeout("timed out", request=None))
        client = FeastClient("http://feast.invalid", timeout=1.0)
        with pytest.raises(httpx.ReadTimeout):
            await client.request("GET", "/x")
