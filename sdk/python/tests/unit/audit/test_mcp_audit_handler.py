"""Tests for MCP protocol-layer audit logging (handler wrapping).

Validates ``_wrap_call_tool_handler`` and ``_principal_from_mcp_context``
from ``feast.infra.mcp_servers.mcp_server``.
"""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from feast.audit.audit_logger import (
    AuditEvent,
    AuditLogger,
    AuditSink,
)
from feast.infra.mcp_servers.mcp_server import (
    _principal_from_mcp_context,
    _wrap_call_tool_handler,
)


class InMemorySink(AuditSink):
    def __init__(self):
        self.events: list[AuditEvent] = []

    def emit(self, event: AuditEvent) -> None:
        self.events.append(event)


def _run(coro):
    """Helper to run an async function synchronously in tests."""
    return asyncio.get_event_loop().run_until_complete(coro)


def _make_fake_mcp(handler=None):
    """Build a fake FastApiMCP with a minimal ``server`` that has ``_request_handlers``."""

    async def default_handler(ctx, params):
        return SimpleNamespace(isError=False)

    handlers = {"tools/call": handler or default_handler}
    server = SimpleNamespace(_request_handlers=handlers)
    return SimpleNamespace(server=server)


class TestWrapCallToolHandler(unittest.TestCase):
    def test_successful_call_logs_success(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        async def handler(ctx, params):
            return SimpleNamespace(isError=False)

        mcp = _make_fake_mcp(handler)
        _wrap_call_tool_handler(mcp, audit)

        ctx = SimpleNamespace(request_id=42)
        params = SimpleNamespace(name="get_online_features")
        _run(mcp.server._request_handlers["tools/call"](ctx, params))

        self.assertEqual(len(sink.events), 1)
        event = sink.events[0]
        self.assertEqual(event.event_type, "mcp.tools.call")
        self.assertEqual(event.action.mcp_tool, "get_online_features")
        self.assertEqual(event.outcome, "success")
        self.assertEqual(event.jsonrpc_id, "42")
        self.assertEqual(event.source.transport, "mcp-http")
        self.assertIsNotNone(event.duration_ms)
        self.assertGreaterEqual(event.duration_ms, 0)

    def test_handler_exception_logs_error(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        async def handler(ctx, params):
            raise RuntimeError("tool exploded")

        mcp = _make_fake_mcp(handler)
        _wrap_call_tool_handler(mcp, audit)

        ctx = SimpleNamespace(request_id=7)
        params = SimpleNamespace(name="failing_tool")
        with self.assertRaises(RuntimeError):
            _run(mcp.server._request_handlers["tools/call"](ctx, params))

        self.assertEqual(len(sink.events), 1)
        event = sink.events[0]
        self.assertEqual(event.outcome, "error")
        self.assertEqual(event.action.mcp_tool, "failing_tool")
        self.assertIn("tool exploded", event.detail)

    def test_isError_result_logs_mcp_error(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        async def handler(ctx, params):
            return SimpleNamespace(isError=True)

        mcp = _make_fake_mcp(handler)
        _wrap_call_tool_handler(mcp, audit)

        ctx = SimpleNamespace(request_id=99)
        params = SimpleNamespace(name="bad_tool")
        _run(mcp.server._request_handlers["tools/call"](ctx, params))

        self.assertEqual(len(sink.events), 1)
        self.assertEqual(sink.events[0].outcome, "mcp_error")

    def test_result_without_isError_attr_logs_success(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        async def handler(ctx, params):
            return {"content": "plain dict result"}

        mcp = _make_fake_mcp(handler)
        _wrap_call_tool_handler(mcp, audit)

        ctx = SimpleNamespace(request_id=1)
        params = SimpleNamespace(name="simple_tool")
        _run(mcp.server._request_handlers["tools/call"](ctx, params))

        self.assertEqual(sink.events[0].outcome, "success")

    def test_no_handler_skips_wrapping(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        server = SimpleNamespace(_request_handlers={})
        mcp = SimpleNamespace(server=server)
        _wrap_call_tool_handler(mcp, audit)

        self.assertNotIn("tools/call", mcp.server._request_handlers)

    def test_no_request_handlers_attr_is_safe(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = SimpleNamespace(server=SimpleNamespace())
        _wrap_call_tool_handler(mcp, audit)

    def test_jsonrpc_id_from_ctx_request_id(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        async def handler(ctx, params):
            return SimpleNamespace(isError=False)

        mcp = _make_fake_mcp(handler)
        _wrap_call_tool_handler(mcp, audit)

        ctx = SimpleNamespace(request_id="req-abc-123")
        params = SimpleNamespace(name="some_tool")
        _run(mcp.server._request_handlers["tools/call"](ctx, params))

        self.assertEqual(sink.events[0].jsonrpc_id, "req-abc-123")

    def test_jsonrpc_id_none_when_ctx_has_no_request_id(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        async def handler(ctx, params):
            return SimpleNamespace(isError=False)

        mcp = _make_fake_mcp(handler)
        _wrap_call_tool_handler(mcp, audit)

        ctx = SimpleNamespace()
        params = SimpleNamespace(name="tool")
        _run(mcp.server._request_handlers["tools/call"](ctx, params))

        self.assertIsNone(sink.events[0].jsonrpc_id)

    def test_params_none_uses_empty_tool_name(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        async def handler(ctx, params):
            return SimpleNamespace(isError=False)

        mcp = _make_fake_mcp(handler)
        _wrap_call_tool_handler(mcp, audit)

        ctx = SimpleNamespace(request_id=1)
        _run(mcp.server._request_handlers["tools/call"](ctx, None))

        self.assertEqual(sink.events[0].action.mcp_tool, "")

    def test_original_handler_result_is_returned(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        sentinel = object()

        async def handler(ctx, params):
            return sentinel

        mcp = _make_fake_mcp(handler)
        _wrap_call_tool_handler(mcp, audit)

        ctx = SimpleNamespace(request_id=1)
        params = SimpleNamespace(name="tool")
        result = _run(mcp.server._request_handlers["tools/call"](ctx, params))
        self.assertIs(result, sentinel)


class TestPrincipalFromMcpContext(unittest.TestCase):
    def test_extracts_auth_type_header(self):
        request = MagicMock()
        request.headers = {"x-feast-auth-type": "oidc", "authorization": "Bearer tok"}
        ctx = SimpleNamespace(request=request)

        principal = _principal_from_mcp_context(ctx)
        self.assertEqual(principal.auth_type, "oidc")
        self.assertEqual(principal.username, "(authenticated)")

    def test_no_auth_header_returns_empty_username(self):
        request = MagicMock()
        request.headers = {}
        ctx = SimpleNamespace(request=request)

        principal = _principal_from_mcp_context(ctx)
        self.assertEqual(principal.username, "")
        self.assertEqual(principal.auth_type, "")

    def test_no_request_returns_empty_principal(self):
        ctx = SimpleNamespace()
        principal = _principal_from_mcp_context(ctx)
        self.assertEqual(principal.username, "")

    def test_none_ctx_returns_empty_principal(self):
        principal = _principal_from_mcp_context(None)
        self.assertEqual(principal.username, "")

    def test_exception_in_headers_returns_empty_principal(self):
        request = MagicMock()
        request.headers = property(lambda self: (_ for _ in ()).throw(RuntimeError))
        ctx = SimpleNamespace(request=request)

        principal = _principal_from_mcp_context(ctx)
        self.assertEqual(principal.username, "")
