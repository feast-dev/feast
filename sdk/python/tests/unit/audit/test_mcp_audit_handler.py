"""Tests for MCP protocol-layer audit logging (handler wrapping).

Validates ``_wrap_call_tool_handler`` and ``_principal_from_mcp_context``
from ``feast.infra.mcp_servers.mcp_server``, using real ``FastApiMCP``
and ``mcp.types.CallToolRequest`` objects so the tests break when the
upstream library changes its handler dispatch conventions.
"""

import asyncio
import unittest
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

from fastapi import FastAPI

from feast.audit.audit_logger import (
    AuditEvent,
    AuditLogger,
    AuditSink,
    mcp_audit_request_id,
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


def _make_real_mcp(app: FastAPI | None = None):
    """Build a *real* ``FastApiMCP`` instance backed by a throwaway FastAPI app.

    The returned object has ``server.request_handlers[CallToolRequest]``
    populated by fastapi_mcp, mirroring the production code path.
    """
    from fastapi_mcp import FastApiMCP

    if app is None:
        app = FastAPI()

        @app.get("/health")
        async def health():
            return {"status": "ok"}

    mcp = FastApiMCP(app, name="test-feast", description="test")
    mcp.mount()
    return mcp


def _handler_key():
    from mcp.types import CallToolRequest

    return CallToolRequest


def _get_handler(mcp: Any):
    """Return the current CallToolRequest handler from the real mcp server."""
    return mcp.server.request_handlers[_handler_key()]


def _make_call_tool_request(
    tool_name: str = "some_tool", arguments: dict | None = None
):
    """Create a real ``CallToolRequest`` pydantic object."""
    from mcp.types import CallToolRequest, CallToolRequestParams

    return CallToolRequest(
        method="tools/call",
        params=CallToolRequestParams(name=tool_name, arguments=arguments or {}),
    )


class TestWrapCallToolHandler(unittest.TestCase):
    """Tests that exercise ``_wrap_call_tool_handler`` against a real
    ``FastApiMCP.server.request_handlers`` dict.
    """

    def test_successful_call_logs_success(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = _make_real_mcp()
        original = _get_handler(mcp)
        _wrap_call_tool_handler(mcp, audit)

        self.assertIsNot(_get_handler(mcp), original, "handler should be replaced")

        req = _make_call_tool_request("get_online_features")
        try:
            _run(_get_handler(mcp)(req))
        except Exception:
            pass

        self.assertEqual(len(sink.events), 1)
        event = sink.events[0]
        self.assertEqual(event.event_type, "mcp.tools.call")
        self.assertEqual(event.action.mcp_tool, "get_online_features")
        self.assertEqual(event.source.transport, "mcp-http")
        self.assertIsNotNone(event.duration_ms)
        self.assertGreaterEqual(event.duration_ms, 0)

    def test_handler_exception_logs_error(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = _make_real_mcp()

        async def exploding_handler(req):
            raise RuntimeError("tool exploded")

        mcp.server.request_handlers[_handler_key()] = exploding_handler
        _wrap_call_tool_handler(mcp, audit)

        req = _make_call_tool_request("failing_tool")
        with self.assertRaises(RuntimeError):
            _run(_get_handler(mcp)(req))

        self.assertEqual(len(sink.events), 1)
        event = sink.events[0]
        self.assertEqual(event.outcome, "error")
        self.assertEqual(event.action.mcp_tool, "failing_tool")
        self.assertIn("tool exploded", event.detail)

    def test_isError_result_logs_mcp_error(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = _make_real_mcp()

        async def error_handler(req):
            return SimpleNamespace(isError=True)

        mcp.server.request_handlers[_handler_key()] = error_handler
        _wrap_call_tool_handler(mcp, audit)

        req = _make_call_tool_request("bad_tool")
        _run(_get_handler(mcp)(req))

        self.assertEqual(len(sink.events), 1)
        self.assertEqual(sink.events[0].outcome, "mcp_error")

    def test_result_without_isError_attr_logs_success(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = _make_real_mcp()

        async def plain_handler(req):
            return {"content": "plain dict result"}

        mcp.server.request_handlers[_handler_key()] = plain_handler
        _wrap_call_tool_handler(mcp, audit)

        req = _make_call_tool_request("simple_tool")
        _run(_get_handler(mcp)(req))

        self.assertEqual(sink.events[0].outcome, "success")

    def test_no_handler_skips_wrapping(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = _make_real_mcp()
        del mcp.server.request_handlers[_handler_key()]

        _wrap_call_tool_handler(mcp, audit)
        self.assertNotIn(_handler_key(), mcp.server.request_handlers)

    def test_no_request_handlers_attr_is_safe(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = SimpleNamespace(server=SimpleNamespace())
        _wrap_call_tool_handler(mcp, audit)

    def test_params_none_uses_empty_tool_name(self):
        """When req.params is None the tool name defaults to empty string."""
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = _make_real_mcp()

        async def handler(req):
            return SimpleNamespace(isError=False)

        mcp.server.request_handlers[_handler_key()] = handler
        _wrap_call_tool_handler(mcp, audit)

        req = SimpleNamespace(params=None)
        _run(_get_handler(mcp)(req))

        self.assertEqual(sink.events[0].action.mcp_tool, "")

    def test_original_handler_result_is_returned(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        sentinel = object()

        mcp = _make_real_mcp()

        async def handler(req):
            return sentinel

        mcp.server.request_handlers[_handler_key()] = handler
        _wrap_call_tool_handler(mcp, audit)

        req = _make_call_tool_request("tool")
        result = _run(_get_handler(mcp)(req))
        self.assertIs(result, sentinel)

    def test_contextvar_propagates_request_id(self):
        """The wrapper sets mcp_audit_request_id so that AuditLoggingMiddleware
        on the internal REST call can reuse the same request_id."""
        sink = InMemorySink()
        audit = AuditLogger(sink)
        captured_ids: list[str | None] = []

        mcp = _make_real_mcp()

        async def handler(req):
            captured_ids.append(mcp_audit_request_id.get())
            return SimpleNamespace(isError=False)

        mcp.server.request_handlers[_handler_key()] = handler
        _wrap_call_tool_handler(mcp, audit)

        req = _make_call_tool_request("tool")
        _run(_get_handler(mcp)(req))

        self.assertEqual(len(captured_ids), 1)
        self.assertIsNotNone(captured_ids[0])
        self.assertEqual(captured_ids[0], sink.events[0].request_id)

    def test_contextvar_reset_after_call(self):
        """The ContextVar is cleaned up after the handler completes."""
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = _make_real_mcp()

        async def handler(req):
            return SimpleNamespace(isError=False)

        mcp.server.request_handlers[_handler_key()] = handler
        _wrap_call_tool_handler(mcp, audit)

        self.assertIsNone(mcp_audit_request_id.get())

        req = _make_call_tool_request("tool")
        _run(_get_handler(mcp)(req))

        self.assertIsNone(mcp_audit_request_id.get())

    def test_mcp_and_rest_events_share_request_id(self):
        """End-to-end: both mcp.tools.call and http.request events emitted
        during a single tool invocation share the same request_id."""
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = _make_real_mcp()

        async def handler(req):
            propagated = mcp_audit_request_id.get()
            audit.log_http_request(
                request_id=propagated or audit.new_request_id(),
                method="POST",
                path="/get-online-features",
                status_code=200,
            )
            return SimpleNamespace(isError=False)

        mcp.server.request_handlers[_handler_key()] = handler
        _wrap_call_tool_handler(mcp, audit)

        req = _make_call_tool_request("get_online_features")
        _run(_get_handler(mcp)(req))

        self.assertEqual(len(sink.events), 2)
        http_event = next(e for e in sink.events if e.event_type == "http.request")
        mcp_event = next(e for e in sink.events if e.event_type == "mcp.tools.call")
        self.assertEqual(http_event.request_id, mcp_event.request_id)

    def test_jsonrpc_id_from_request_context(self):
        """When request_context is available, jsonrpc_id is captured."""
        sink = InMemorySink()
        audit = AuditLogger(sink)

        mcp = _make_real_mcp()

        async def handler(req):
            return SimpleNamespace(isError=False)

        mcp.server.request_handlers[_handler_key()] = handler
        _wrap_call_tool_handler(mcp, audit)

        req = _make_call_tool_request("some_tool")
        _run(_get_handler(mcp)(req))

        # Without a live transport, request_context raises LookupError,
        # so jsonrpc_id should be None.
        self.assertIsNone(sink.events[0].jsonrpc_id)


class TestPrincipalFromMcpContext(unittest.TestCase):
    def test_extracts_auth_type_header(self):
        request = MagicMock()
        request.headers = {"x-feast-auth-type": "oidc", "authorization": "Bearer tok"}
        ctx = SimpleNamespace(request=request)

        server = SimpleNamespace(request_context=ctx)
        principal = _principal_from_mcp_context(server)
        self.assertEqual(principal.auth_type, "oidc")
        self.assertEqual(principal.username, "(authenticated)")

    def test_no_auth_header_returns_empty_username(self):
        request = MagicMock()
        request.headers = {}
        ctx = SimpleNamespace(request=request)

        server = SimpleNamespace(request_context=ctx)
        principal = _principal_from_mcp_context(server)
        self.assertEqual(principal.username, "")
        self.assertEqual(principal.auth_type, "")

    def test_no_request_returns_empty_principal(self):
        ctx = SimpleNamespace()
        server = SimpleNamespace(request_context=ctx)
        principal = _principal_from_mcp_context(server)
        self.assertEqual(principal.username, "")

    def test_lookup_error_returns_empty_principal(self):
        """When request_context ContextVar is not set, LookupError is raised."""

        class FakeServer:
            @property
            def request_context(self):
                raise LookupError("no context")

        principal = _principal_from_mcp_context(FakeServer())
        self.assertEqual(principal.username, "")

    def test_none_server_returns_empty_principal(self):
        principal = _principal_from_mcp_context(None)
        self.assertEqual(principal.username, "")

    def test_exception_in_headers_returns_empty_principal(self):
        request = MagicMock()
        request.headers = property(lambda self: (_ for _ in ()).throw(RuntimeError))
        ctx = SimpleNamespace(request=request)

        server = SimpleNamespace(request_context=ctx)
        principal = _principal_from_mcp_context(server)
        self.assertEqual(principal.username, "")
