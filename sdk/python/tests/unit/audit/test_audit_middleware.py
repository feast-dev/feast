import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from feast.audit.audit_logger import (
    AuditEvent,
    AuditLogger,
    AuditSink,
)
from feast.audit.audit_middleware import AuditLoggingMiddleware, McpAuditMiddleware


class InMemorySink(AuditSink):
    def __init__(self):
        self.events: list[AuditEvent] = []

    def emit(self, event: AuditEvent) -> None:
        self.events.append(event)


def _make_app(audit_logger=None):
    """Create a minimal FastAPI app with audit middleware for testing."""
    app = FastAPI()
    app.state.audit_logger = audit_logger

    app.add_middleware(McpAuditMiddleware)
    app.add_middleware(AuditLoggingMiddleware)

    @app.post("/get-online-features")
    async def get_online_features():
        return {"result": "ok"}

    @app.post("/push")
    async def push():
        return {"result": "ok"}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/mcp")
    async def mcp():
        return {"jsonrpc": "2.0", "result": "ok"}

    @app.post("/error-endpoint")
    async def error_endpoint():
        raise ValueError("test error")

    return app


class TestAuditLoggingMiddleware(unittest.TestCase):
    def test_logs_http_request(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        resp = client.post("/get-online-features")
        self.assertEqual(resp.status_code, 200)

        http_events = [e for e in sink.events if e.event_type == "http.request"]
        self.assertEqual(len(http_events), 1)
        event = http_events[0]
        self.assertEqual(event.outcome, "success")
        self.assertIn("/get-online-features", event.detail)
        self.assertIsNotNone(event.duration_ms)
        self.assertGreaterEqual(event.duration_ms, 0)

    def test_skips_health_endpoint(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        client.get("/health")
        http_events = [e for e in sink.events if e.event_type == "http.request"]
        self.assertEqual(len(http_events), 0)

    def test_skips_mcp_endpoint(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        client.post("/mcp", json={"jsonrpc": "2.0", "method": "tools/list"})
        http_events = [e for e in sink.events if e.event_type == "http.request"]
        self.assertEqual(len(http_events), 0)

    def test_logs_failure_on_error(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        resp = client.post("/error-endpoint")
        self.assertEqual(resp.status_code, 500)

        http_events = [e for e in sink.events if e.event_type == "http.request"]
        self.assertEqual(len(http_events), 1)
        self.assertEqual(http_events[0].outcome, "error")

    def test_no_logging_when_audit_logger_is_none(self):
        app = _make_app(audit_logger=None)
        client = TestClient(app, raise_server_exceptions=False)

        resp = client.post("/get-online-features")
        self.assertEqual(resp.status_code, 200)

    def test_uses_x_request_id_header(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        client.post(
            "/get-online-features",
            headers={"x-request-id": "custom-id-123"},
        )
        http_events = [e for e in sink.events if e.event_type == "http.request"]
        self.assertEqual(http_events[0].request_id, "custom-id-123")

    def test_resource_mapping(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        client.post("/push")
        http_events = [e for e in sink.events if e.event_type == "http.request"]
        self.assertEqual(http_events[0].resource.type, "push_source")
        self.assertIn("WRITE_ONLINE", http_events[0].resource.actions)


class TestMcpAuditMiddleware(unittest.TestCase):
    def test_logs_mcp_tools_call(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        body = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": "get_online_features"},
            "id": 1,
        }
        client.post("/mcp", json=body)

        mcp_events = [e for e in sink.events if e.event_type == "mcp.tools.call"]
        self.assertEqual(len(mcp_events), 1)
        event = mcp_events[0]
        self.assertEqual(event.action.mcp_tool, "get_online_features")
        self.assertEqual(event.source.transport, "mcp-http")

    def test_logs_mcp_generic_request(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        body = {"jsonrpc": "2.0", "method": "tools/list", "id": 1}
        client.post("/mcp", json=body)

        mcp_events = [e for e in sink.events if e.event_type == "mcp.request"]
        self.assertEqual(len(mcp_events), 1)
        self.assertIn("tools/list", mcp_events[0].detail)

    def test_handles_non_json_body_gracefully(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        client.post("/mcp", content=b"not json")
        mcp_events = [e for e in sink.events if e.event_type.startswith("mcp.")]
        self.assertEqual(len(mcp_events), 1)
        self.assertEqual(mcp_events[0].event_type, "mcp.request")

    def test_does_not_intercept_non_mcp_paths(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        client.post("/get-online-features")
        mcp_events = [e for e in sink.events if e.event_type.startswith("mcp.")]
        self.assertEqual(len(mcp_events), 0)

    def test_uses_x_request_id_for_mcp(self):
        sink = InMemorySink()
        audit = AuditLogger(sink)
        app = _make_app(audit)
        client = TestClient(app, raise_server_exceptions=False)

        body = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": "push"},
            "id": 1,
        }
        client.post("/mcp", json=body, headers={"x-request-id": "mcp-req-42"})

        mcp_events = [e for e in sink.events if e.event_type == "mcp.tools.call"]
        self.assertEqual(mcp_events[0].request_id, "mcp-req-42")
