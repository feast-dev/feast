import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from feast.audit.audit_logger import (
    AuditEvent,
    AuditLogger,
    AuditPrincipal,
    AuditResource,
    AuditSink,
    AuditSource,
    FileAuditSink,
    LoggerAuditSink,
    StdoutAuditSink,
    create_audit_logger_from_config,
)


class InMemorySink(AuditSink):
    """Test sink that captures events in a list."""

    def __init__(self):
        self.events: list[AuditEvent] = []

    def emit(self, event: AuditEvent) -> None:
        self.events.append(event)


class TestAuditEvent(unittest.TestCase):
    def test_event_to_jsonl(self):
        event = AuditEvent(
            event_type="mcp.tools.call",
            timestamp="2026-05-28T12:00:00.000Z",
            request_id="abc-123",
            principal=AuditPrincipal(
                username="jane@co.com", roles=["reader"], auth_type="oidc"
            ),
            source=AuditSource(ip="10.0.0.1", transport="mcp-http"),
            outcome="success",
            duration_ms=42.0,
        )
        line = event.to_jsonl()
        parsed = json.loads(line)

        self.assertEqual(parsed["event_type"], "mcp.tools.call")
        self.assertEqual(parsed["principal"]["username"], "jane@co.com")
        self.assertEqual(parsed["source"]["ip"], "10.0.0.1")
        self.assertEqual(parsed["duration_ms"], 42.0)
        self.assertNotIn("\n", line)

    def test_event_excludes_none_duration(self):
        event = AuditEvent(event_type="authn.success")
        parsed = json.loads(event.to_jsonl())
        self.assertNotIn("duration_ms", parsed)

    def test_event_includes_trace_id_and_span_id(self):
        event = AuditEvent(
            event_type="mcp.tools.call",
            trace_id="0" * 32,
            span_id="f" * 16,
        )
        parsed = json.loads(event.to_jsonl())
        self.assertEqual(parsed["trace_id"], "0" * 32)
        self.assertEqual(parsed["span_id"], "f" * 16)

    def test_event_excludes_none_trace_fields(self):
        event = AuditEvent(event_type="test")
        parsed = json.loads(event.to_jsonl())
        self.assertNotIn("trace_id", parsed)
        self.assertNotIn("span_id", parsed)
        self.assertNotIn("jsonrpc_id", parsed)

    def test_event_includes_jsonrpc_id(self):
        event = AuditEvent(event_type="mcp.tools.call", jsonrpc_id="42")
        parsed = json.loads(event.to_jsonl())
        self.assertEqual(parsed["jsonrpc_id"], "42")


class TestAuditLogger(unittest.TestCase):
    def test_log_populates_timestamp_and_request_id(self):
        sink = InMemorySink()
        al = AuditLogger(sink)
        al.log(AuditEvent(event_type="test"))

        self.assertEqual(len(sink.events), 1)
        event = sink.events[0]
        self.assertTrue(event.timestamp.endswith("Z"))
        self.assertTrue(len(event.request_id) > 0)

    def test_log_mcp_call(self):
        sink = InMemorySink()
        al = AuditLogger(sink)
        al.log_mcp_call(
            request_id="r1",
            tool_name="get_online_features",
            path="/get-online-features",
            outcome="success",
            duration_ms=10.5,
        )

        self.assertEqual(len(sink.events), 1)
        event = sink.events[0]
        self.assertEqual(event.event_type, "mcp.tools.call")
        self.assertEqual(event.action.mcp_tool, "get_online_features")
        self.assertEqual(event.outcome, "success")

    def test_log_http_request(self):
        sink = InMemorySink()
        al = AuditLogger(sink)
        al.log_http_request(
            request_id="r2",
            method="POST",
            path="/push",
            status_code=200,
        )

        self.assertEqual(len(sink.events), 1)
        event = sink.events[0]
        self.assertEqual(event.event_type, "http.request")
        self.assertIn("POST /push -> 200", event.detail)

    def test_log_authn_success(self):
        sink = InMemorySink()
        al = AuditLogger(sink)
        al.log_authn(request_id="r3", outcome="success")
        self.assertEqual(sink.events[0].event_type, "authn.success")

    def test_log_authn_failure(self):
        sink = InMemorySink()
        al = AuditLogger(sink)
        al.log_authn(request_id="r4", outcome="failure", detail="bad token")
        event = sink.events[0]
        self.assertEqual(event.event_type, "authn.failure")
        self.assertEqual(event.detail, "bad token")

    def test_log_authz_decision(self):
        sink = InMemorySink()
        al = AuditLogger(sink)
        al.log_authz(
            request_id="r5",
            outcome="denied",
            resource=AuditResource(
                type="feature_service", name="driver_fs", actions=["READ_ONLINE"]
            ),
        )
        event = sink.events[0]
        self.assertEqual(event.event_type, "authz.decision")
        self.assertEqual(event.resource.name, "driver_fs")

    def test_log_successful_reads_suppressed(self):
        sink = InMemorySink()
        al = AuditLogger(sink, log_successful_reads=False)
        al.log_http_request(
            request_id="r6",
            method="POST",
            path="/get-online-features",
            resource=AuditResource(type="feature_service", actions=["READ_ONLINE"]),
            outcome="success",
            status_code=200,
        )
        # Successful read should be suppressed
        self.assertEqual(len(sink.events), 0)

    def test_log_successful_reads_not_suppressed_for_writes(self):
        sink = InMemorySink()
        al = AuditLogger(sink, log_successful_reads=False)
        al.log_http_request(
            request_id="r7",
            method="POST",
            path="/push",
            resource=AuditResource(type="push_source", actions=["WRITE_ONLINE"]),
            outcome="success",
            status_code=200,
        )
        self.assertEqual(len(sink.events), 1)

    def test_log_failed_reads_not_suppressed(self):
        sink = InMemorySink()
        al = AuditLogger(sink, log_successful_reads=False)
        al.log_http_request(
            request_id="r8",
            method="POST",
            path="/get-online-features",
            resource=AuditResource(type="feature_service", actions=["READ_ONLINE"]),
            outcome="failure",
            status_code=500,
        )
        self.assertEqual(len(sink.events), 1)

    def test_emit_exception_does_not_raise(self):
        bad_sink = MagicMock(spec=AuditSink)
        bad_sink.emit.side_effect = RuntimeError("disk full")
        al = AuditLogger(bad_sink)
        al.log(AuditEvent(event_type="test"))

    def test_otel_context_injected_when_active(self):
        import sys
        import types

        mock_ctx = MagicMock()
        mock_ctx.is_valid = True
        mock_ctx.trace_id = 0xABCDEF1234567890ABCDEF1234567890
        mock_ctx.span_id = 0x1234567890ABCDEF

        mock_span = MagicMock()
        mock_span.get_span_context.return_value = mock_ctx

        mock_otel_trace = types.ModuleType("opentelemetry.trace")
        mock_otel_trace.get_current_span = lambda: mock_span  # type: ignore[attr-defined]

        mock_otel = types.ModuleType("opentelemetry")
        mock_otel.trace = mock_otel_trace  # type: ignore[attr-defined]

        saved_otel = sys.modules.get("opentelemetry")
        saved_trace = sys.modules.get("opentelemetry.trace")
        sys.modules["opentelemetry"] = mock_otel
        sys.modules["opentelemetry.trace"] = mock_otel_trace
        try:
            sink = InMemorySink()
            al = AuditLogger(sink)
            al.log(AuditEvent(event_type="test"))
            event = sink.events[0]
            self.assertEqual(event.trace_id, "abcdef1234567890abcdef1234567890")
            self.assertEqual(event.span_id, "1234567890abcdef")
        finally:
            if saved_otel is None:
                sys.modules.pop("opentelemetry", None)
            else:
                sys.modules["opentelemetry"] = saved_otel
            if saved_trace is None:
                sys.modules.pop("opentelemetry.trace", None)
            else:
                sys.modules["opentelemetry.trace"] = saved_trace

    def test_otel_not_available_no_crash(self):
        sink = InMemorySink()
        al = AuditLogger(sink)
        al.log(AuditEvent(event_type="test"))
        event = sink.events[0]
        self.assertIsNone(event.trace_id)
        self.assertIsNone(event.span_id)

    def test_otel_skipped_when_trace_id_already_set(self):
        sink = InMemorySink()
        al = AuditLogger(sink)
        al.log(AuditEvent(event_type="test", trace_id="preexisting"))
        event = sink.events[0]
        self.assertEqual(event.trace_id, "preexisting")


class TestStdoutSink(unittest.TestCase):
    @patch("sys.stdout")
    def test_emit_writes_to_stdout(self, mock_stdout):
        sink = StdoutAuditSink()
        event = AuditEvent(event_type="test", timestamp="t", request_id="r")
        sink.emit(event)
        mock_stdout.write.assert_called_once()
        written = mock_stdout.write.call_args[0][0]
        self.assertIn('"event_type":"test"', written)
        self.assertTrue(written.endswith("\n"))


class TestFileAuditSink(unittest.TestCase):
    def test_emit_appends_to_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as tmp:
            path = tmp.name

        try:
            sink = FileAuditSink(path)
            event = AuditEvent(event_type="test", timestamp="t", request_id="r")
            sink.emit(event)
            sink.close()

            with open(path) as f:
                lines = f.readlines()
            self.assertEqual(len(lines), 1)
            parsed = json.loads(lines[0])
            self.assertEqual(parsed["event_type"], "test")
        finally:
            os.unlink(path)


class TestLoggerAuditSink(unittest.TestCase):
    @patch("logging.getLogger")
    def test_emit_logs_at_info(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        sink = LoggerAuditSink("feast.audit")
        event = AuditEvent(event_type="test", timestamp="t", request_id="r")
        sink.emit(event)
        mock_logger.info.assert_called_once()


class TestCreateAuditLoggerFromConfig(unittest.TestCase):
    def test_returns_none_when_disabled(self):
        from types import SimpleNamespace

        cfg = SimpleNamespace(enabled=False)
        self.assertIsNone(create_audit_logger_from_config(cfg))

    def test_returns_none_when_none(self):
        self.assertIsNone(create_audit_logger_from_config(None))

    def test_creates_stdout_logger(self):
        from types import SimpleNamespace

        cfg = SimpleNamespace(enabled=True, sink="stdout", log_successful_reads=True)
        al = create_audit_logger_from_config(cfg)
        self.assertIsNotNone(al)
        self.assertIsInstance(al._sink, StdoutAuditSink)

    def test_creates_file_logger(self):
        from types import SimpleNamespace

        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as tmp:
            path = tmp.name

        try:
            cfg = SimpleNamespace(
                enabled=True, sink="file", file_path=path, log_successful_reads=True
            )
            al = create_audit_logger_from_config(cfg)
            self.assertIsNotNone(al)
            self.assertIsInstance(al._sink, FileAuditSink)
            al.close()
        finally:
            os.unlink(path)

    def test_creates_logger_sink(self):
        from types import SimpleNamespace

        cfg = SimpleNamespace(enabled=True, sink="logger", log_successful_reads=True)
        al = create_audit_logger_from_config(cfg)
        self.assertIsNotNone(al)
        self.assertIsInstance(al._sink, LoggerAuditSink)

    def test_unknown_sink_falls_back_to_stdout(self):
        from types import SimpleNamespace

        cfg = SimpleNamespace(enabled=True, sink="kafka", log_successful_reads=True)
        al = create_audit_logger_from_config(cfg)
        self.assertIsNotNone(al)
        self.assertIsInstance(al._sink, StdoutAuditSink)


class TestAuditLoggingConfig(unittest.TestCase):
    def test_config_defaults(self):
        from feast.infra.feature_servers.base_config import AuditLoggingConfig

        cfg = AuditLoggingConfig()
        self.assertFalse(cfg.enabled)
        self.assertEqual(cfg.sink, "stdout")
        self.assertEqual(cfg.file_path, "feast_audit.log")
        self.assertTrue(cfg.log_successful_reads)

    def test_config_custom(self):
        from feast.infra.feature_servers.base_config import AuditLoggingConfig

        cfg = AuditLoggingConfig(
            enabled=True,
            sink="file",
            file_path="/var/log/feast_audit.jsonl",
            log_successful_reads=False,
        )
        self.assertTrue(cfg.enabled)
        self.assertEqual(cfg.sink, "file")
        self.assertEqual(cfg.file_path, "/var/log/feast_audit.jsonl")
        self.assertFalse(cfg.log_successful_reads)

    def test_base_feature_server_config_includes_audit_logging(self):
        from feast.infra.feature_servers.base_config import (
            AuditLoggingConfig,
            BaseFeatureServerConfig,
        )

        cfg = BaseFeatureServerConfig(
            audit_logging=AuditLoggingConfig(enabled=True, sink="stdout")
        )
        self.assertIsNotNone(cfg.audit_logging)
        self.assertTrue(cfg.audit_logging.enabled)

    def test_mcp_config_includes_audit_logging(self):
        from feast.infra.feature_servers.base_config import AuditLoggingConfig
        from feast.infra.mcp_servers.mcp_config import McpFeatureServerConfig

        cfg = McpFeatureServerConfig(
            mcp_enabled=True,
            audit_logging=AuditLoggingConfig(
                enabled=True, sink="file", file_path="/tmp/audit.log"
            ),
        )
        self.assertIsNotNone(cfg.audit_logging)
        self.assertTrue(cfg.audit_logging.enabled)
        self.assertEqual(cfg.audit_logging.sink, "file")
