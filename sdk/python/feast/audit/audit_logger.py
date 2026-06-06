# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Structured audit logging for the Feast feature server.

Emits JSONL audit events for MCP tool calls, REST requests, and
authentication/authorization decisions. Sensitive payloads (tokens,
entity rows, feature values) are never included.
"""

import abc
import contextvars
import logging
import sys
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ContextVar used by the MCP audit wrapper to propagate a single request_id
# into the internal REST call so that ``mcp.tools.call`` and
# ``http.request`` events share the same identifier in SIEM.
mcp_audit_request_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "mcp_audit_request_id", default=None
)


# ---------------------------------------------------------------------------
# Audit event schema
# ---------------------------------------------------------------------------


class AuditPrincipal(BaseModel):
    username: str = ""
    roles: List[str] = Field(default_factory=list)
    auth_type: str = ""


class AuditSource(BaseModel):
    ip: str = ""
    transport: str = ""


class AuditAction(BaseModel):
    mcp_tool: str = ""
    path: str = ""


class AuditResource(BaseModel):
    type: str = ""
    name: str = ""
    actions: List[str] = Field(default_factory=list)


class AuditEvent(BaseModel):
    """A single structured audit log entry.

    Fields follow the schema proposed in feast-dev/feast#6452.
    No sensitive payloads (tokens, entity rows, feature values) are stored.
    When OpenTelemetry is active, ``trace_id`` and ``span_id`` are populated
    automatically for correlation with distributed traces.
    """

    event_type: str
    timestamp: str = ""
    request_id: str = ""
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    jsonrpc_id: Optional[str] = None
    principal: AuditPrincipal = Field(default_factory=AuditPrincipal)
    source: AuditSource = Field(default_factory=AuditSource)
    action: AuditAction = Field(default_factory=AuditAction)
    resource: AuditResource = Field(default_factory=AuditResource)
    outcome: str = ""
    duration_ms: Optional[float] = None
    detail: str = ""

    def to_jsonl(self) -> str:
        return self.model_dump_json(exclude_none=True)


# ---------------------------------------------------------------------------
# Sink abstraction
# ---------------------------------------------------------------------------


class AuditSink(abc.ABC):
    """Base class for audit event sinks."""

    @abc.abstractmethod
    def emit(self, event: AuditEvent) -> None: ...

    def close(self) -> None:
        pass


class StdoutAuditSink(AuditSink):
    """Write JSONL events to stdout (the default)."""

    def emit(self, event: AuditEvent) -> None:
        sys.stdout.write(event.to_jsonl() + "\n")
        sys.stdout.flush()


class FileAuditSink(AuditSink):
    """Append JSONL events to a local file."""

    def __init__(self, file_path: str) -> None:
        self._file_path = file_path
        self._fh = open(file_path, "a", buffering=1)

    def emit(self, event: AuditEvent) -> None:
        self._fh.write(event.to_jsonl() + "\n")

    def close(self) -> None:
        self._fh.close()


class LoggerAuditSink(AuditSink):
    """Emit audit events through Python's ``logging`` module at INFO level."""

    def __init__(self, logger_name: str = "feast.audit") -> None:
        self._logger = logging.getLogger(logger_name)

    def emit(self, event: AuditEvent) -> None:
        self._logger.info(event.to_jsonl())


# ---------------------------------------------------------------------------
# AuditLogger — the main entry point
# ---------------------------------------------------------------------------


class AuditLogger:
    """Central audit logger that routes events to the configured sink.

    Instantiate once during feature-server startup and share via
    ``app.state.audit_logger``.
    """

    def __init__(
        self,
        sink: AuditSink,
        *,
        log_successful_reads: bool = True,
    ) -> None:
        self._sink = sink
        self._log_successful_reads = log_successful_reads
        self._lock = threading.Lock()

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def new_request_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def _utcnow_iso() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    @staticmethod
    def _inject_otel_context(event: AuditEvent) -> None:
        """Populate trace_id/span_id from the active OpenTelemetry span, if any."""
        if event.trace_id is not None:
            return
        try:
            from opentelemetry import trace as otel_trace

            span = otel_trace.get_current_span()
            ctx = span.get_span_context()
            if ctx and ctx.is_valid:
                event.trace_id = format(ctx.trace_id, "032x")
                event.span_id = format(ctx.span_id, "016x")
        except ImportError:
            pass
        except Exception:
            pass

    # -- public API --------------------------------------------------------

    def log(self, event: AuditEvent) -> None:
        if not event.timestamp:
            event.timestamp = self._utcnow_iso()
        if not event.request_id:
            event.request_id = self.new_request_id()

        self._inject_otel_context(event)

        if event.outcome == "success" and not self._log_successful_reads:
            read_events = {"mcp.tools.call", "http.request"}
            if event.event_type in read_events:
                read_actions = {"READ_ONLINE", "READ_OFFLINE"}
                resource_actions = set(event.resource.actions)
                if resource_actions and resource_actions.issubset(read_actions):
                    return

        try:
            with self._lock:
                self._sink.emit(event)
        except Exception:
            logger.exception("Failed to emit audit event")

    def log_mcp_call(
        self,
        *,
        request_id: str,
        tool_name: str,
        path: str = "",
        principal: Optional[AuditPrincipal] = None,
        source: Optional[AuditSource] = None,
        resource: Optional[AuditResource] = None,
        outcome: str = "success",
        duration_ms: Optional[float] = None,
        detail: str = "",
    ) -> None:
        self.log(
            AuditEvent(
                event_type="mcp.tools.call",
                request_id=request_id,
                principal=principal or AuditPrincipal(),
                source=source or AuditSource(transport="mcp-http"),
                action=AuditAction(mcp_tool=tool_name, path=path),
                resource=resource or AuditResource(),
                outcome=outcome,
                duration_ms=duration_ms,
                detail=detail,
            )
        )

    def log_http_request(
        self,
        *,
        request_id: str,
        method: str,
        path: str,
        principal: Optional[AuditPrincipal] = None,
        source: Optional[AuditSource] = None,
        resource: Optional[AuditResource] = None,
        outcome: str = "success",
        duration_ms: Optional[float] = None,
        status_code: int = 200,
    ) -> None:
        self.log(
            AuditEvent(
                event_type="http.request",
                request_id=request_id,
                principal=principal or AuditPrincipal(),
                source=source or AuditSource(transport="http"),
                action=AuditAction(path=path),
                resource=resource or AuditResource(),
                outcome=outcome,
                duration_ms=duration_ms,
                detail=f"{method} {path} -> {status_code}",
            )
        )

    def log_authn(
        self,
        *,
        request_id: str,
        outcome: str,
        principal: Optional[AuditPrincipal] = None,
        source: Optional[AuditSource] = None,
        detail: str = "",
    ) -> None:
        event_type = "authn.success" if outcome == "success" else "authn.failure"
        self.log(
            AuditEvent(
                event_type=event_type,
                request_id=request_id,
                principal=principal or AuditPrincipal(),
                source=source or AuditSource(),
                outcome=outcome,
                detail=detail,
            )
        )

    def log_authz(
        self,
        *,
        request_id: str,
        outcome: str,
        principal: Optional[AuditPrincipal] = None,
        resource: Optional[AuditResource] = None,
        detail: str = "",
    ) -> None:
        self.log(
            AuditEvent(
                event_type="authz.decision",
                request_id=request_id,
                principal=principal or AuditPrincipal(),
                resource=resource or AuditResource(),
                outcome=outcome,
                detail=detail,
            )
        )

    def close(self) -> None:
        self._sink.close()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_SINK_FACTORIES = {
    "stdout": lambda cfg: StdoutAuditSink(),
    "file": lambda cfg: FileAuditSink(cfg.get("file_path", "feast_audit.log")),
    "logger": lambda cfg: LoggerAuditSink(cfg.get("logger_name", "feast.audit")),
}


def create_audit_logger_from_config(
    audit_cfg: Any,
) -> Optional[AuditLogger]:
    """Build an ``AuditLogger`` from an ``AuditLoggingConfig`` pydantic model.

    Returns ``None`` when audit logging is disabled.
    """
    if audit_cfg is None or not getattr(audit_cfg, "enabled", False):
        return None

    sink_type = getattr(audit_cfg, "sink", "stdout")
    raw: Dict[str, Any] = {}
    if hasattr(audit_cfg, "file_path"):
        raw["file_path"] = audit_cfg.file_path
    if hasattr(audit_cfg, "logger_name"):
        raw["logger_name"] = audit_cfg.logger_name

    factory = _SINK_FACTORIES.get(sink_type)
    if factory is None:
        logger.warning("Unknown audit sink %r, falling back to stdout", sink_type)
        factory = _SINK_FACTORIES["stdout"]

    sink = factory(raw)
    return AuditLogger(
        sink,
        log_successful_reads=getattr(audit_cfg, "log_successful_reads", True),
    )
