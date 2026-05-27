"""
Thread-local trace-scoped feature accumulator.

During an agent→Feast round-trip the server-side retrieval pushes feature refs
into this buffer.  After the response returns, the agent-side
``FeastSpanProcessor`` reads it to tag the LLM span with
``feast.context_features``.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Set


@dataclass
class FeastTraceContext:
    """Accumulated feature-retrieval metadata for the current trace."""

    feature_refs: List[str] = field(default_factory=list)
    feature_views: Set[str] = field(default_factory=set)
    feature_service: Optional[str] = None
    retrieval_span_ids: List[str] = field(default_factory=list)

    def push_retrieval(
        self,
        feature_refs: List[str],
        feature_service: Optional[str] = None,
        span_id: Optional[str] = None,
    ) -> None:
        """Record one retrieval's metadata into this context."""
        self.feature_refs.extend(feature_refs)
        for ref in feature_refs:
            parts = ref.split(":", 1)
            if len(parts) == 2:
                self.feature_views.add(parts[0])
        if feature_service:
            self.feature_service = feature_service
        if span_id:
            self.retrieval_span_ids.append(span_id)

    def get_context_attributes(self) -> Dict[str, str]:
        """Return span-attribute-ready dict of accumulated metadata."""
        if not self.feature_refs:
            return {}
        deduplicated = sorted(set(self.feature_refs))
        attrs: Dict[str, str] = {
            "feast.context_features": ",".join(deduplicated),
            "feast.context_feature_count": str(len(deduplicated)),
        }
        if self.feature_views:
            attrs["feast.context_feature_views"] = ",".join(sorted(self.feature_views))
        if self.feature_service:
            attrs["feast.context_feature_service"] = self.feature_service
        return attrs

    def clear(self) -> None:
        self.feature_refs.clear()
        self.feature_views.clear()
        self.feature_service = None
        self.retrieval_span_ids.clear()


_thread_local = threading.local()


def get_current_context() -> Optional[FeastTraceContext]:
    """Return the active ``FeastTraceContext`` for this thread, or ``None``."""
    return getattr(_thread_local, "feast_ctx", None)


@contextmanager
def feast_trace_scope() -> Iterator[FeastTraceContext]:
    """Context manager that creates and cleans up a ``FeastTraceContext``."""
    ctx = FeastTraceContext()
    _thread_local.feast_ctx = ctx
    try:
        yield ctx
    finally:
        ctx.clear()
        _thread_local.feast_ctx = None
