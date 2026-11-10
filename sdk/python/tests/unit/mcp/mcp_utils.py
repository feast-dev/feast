"""Helpers for the standalone ``feast mcp`` server tests.

Everything here runs in a single process. The Feast feature and registry
servers behind the proxy are replaced by an ``httpx.MockTransport``, and the MCP
server is driven either through FastMCP's in-memory client or through an
ASGI transport -- so there are no sockets, no subprocesses, and no live
Feast deployment behind any of these tests.

Note the deliberate split: the in-memory client cannot carry an
``Authorization`` header (FastMCP raises "This transport does not support
auth"), and with no auth provider configured FastMCP never populates the
access-token context. Anything that asserts on token handling therefore has
to go through :func:`mcp_over_http`, which runs a real HTTP request cycle
through the real ASGI stack -- just without binding a port.
"""

from __future__ import annotations

import json as jsonlib
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

FEATURES_URL = "http://feature-server.invalid"
REGISTRY_URL = "http://registry-server.invalid"

#: Hosts the proxy is allowed to talk to. Anything else is a test bug, not a
#: silent 200 -- see :meth:`FakeFeastBackend.handler`.
ALLOWED_HOSTS = {
    "feature-server.invalid",
    "registry-server.invalid",
    "feast.invalid",
}


@dataclass
class RecordedRequest:
    """One call the MCP server made to a Feast server behind it."""

    method: str
    path: str
    authorization: Optional[str]
    host: str = ""
    body: Any = None
    params: dict = field(default_factory=dict)

    @property
    def bearer(self) -> Optional[str]:
        """The forwarded token, or ``None`` if no bearer header was sent."""
        if not self.authorization:
            return None
        scheme, _, value = self.authorization.partition(" ")
        return value if scheme.lower() == "bearer" else None


class FakeFeastBackend:
    """Stands in for the Feast feature / registry server.

    Records every request the proxy makes and replies with either a queued
    response, a raised exception, or a default ``200 {"ok": true}``.
    """

    def __init__(self) -> None:
        self.requests: list[RecordedRequest] = []
        self._queued: deque = deque()
        self.raises: Optional[BaseException] = None
        self.default_body: Any = {"ok": True}
        #: The ``timeout=`` each faked ``httpx.AsyncClient`` was built with.
        #: Injecting a timeout exception proves the error path; only this
        #: proves a timeout was ever configured in the first place.
        self.client_timeouts: list[Any] = []

    # -- programming the fake -------------------------------------------
    def queue(
        self,
        status: int,
        body: Any = None,
        *,
        content_type: str = "application/json",
    ) -> None:
        """Queue one response, consumed by the next request."""
        self._queued.append((status, body, content_type))

    def fail_with(self, exc: BaseException) -> None:
        """Make every subsequent request raise ``exc`` (fault injection)."""
        self.raises = exc

    # -- inspection ------------------------------------------------------
    @property
    def last(self) -> RecordedRequest:
        assert self.requests, "the fake Feast backend received no requests"
        return self.requests[-1]

    @property
    def call_count(self) -> int:
        return len(self.requests)

    # -- the httpx hook --------------------------------------------------
    def handler(self, request: httpx.Request) -> httpx.Response:
        # The patch in the `feast_backend` fixture is global to httpx, so a stray
        # client built anywhere would otherwise land here and get a cheerful
        # 200. Fail loudly instead of passing silently.
        assert request.url.host in ALLOWED_HOSTS, (
            f"unexpected request to {request.url} -- "
            "something built an httpx.AsyncClient the test did not intend"
        )

        body: Any = None
        raw = request.content
        if raw:
            try:
                body = jsonlib.loads(raw)
            except ValueError:
                body = raw.decode(errors="replace")

        self.requests.append(
            RecordedRequest(
                method=request.method,
                path=request.url.path,
                authorization=request.headers.get("authorization"),
                host=request.url.host,
                body=body,
                params=dict(request.url.params),
            )
        )

        if self.raises is not None:
            raise self.raises

        if self._queued:
            status, payload, content_type = self._queued.popleft()
        else:
            status, payload, content_type = 200, self.default_body, "application/json"

        if content_type.startswith("application/json"):
            return httpx.Response(
                status, json=payload, headers={"content-type": content_type}
            )
        return httpx.Response(
            status, text=str(payload), headers={"content-type": content_type}
        )


def build_mcp(
    *,
    features: bool = True,
    registry: bool = True,
    auth: Any = None,
    timeout: float = 30.0,
):
    """Compose the same server shape ``feast mcp`` builds at runtime."""
    from fastmcp import FastMCP

    from feast.mcp.client import FeastClient
    from feast.mcp.features import create_features_mcp
    from feast.mcp.registry import create_registry_mcp

    root = FastMCP("feast-test")
    if features:
        root.mount(
            create_features_mcp(FeastClient(FEATURES_URL, timeout=timeout)),
            namespace="features",
        )
    if registry:
        root.mount(
            create_registry_mcp(FeastClient(REGISTRY_URL, timeout=timeout)),
            namespace="registry",
        )
    if auth is not None:
        root.auth = auth
    return root


@asynccontextmanager
async def mcp_over_http(server, *, request_headers: Optional[dict] = None):
    """Drive ``server`` over a real HTTP cycle without binding a socket.

    An ``httpx.ASGITransport`` feeds requests straight into the Starlette app
    that ``http_app()`` returns, so FastMCP's auth middleware, request context
    and access-token plumbing all run exactly as they do in production.
    """
    from fastmcp import Client
    from fastmcp.client.transports import StreamableHttpTransport

    app = server.http_app(path="/mcp", transport="http")

    def client_factory(headers=None, auth=None, timeout=None, **kwargs):
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://mcp.test",
            headers=headers,
            auth=auth,
            timeout=timeout,
        )

    # FastMCP starts its streamable-http session manager in the app lifespan;
    # skipping it makes every request fail with a missing task group.
    async with app.router.lifespan_context(app):
        transport = StreamableHttpTransport(
            "http://mcp.test/mcp",
            headers=request_headers or {},
            httpx_client_factory=client_factory,  # type: ignore[arg-type]
        )
        async with Client(transport) as client:
            yield client
