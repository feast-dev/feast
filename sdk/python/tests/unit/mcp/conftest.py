"""Fixtures for the standalone ``feast mcp`` server tests.

Helpers live in :mod:`tests.unit.mcp.mcp_utils`; this module holds only the
fixtures, so nothing has to import from a conftest.
"""

from __future__ import annotations

import pytest

from tests.unit.mcp.mcp_utils import FakeFeastBackend


@pytest.fixture
def feast_backend(monkeypatch) -> FakeFeastBackend:
    """Replace the transport ``FeastClient`` builds internally.

    ``FeastClient.request`` constructs its own ``httpx.AsyncClient``, so the
    class itself is the seam. The replacement has to be a *subclass* rather
    than a factory function: authlib (pulled in via ``feast.mcp.auth`` ->
    fastmcp's OIDC proxy) executes

        class AsyncOAuth2Client(_OAuth2Client, httpx.AsyncClient)

    at import time, and subclassing a plain function raises a metaclass
    conflict -- which made this suite pass or fail depending on whether some
    other module had already imported authlib.

    An explicitly supplied ``transport`` is left alone, which is what lets
    ``mcp_over_http`` pick an ASGI transport for the MCP hop while the Feast
    hop stays faked.
    """
    import httpx

    fake = FakeFeastBackend()
    real_async_client = httpx.AsyncClient

    class _FakedAsyncClient(real_async_client):  # type: ignore[valid-type,misc]
        def __init__(self, *args, **kwargs):
            if "transport" not in kwargs:
                kwargs["transport"] = httpx.MockTransport(fake.handler)
                fake.client_timeouts.append(kwargs.get("timeout"))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _FakedAsyncClient)
    return fake
