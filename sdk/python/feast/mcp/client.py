import logging
from typing import Any, Dict, Optional
from urllib.parse import quote

import httpx
from fastmcp.exceptions import ToolError

logger = logging.getLogger(__name__)

#: Feast error bodies are echoed back to the caller (and to the LLM), so
#: cap how much of an unexpected payload can ride along in the message.
_MAX_DETAIL_CHARS = 500


def _error_detail(response: httpx.Response) -> str:
    """Best-effort human-readable reason from a Feast error response."""
    try:
        body: Any = response.json()
    except ValueError:
        return response.text[:_MAX_DETAIL_CHARS] or response.reason_phrase
    if isinstance(body, dict):
        for key in ("detail", "message", "error"):
            value = body.get(key)
            if value:
                return str(value)[:_MAX_DETAIL_CHARS]
    return str(body)[:_MAX_DETAIL_CHARS]


def path_segment(value: str) -> str:
    """Encode a tool argument as exactly one URL path segment.

    Tool arguments come from the LLM, so ``/`` must not open another route.
    ``quote`` leaves a bare ``.`` or ``..`` alone and httpx would resolve it,
    so those are encoded as well.
    """
    segment = quote(value, safe="")
    return segment.replace(".", "%2E") if segment in (".", "..") else segment


class FeastClient:
    """HTTP client that proxies requests to the Feast feature server."""

    def __init__(self, base_url: str, timeout: float = 30.0):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def _headers(self, token: Optional[str] = None) -> Dict[str, str]:
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def request(
        self,
        method: str,
        path: str,
        *,
        token: Optional[str] = None,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> Any:
        url = f"{self._base_url}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.request(
                method,
                url,
                headers=self._headers(token),
                json=json,
                params=params,
            )
            # Every error status must become an MCP error. Returning a 401
            # or 403 body as a successful tool result would hand the caller a
            # denial payload dressed as feature data. ToolError (rather than a
            # bare raise) survives servers configured with mask_error_details.
            if response.status_code >= 400:
                detail = _error_detail(response)
                logger.warning(
                    "Feast %s %s returned %s: %s",
                    method,
                    url,
                    response.status_code,
                    detail,
                )
                raise ToolError(
                    f"Feast returned HTTP {response.status_code} for "
                    f"{method} {path}: {detail}"
                )
            if response.headers.get("content-type", "").startswith("application/json"):
                return response.json()
            return response.text
