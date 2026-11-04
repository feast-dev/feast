"""Authentication helpers for the Feast MCP server.

This server checks *who* the caller is. It never decides *what* they are
allowed to do. It always sends the caller's token on to the Feast feature
server and REST registry server, and those servers check the token again and
apply Feast's permission model.

Checking the token here is still useful: a bad token is rejected with a 401
right away, instead of being passed on to Feast.

There are three modes. They use the same names as Feast's ``auth.type``:

1. **Passthrough** (default, ``--auth-mode passthrough``):
   Nothing is checked here. If the caller sends a bearer token, it is read
   from the request and passed on as-is. Use this for local development, or
   when this server runs outside the cluster and cannot check tokens itself.

2. **Kubernetes** (``--auth-mode kubernetes``):
   :class:`KubernetesTokenVerifier` checks Service Account and user tokens
   with the Kubernetes Token Access Review API. It uses the same parser the
   Feast servers use. This mode only works inside a cluster, and the pod
   needs RBAC permission to create ``tokenreviews``.

3. **OIDC login** (``--auth-mode oidc``):
   An OIDCProxy reads the OIDC discovery URL (the same
   ``auth_discovery_url`` used in Feast's feature_store.yaml) and shows a
   browser login page, so IDE clients such as Cursor and VS Code can sign
   the user in. After login the access token is kept server-side and sent to
   Feast on every tool call.

   Scripts and SDK clients can also send an OIDC provider token directly as
   a Bearer token. The server then checks it against the provider's JWKS.

In all three modes Feast receives the caller's own token, so Feast sees the
real user and not a service identity of this server.
"""

from typing import TYPE_CHECKING, Any, Optional

from fastmcp.server.auth import AccessToken, TokenVerifier
from fastmcp.server.auth.oidc_proxy import OIDCProxy
from fastmcp.server.dependencies import (
    get_access_token,
    get_http_request,
)
from starlette.authentication import AuthenticationError

from feast.mcp.logging_config import get_logger
from feast.permissions.server.rest_token_extractor import RestTokenExtractor

if TYPE_CHECKING:
    from feast.permissions.auth.token_parser import TokenParser

logger = get_logger(__name__)

#: Feast's own header parser. It finds the ``Authorization`` header and
#: checks the ``Bearer`` scheme, exactly like the Feast REST servers do. It
#: holds no state, so one instance is enough for every request.
_TOKEN_EXTRACTOR = RestTokenExtractor()

#: ``client_id`` is an OAuth field and Kubernetes has nothing like it, so we
#: store the mode name instead. This makes the auth mode easy to see in the
#: request logs.
_KUBERNETES_CLIENT_ID = "kubernetes"


class FeastOIDCProxy(OIDCProxy):
    """OIDCProxy that also accepts direct OIDC provider tokens.

    IDE clients go through the full OAuth flow and get MCP-issued JWTs.
    Programmatic clients (MCP SDK, scripts) can send OIDC provider tokens
    directly — they are validated against the provider's JWKS as a fallback.
    """

    async def load_access_token(self, token: str) -> AccessToken | None:
        # Step 1: Try MCP-issued JWT validation (IDE OAuth flow tokens)
        result = await super().load_access_token(token)
        if result is not None:
            return result  # type: ignore[return-value]

        # Step 2: Try direct OIDC provider token validation (SDK clients)
        # _token_validator is a JWTVerifier configured with the OIDC
        # provider's JWKS URI (e.g. http://keycloak:8081/jwks)
        try:
            logger.debug("MCP JWT validation failed, trying direct OIDC provider token")
            validated = await self._token_validator.verify_token(token)
            if validated is not None:
                logger.debug(
                    "Direct OIDC provider token accepted for sub=%s",
                    validated.claims.get("sub"),
                )
            return validated
        except Exception as e:
            logger.debug("Direct OIDC provider token validation also failed: %s", e)
            return None


def create_oidc_auth(
    *,
    discovery_url: str,
    client_id: str,
    client_secret: Optional[str] = None,
    base_url: str,
    audience: Optional[str] = None,
) -> FeastOIDCProxy:
    """Build the OIDC proxy."""
    return FeastOIDCProxy(
        config_url=discovery_url,
        client_id=client_id,
        client_secret=client_secret,
        base_url=base_url,
        audience=audience,
    )


class KubernetesTokenVerifier(TokenVerifier):
    """Check bearer tokens using Feast's own Kubernetes authentication.

    This class does no work of its own. It passes the token to
    :class:`feast.permissions.auth.kubernetes_token_parser.KubernetesTokenParser`,
    which is the same parser the Feast feature server and registry server use
    for ``auth.type: kubernetes``. So this server accepts the same tokens,
    finds the same ``Role``s, and handles ``INTRA_COMMUNICATION_BASE64`` the
    same way.

    The token is returned unchanged in ``AccessToken.token``, so
    :func:`get_auth_token` can send the caller's own token on to Feast.
    """

    def __init__(self, parser: Optional["TokenParser"] = None, **kwargs: Any) -> None:
        """
        Args:
            parser: The parser to use. Defaults to Feast's
                ``KubernetesTokenParser``. Tests pass their own parser, so the
                test suite does not need a real cluster.
        """
        super().__init__(**kwargs)
        self._parser = parser if parser is not None else _build_kubernetes_parser()

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        try:
            user = await self._parser.user_details_from_access_token(token)
        except AuthenticationError as e:
            logger.info("Kubernetes token rejected: %s", e)
            return None
        except Exception as e:
            # If we cannot work out who the caller is, we answer 401. That
            # includes the case where the API server could not answer the
            # TokenReview. Feast's own REST path
            # (feast.permissions.server.rest) does the same, so both agree on
            # what an unusable token looks like.
            logger.warning("Kubernetes token review failed: %s", e)
            return None

        logger.debug(
            "Kubernetes token accepted for %s (roles=%s)", user.username, user.roles
        )
        return AccessToken(
            token=token,
            client_id=_KUBERNETES_CLIENT_ID,
            scopes=[],
            subject=user.username,
            claims={
                "sub": user.username,
                "roles": user.roles,
                "groups": user.groups,
                "namespaces": user.namespaces,
            },
        )


def _build_kubernetes_parser() -> "TokenParser":
    """Build Feast's ``KubernetesTokenParser``, or say why we cannot.

    Two things can be missing, and each one gets its own message:

    * The Kubernetes client is an optional dependency, so the import can fail.
    * The parser's constructor calls ``load_incluster_config()`` and builds
      three API clients, so it only works inside a pod.

    Both are checked at startup. A clear error at startup is much easier to
    fix than a plain 401 on the first tool call.

    The import is inside the function so the other two auth modes never need
    the Kubernetes client.
    """
    try:
        from feast.permissions.auth.kubernetes_token_parser import (
            KubernetesTokenParser,
        )
    except ImportError as e:
        raise RuntimeError(
            "--auth-mode kubernetes needs the Kubernetes client, which is an "
            f"optional dependency. Install it with feast[k8s] ({e}). The "
            "feature server image that the MCP image is built from already "
            "has it."
        ) from e

    try:
        return KubernetesTokenParser()
    except Exception as e:
        raise RuntimeError(
            "--auth-mode kubernetes only works inside a Kubernetes cluster. "
            f"The in-cluster config could not be loaded ({type(e).__name__}: "
            f"{e}). Use --auth-mode oidc or --auth-mode passthrough when you "
            "run outside a pod."
        ) from e


def create_kubernetes_auth() -> KubernetesTokenVerifier:
    """Build the Kubernetes token verifier."""
    return KubernetesTokenVerifier()


def _request_context() -> tuple[Optional[str], Optional[str]]:
    """Best-effort ``(client_ip, "METHOD /path")`` of the current request.

    The IP is the direct socket peer. Forwarding headers are not read here,
    because any caller can set them. Behind a reverse proxy, list the proxy in
    ``FORWARDED_ALLOW_IPS`` and uvicorn replaces the peer with the client from
    ``X-Forwarded-For``. Both are ``None`` outside of an HTTP request (e.g.
    stdio transport).
    """
    try:
        request = get_http_request()
    except Exception:
        return None, None
    if request is None:
        return None, None

    client = getattr(request, "client", None)
    ip: Optional[str] = getattr(client, "host", None) if client else None

    method = getattr(request, "method", None)
    path = getattr(getattr(request, "url", None), "path", None)
    where = f"{method} {path}" if method and path else None
    return ip, where


def _describe_user(access_token: AccessToken) -> str:
    """Human-readable identity of the authenticated caller for logs."""
    claims = getattr(access_token, "claims", None) or {}
    user = (
        claims.get("preferred_username")
        or claims.get("email")
        or claims.get("sub")
        or getattr(access_token, "subject", None)
        or "unknown"
    )
    client_id = getattr(access_token, "client_id", None)
    return f"{user} (client_id={client_id})" if client_id else str(user)


def _raw_bearer_token() -> Optional[str]:
    """Bearer token read straight off the request, bypassing FastMCP's auth.

    In ``passthrough`` mode no auth provider is configured, so FastMCP never
    parses ``Authorization`` and :func:`get_access_token` stays ``None`` --
    even when the caller is holding exactly the token Feast needs. Reading
    the header directly is what makes "the client already holds a valid
    token" actually work.
    """
    try:
        request = get_http_request()
    except Exception:
        return None
    if request is None:
        return None

    try:
        token = _TOKEN_EXTRACTOR.extract_access_token(request=request)
    except (AuthenticationError, ValueError):
        # There is no Authorization header, or it is not a Bearer token.
        # Neither is an error here, because passthrough mode allows callers
        # with no token at all.
        return None
    return token.strip() or None


def get_auth_token() -> Optional[str]:
    """Return the caller's bearer token, logging who is calling from where.

    Called on every tool invocation, so this is the natural choke point to
    record per-request auth context: the authenticated user, their source
    IP, and which request (method + path) they made.
    """
    access_token: Optional[AccessToken] = get_access_token()
    ip, where = _request_context()

    if access_token is None:
        # Either no auth provider is configured (passthrough) or the provider
        # permits anonymous access. When a provider IS set, FastMCP rejects
        # missing/invalid tokens before the tool body runs, so this fallback
        # cannot launder an unvalidated token past validation -- and Feast
        # validates whatever is forwarded either way.
        raw_token = _raw_bearer_token()
        logger.info(
            "Unauthenticated request: ip=%s request=%s forwarded_caller_token=%s",
            ip or "unknown",
            where or "n/a",
            "yes" if raw_token else "no",
        )
        return raw_token

    logger.info(
        "Authenticated request: user=%s ip=%s request=%s",
        _describe_user(access_token),
        ip or "unknown",
        where or "n/a",
    )
    return access_token.token
