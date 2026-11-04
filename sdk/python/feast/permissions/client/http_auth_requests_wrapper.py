import logging
import threading
import time
from typing import Optional

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import (
    AuthConfig,
)
from feast.permissions.client.client_auth_token import get_auth_token

logger = logging.getLogger(__name__)


class AuthenticatedRequestsSession(Session):
    def __init__(self, auth_token: str):
        super().__init__()
        self.headers.update({"Authorization": f"Bearer {auth_token}"})


class HttpSessionManager:
    """
    Manages HTTP sessions with connection pooling for improved performance.

    This class provides:
    - Session caching based on auth configuration
    - Connection pooling via HTTPAdapter
    - Automatic retry with exponential backoff
    - Thread-safe session management
    - Automatic idle timeout (closes stale sessions)

    Configuration can be customized via feature_store.yaml:
    ```yaml
    online_store:
      type: remote
      path: http://localhost:6566
      connection_pool_size: 50        # Max connections in pool
      connection_idle_timeout: 300    # Seconds before idle session closes
      connection_retries: 3           # Retry count with backoff
    ```
    """

    _session: Optional[Session] = None
    _session_auth_type: Optional[str] = None
    _session_last_used: Optional[float] = None
    _session_config_hash: Optional[int] = None
    _lock = threading.Lock()

    # Default configuration (can be overridden via feature_store.yaml)
    DEFAULT_POOL_CONNECTIONS = 10  # Number of connection pools to cache
    DEFAULT_POOL_MAXSIZE = 50  # Max connections per pool
    DEFAULT_MAX_RETRIES = 3  # Number of retries
    DEFAULT_BACKOFF_FACTOR = 0.5  # Backoff factor for retries
    DEFAULT_MAX_IDLE_SECONDS = 300  # 5 minutes

    # Current active configuration (updated when session is created)
    _pool_maxsize: int = DEFAULT_POOL_MAXSIZE
    _max_retries: int = DEFAULT_MAX_RETRIES
    _max_idle_seconds: int = DEFAULT_MAX_IDLE_SECONDS

    @classmethod
    def get_session(
        cls,
        auth_config: AuthConfig,
        pool_maxsize: Optional[int] = None,
        max_idle_seconds: Optional[int] = None,
        max_retries: Optional[int] = None,
    ) -> Session:
        """
        Get or create a cached HTTP session with connection pooling.

        The session is cached and reused across requests. A new session
        is created if:
        - No session exists
        - Auth type changes
        - Configuration changes
        - Session has been idle longer than max_idle_seconds

        Args:
            auth_config: Authentication configuration
            pool_maxsize: Max connections in pool (default: 50)
            max_idle_seconds: Idle timeout in seconds (default: 300, 0 to disable)
            max_retries: Number of retries (default: 3)

        Returns:
            A requests Session configured with connection pooling
        """
        auth_type = auth_config.type if auth_config else AuthType.NONE.value
        current_time = time.time()

        # Use provided values or defaults
        pool_maxsize = (
            pool_maxsize if pool_maxsize is not None else cls.DEFAULT_POOL_MAXSIZE
        )
        max_idle_seconds = (
            max_idle_seconds
            if max_idle_seconds is not None
            else cls.DEFAULT_MAX_IDLE_SECONDS
        )
        max_retries = (
            max_retries if max_retries is not None else cls.DEFAULT_MAX_RETRIES
        )

        # Create config hash to detect configuration changes
        config_hash = hash((auth_type, pool_maxsize, max_idle_seconds, max_retries))

        with cls._lock:
            # Check if session has been idle too long (if timeout is enabled)
            if (
                cls._session is not None
                and cls._session_last_used is not None
                and cls._max_idle_seconds > 0
            ):
                idle_time = current_time - cls._session_last_used
                if idle_time > cls._max_idle_seconds:
                    logger.debug(
                        f"Session idle for {idle_time:.1f}s (max: {cls._max_idle_seconds}s), "
                        "closing stale session"
                    )
                    cls._close_session_internal()

            # Check if we can reuse the cached session (same auth type and config)
            if (
                cls._session is not None
                and cls._session_auth_type == auth_type
                and cls._session_config_hash == config_hash
            ):
                # For authenticated sessions, update the token in case it expired
                if auth_type != AuthType.NONE.value:
                    try:
                        auth_token = get_auth_token(auth_config)
                        cls._session.headers.update(
                            {"Authorization": f"Bearer {auth_token}"}
                        )
                    except Exception as e:
                        logger.warning(f"Failed to refresh auth token: {e}")
                        raise

                # Update last used time
                cls._session_last_used = current_time
                return cls._session

            # Close existing session if auth type or config changed
            if cls._session is not None:
                cls._close_session_internal()

            # Create new session with connection pooling
            if auth_type == AuthType.NONE.value:
                session = requests.Session()
            else:
                auth_token = get_auth_token(auth_config)
                session = AuthenticatedRequestsSession(auth_token)

            # Configure retry strategy with exponential backoff
            retry_strategy = Retry(
                total=max_retries,
                backoff_factor=cls.DEFAULT_BACKOFF_FACTOR,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST", "PUT", "DELETE"],
            )

            # Create HTTP adapter with connection pooling
            adapter = HTTPAdapter(
                pool_connections=cls.DEFAULT_POOL_CONNECTIONS,
                pool_maxsize=pool_maxsize,
                max_retries=retry_strategy,
            )

            # Mount adapter for both HTTP and HTTPS
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            # Set keep-alive header
            session.headers.update({"Connection": "keep-alive"})

            # Cache the session and track configuration
            cls._session = session
            cls._session_auth_type = auth_type
            cls._session_last_used = current_time
            cls._session_config_hash = config_hash
            cls._pool_maxsize = pool_maxsize
            cls._max_retries = max_retries
            cls._max_idle_seconds = max_idle_seconds

            idle_timeout_str = (
                f"{max_idle_seconds}s" if max_idle_seconds > 0 else "disabled"
            )
            logger.debug(
                f"Created new HTTP session with connection pooling: "
                f"pool_maxsize={pool_maxsize}, retries={max_retries}, "
                f"idle_timeout={idle_timeout_str}"
            )

            return session

    @classmethod
    def _close_session_internal(cls) -> None:
        """
        Internal method to close session without acquiring lock.
        Must be called while holding cls._lock.
        """
        if cls._session is not None:
            try:
                cls._session.close()
            except Exception:
                pass
            cls._session = None
            cls._session_auth_type = None
            cls._session_last_used = None
            cls._session_config_hash = None

    @classmethod
    def close_session(cls) -> None:
        """
        Close the cached HTTP session and release resources.

        Call this method during application shutdown to clean up.
        """
        with cls._lock:
            cls._close_session_internal()
            logger.debug("HTTP session closed")


def get_http_auth_requests_session(
    auth_config: AuthConfig,
    pool_maxsize: Optional[int] = None,
    max_idle_seconds: Optional[int] = None,
    max_retries: Optional[int] = None,
) -> Session:
    """
    Get an HTTP session with connection pooling and optional authentication.

    This function returns a cached session that reuses TCP/TLS connections
    for improved performance. Configuration can be customized via parameters
    or defaults are used.

    Args:
        auth_config: Authentication configuration
        pool_maxsize: Max connections in pool (default: 50)
        max_idle_seconds: Idle timeout in seconds (default: 300, 0 to disable)
        max_retries: Number of retries (default: 3)

    Returns:
        A requests Session configured for the given auth config
    """
    return HttpSessionManager.get_session(
        auth_config,
        pool_maxsize=pool_maxsize,
        max_idle_seconds=max_idle_seconds,
        max_retries=max_retries,
    )
