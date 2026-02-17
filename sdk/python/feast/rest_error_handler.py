import logging
from functools import wraps

import requests

from feast import RepoConfig
from feast.errors import FeastError
from feast.permissions.client.http_auth_requests_wrapper import (
    get_http_auth_requests_session,
)

logger = logging.getLogger(__name__)


def rest_error_handling_decorator(func):
    """
    Decorator that provides HTTP session management and error handling for REST API calls.

    This decorator:
    - Provides a cached HTTP session with connection pooling for improved performance
    - Wraps session methods to add logging and error handling
    - Maps Feast-specific errors from API responses

    The session is reused across requests (connection pooling), which saves
    TCP/TLS handshake overhead on subsequent calls.

    Connection pool settings can be configured via feature_store.yaml:
    ```yaml
    online_store:
      type: remote
      path: http://localhost:6566
      connection_pool_size: 50        # Max connections in pool
      connection_idle_timeout: 300    # Seconds before idle session closes (0 to disable)
      connection_retries: 3           # Retry count with backoff
    ```
    """

    @wraps(func)
    def wrapper(config: RepoConfig, *args, **kwargs):
        assert isinstance(config, RepoConfig)

        # Extract connection pool configuration from online_store if available
        pool_maxsize = None
        max_idle_seconds = None
        max_retries = None

        if config.online_store is not None:
            attr_map = {
                "pool_maxsize": "connection_pool_size",
                "max_idle_seconds": "connection_idle_timeout",
                "max_retries": "connection_retries",
            }
            conn_config = {
                key: getattr(config.online_store, attr_name, None)
                for key, attr_name in attr_map.items()
            }
            pool_maxsize = conn_config["pool_maxsize"]
            max_idle_seconds = conn_config["max_idle_seconds"]
            max_retries = conn_config["max_retries"]

        # Get a cached session with connection pooling
        session = get_http_auth_requests_session(
            config.auth_config,
            pool_maxsize=pool_maxsize,
            max_idle_seconds=max_idle_seconds,
            max_retries=max_retries,
        )

        # Define a wrapper for session methods to add logging and error handling
        def method_wrapper(method_name):
            original_method = getattr(session, method_name)

            @wraps(original_method)
            def wrapped_method(*args, **kwargs):
                logger.debug(
                    f"Calling {method_name} with args: {args}, kwargs: {kwargs}"
                )
                response = original_method(*args, **kwargs)
                logger.debug(
                    f"{method_name} response status code: {response.status_code}"
                )

                try:
                    response.raise_for_status()
                except requests.RequestException:
                    logger.debug(f"response.json() = {response.json()}")
                    mapped_error = FeastError.from_error_detail(response.json())
                    logger.debug(f"mapped_error = {str(mapped_error)}")
                    if mapped_error is not None:
                        raise mapped_error
                return response

            return wrapped_method

        # Enhance session methods with logging and error handling
        session.get = method_wrapper("get")  # type: ignore[method-assign]
        session.post = method_wrapper("post")  # type: ignore[method-assign]
        session.put = method_wrapper("put")  # type: ignore[method-assign]
        session.delete = method_wrapper("delete")  # type: ignore[method-assign]

        # Pass the enhanced session object to the decorated function
        return func(session, config, *args, **kwargs)

    return wrapper
