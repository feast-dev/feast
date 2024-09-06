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
    @wraps(func)
    def wrapper(config: RepoConfig, *args, **kwargs):
        assert isinstance(config, RepoConfig)

        # Get a Session object
        with get_http_auth_requests_session(config.auth_config) as session:
            # Define a wrapper for session methods
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

            # Enhance session methods
            session.get = method_wrapper("get")  # type: ignore[method-assign]
            session.post = method_wrapper("post")  # type: ignore[method-assign]
            session.put = method_wrapper("put")  # type: ignore[method-assign]
            session.delete = method_wrapper("delete")  # type: ignore[method-assign]

            # Pass the enhanced session object to the decorated function
            return func(session, config, *args, **kwargs)

    return wrapper
