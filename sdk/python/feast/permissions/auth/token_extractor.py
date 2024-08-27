import re
from abc import ABC

from starlette.authentication import (
    AuthenticationError,
)


class TokenExtractor(ABC):
    """
    A class to extract the authorization token from a user request.
    """

    def extract_access_token(self, **kwargs) -> str:
        """
        Extract the authorization token from a user request.

        The actual implementation has to specify what arguments have to be defined in the kwywork args `kwargs`

        Returns:
            The extracted access token.
        """
        raise NotImplementedError()

    def _extract_bearer_token(self, auth_header: str) -> str:
        """
        Extract the bearer token from the authorization header value.

        Args:
            auth_header: The full value of the authorization header.

        Returns:
            str: The token value, without the `Bearer` part.

        Raises:
            AuthenticationError if the authorization token does not match the `Bearer` scheme.
        """
        pattern = r"(?i)Bearer .+"
        if not bool(re.match(pattern, auth_header)):
            raise AuthenticationError(f"Expected Bearer schema, found {auth_header}")
        _, access_token = auth_header.split()
        return access_token


class NoAuthTokenExtractor(TokenExtractor):
    """
    A `TokenExtractor` always returning an empty token
    """

    def extract_access_token(self, **kwargs) -> str:
        return ""
