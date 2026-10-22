from fastapi.requests import Request
from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.auth.token_extractor import TokenExtractor


class RestTokenExtractor(TokenExtractor):
    def extract_access_token(self, **kwargs) -> str:
        """
        Token extractor for REST requests.

        Requires a keyword argument called `request` of type `Request`

        Returns:
            The extracted access token.
        """

        if "request" not in kwargs:
            raise ValueError("Missing keywork argument 'request'")
        if not isinstance(kwargs["request"], Request):
            raise ValueError(
                f"The keywork argument 'request' is not of the expected type {Request.__name__}"
            )

        access_token = None
        request = kwargs["request"]
        if isinstance(request, Request):
            headers = request.headers
            for header in headers:
                if header.lower() == "authorization":
                    return self._extract_bearer_token(headers[header])

        if access_token is None:
            raise AuthenticationError("Missing authorization header")

        return access_token
