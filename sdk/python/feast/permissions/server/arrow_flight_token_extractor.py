from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.auth.token_extractor import TokenExtractor


class ArrowFlightTokenExtractor(TokenExtractor):
    def extract_access_token(self, **kwargs) -> str:
        """
        Token extractor for Arrow Flight requests.

        Requires a keyword argument called `headers` of type `dict`.

        Returns:
            The extracted access token.
        """

        if "headers" not in kwargs:
            raise ValueError("Missing keywork argument 'headers'")
        if not isinstance(kwargs["headers"], dict):
            raise ValueError(
                f"The keywork argument 'headers' is not of the expected type {dict.__name__}"
            )

        access_token = None
        headers = kwargs["headers"]
        if isinstance(headers, dict):
            for header in headers:
                if header.lower() == "authorization":
                    # With Arrow Flight, the header value is a list and we take the 0-th element
                    if not isinstance(headers[header], list):
                        raise AuthenticationError(
                            f"Authorization header must be of type list, found {type(headers[header])}"
                        )

                    return self._extract_bearer_token(headers[header][0])

        if access_token is None:
            raise AuthenticationError("Missing authorization header")

        return access_token
