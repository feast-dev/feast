from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.auth.token_extractor import TokenExtractor


class GrpcTokenExtractor(TokenExtractor):
    def extract_access_token(self, **kwargs) -> str:
        """
        Token extractor for grpc server requests.

        Requires a keyword argument called `metadata` of type `dict`.

        Returns:
            The extracted access token.
        """

        if "metadata" not in kwargs:
            raise ValueError("Missing keywork argument 'metadata'")
        if not isinstance(kwargs["metadata"], dict):
            raise ValueError(
                f"The keywork argument 'metadata' is not of the expected type {dict.__name__} but {type(kwargs['metadata'])}"
            )

        access_token = None
        metadata = kwargs["metadata"]
        if isinstance(metadata, dict):
            for header in metadata:
                if header.lower() == "authorization":
                    return self._extract_bearer_token(metadata[header])

        if access_token is None:
            raise AuthenticationError("Missing authorization header")

        return access_token
