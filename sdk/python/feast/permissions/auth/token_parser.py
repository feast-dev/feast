from abc import ABC, abstractmethod


class TokenParser(ABC):
    """
    A class to parse an access token to extract the user credential and roles.
    """

    @abstractmethod
    async def user_details_from_access_token(
        self, access_token: str, **kwargs
    ) -> tuple[str, list[str]]:
        """
        Parse the access token and return the current user and the list of associated roles.

        Returns:
            str: Current user.
            list[str]: Roles associated to the user.
        """
        raise NotImplementedError()


class NoAuthTokenParser(TokenParser):
    """
    A `TokenParser` always returning an empty token
    """

    async def user_details_from_access_token(
        self, access_token: str, **kwargs
    ) -> tuple[str, list[str]]:
        return ("", [])
