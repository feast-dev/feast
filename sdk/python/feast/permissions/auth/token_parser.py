from abc import ABC, abstractmethod

from feast.permissions.user import User


class TokenParser(ABC):
    """
    A class to parse an access token to extract the user credential and roles.
    """

    @abstractmethod
    async def user_details_from_access_token(self, access_token: str) -> User:
        """
        Parse the access token and return the current user and the list of associated roles.

        Returns:
            User: Current user, with associated roles.
        """
        raise NotImplementedError()


class NoAuthTokenParser(TokenParser):
    """
    A `TokenParser` always returning an empty token
    """

    async def user_details_from_access_token(self, access_token: str, **kwargs) -> User:
        return User(username="", roles=[])
