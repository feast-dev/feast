from abc import ABC
from typing import Optional

from .token_extractor import NoAuthTokenExtractor, TokenExtractor
from .token_parser import NoAuthTokenParser, TokenParser


class AuthManager(ABC):
    """
    The authorization manager offers services to manage authorization tokens from client requests
    to extract user details before injecting them in the security context.
    """

    _token_parser: TokenParser
    _token_extractor: TokenExtractor

    def __init__(self, token_parser: TokenParser, token_extractor: TokenExtractor):
        self._token_parser = token_parser
        self._token_extractor = token_extractor

    @property
    def token_parser(self) -> TokenParser:
        return self._token_parser

    @property
    def token_extractor(self) -> TokenExtractor:
        return self._token_extractor


"""
The possibly empty global instance of `AuthManager`.
"""
_auth_manager: Optional[AuthManager] = None


def get_auth_manager() -> AuthManager:
    """
    Return the global instance of `AuthManager`.

    Raises:
        RuntimeError if the clobal instance is not set.
    """
    global _auth_manager
    if _auth_manager is None:
        raise RuntimeError(
            "AuthManager is not initialized. Call 'set_auth_manager' first."
        )
    return _auth_manager


def set_auth_manager(auth_manager: AuthManager):
    """
    Initialize the global instance of `AuthManager`.
    """

    global _auth_manager
    _auth_manager = auth_manager


class AllowAll(AuthManager):
    """
    An AuthManager not extracting nor parsing the authorization token.
    """

    def __init__(self):
        super().__init__(
            token_extractor=NoAuthTokenExtractor(), token_parser=NoAuthTokenParser()
        )
