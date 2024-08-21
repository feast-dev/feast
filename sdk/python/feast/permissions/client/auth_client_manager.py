from abc import ABC, abstractmethod


class AuthenticationClientManager(ABC):
    @abstractmethod
    def get_token(self) -> str:
        """Retrieves the token based on the authentication type configuration"""
        pass
