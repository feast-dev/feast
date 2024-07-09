from typing import Literal

from feast.repo_config import FeastConfigBaseModel


class AuthConfig(FeastConfigBaseModel):
    type: Literal["oidc", "kubernetes", "no_auth"] = "no_auth"


class OidcAuthConfig(AuthConfig):
    auth_server_url: str
    client_id: str
    client_secret: str
    username: str
    password: str
    realm: str = "master"


class NoAuthConfig(AuthConfig):
    pass


class K8AuthConfig(AuthConfig):
    pass
