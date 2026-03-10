from __future__ import annotations

from typing import Literal, Optional, Tuple

from pydantic import ConfigDict, model_validator

from feast.repo_config import FeastConfigBaseModel


def _check_mutually_exclusive(**groups: Tuple[object, ...]) -> None:
    """Validate that at most one named group is configured, and completely.

    Each *group* is a tuple of field values.
    A group is **active** if *any* value in its tuple is truthy.
    An active group is **valid** only if *all* its values are truthy.
    At most one group may be active.
    """
    active = {name: values for name, values in groups.items() if any(values)}
    if len(active) > 1:
        raise ValueError(
            f"Only one of [{', '.join(groups)}] may be set, "
            f"but got: {', '.join(active)}"
        )
    for name, values in active.items():
        if not all(values):
            raise ValueError(
                f"Incomplete configuration for '{name}': "
                f"all fields in this group are required when any is set."
            )


class AuthConfig(FeastConfigBaseModel):
    type: Literal["oidc", "kubernetes", "no_auth"] = "no_auth"


class OidcAuthConfig(AuthConfig):
    auth_discovery_url: str
    client_id: str


class OidcClientAuthConfig(OidcAuthConfig):
    auth_discovery_url: Optional[str] = None
    client_id: Optional[str] = None

    username: Optional[str] = None
    password: Optional[str] = None
    client_secret: Optional[str] = None
    token: Optional[str] = None
    token_env_var: Optional[str] = None

    @model_validator(mode="after")
    def _validate_credentials(self):
        network = (self.client_secret, self.auth_discovery_url, self.client_id)
        if self.username or self.password:
            network += (self.username, self.password)

        _check_mutually_exclusive(
            token=(self.token,),
            token_env_var=(self.token_env_var,),
            client_credentials=network,
        )
        return self


class NoAuthConfig(AuthConfig):
    pass


class KubernetesAuthConfig(AuthConfig):
    user_token: Optional[str] = None

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
