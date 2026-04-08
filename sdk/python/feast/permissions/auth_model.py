from __future__ import annotations

from typing import Literal, Optional, Tuple

from pydantic import ConfigDict, model_validator

from feast.repo_config import FeastConfigBaseModel


def _check_mutually_exclusive(**groups: Tuple[object, ...]) -> None:
    """Validate that at most one named group is configured, and completely.

    Each *group* is a tuple of field values.
    A group is **active** only when *all* its values are truthy.
    A group is **partial** (error) when *any* but not *all* values are truthy.
    At most one active group may exist.
    """
    partial = [name for name, vals in groups.items() if any(vals) and not all(vals)]
    if partial:
        raise ValueError(
            f"Incomplete configuration for '{partial[0]}': "
            f"configure all of these fields together, or none at all. "
            f"Check the documentation for valid credential combinations."
        )
    active = [name for name, vals in groups.items() if all(vals)]
    if len(active) > 1:
        raise ValueError(
            f"Only one of [{', '.join(groups)}] may be set, "
            f"but got: {', '.join(active)}"
        )


class AuthConfig(FeastConfigBaseModel):
    type: Literal["oidc", "kubernetes", "no_auth"] = "no_auth"


class OidcAuthConfig(AuthConfig):
    auth_discovery_url: str
    client_id: Optional[str] = None
    verify_ssl: bool = True
    ca_cert_path: str = ""


class OidcClientAuthConfig(OidcAuthConfig):
    auth_discovery_url: Optional[str] = None  # type: ignore[assignment]
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
