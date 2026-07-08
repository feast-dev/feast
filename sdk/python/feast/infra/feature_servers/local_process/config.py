from typing import Literal, Optional

from pydantic import model_validator

from feast.infra.feature_servers.base_config import BaseFeatureServerConfig


class LocalFeatureServerConfig(BaseFeatureServerConfig):
    # Feature server type selector.
    type: Literal["local"] = "local"

    # The endpoint definition for transformation_service
    transformation_service_endpoint: str = "localhost:6569"

    materialize_mode: Literal["local", "remote"] = "local"
    """When 'remote', store.materialize() delegates to the feature server's
    async endpoint instead of running the batch engine locally."""

    url: Optional[str] = None
    """Feature server URL for remote materialization (e.g. http://feast-online:80).
    Required when materialize_mode is 'remote'."""

    materialize_timeout: float = 3600.0
    """Max seconds to wait for remote materialization to complete."""

    materialize_poll_interval: float = 5.0
    """Seconds between polling the registry for FV state updates."""

    @model_validator(mode="after")
    def _validate_remote_requires_url(self) -> "LocalFeatureServerConfig":
        if self.materialize_mode == "remote" and not self.url:
            raise ValueError(
                "feature_server.url must be set when materialize_mode is 'remote'"
            )
        return self
