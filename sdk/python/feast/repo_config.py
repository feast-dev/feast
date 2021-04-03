from pathlib import Path
from typing import Optional, Union

import yaml
from pydantic import BaseModel, StrictInt, StrictStr, ValidationError


class FeastBaseModel(BaseModel):
    """ Feast Pydantic Configuration Class """

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"


class LocalOnlineStoreConfig(FeastBaseModel):
    """ Online store config for local (SQLite-based) online store """

    path: StrictStr
    """ str: Path to sqlite db """


class DatastoreOnlineStoreConfig(FeastBaseModel):
    """ Online store config for GCP Datastore """

    project_id: StrictStr
    """ str: GCP Project Id """


class OnlineStoreConfig(FeastBaseModel):
    datastore: Optional[DatastoreOnlineStoreConfig] = None
    """ DatastoreOnlineStoreConfig: Optional Google Cloud Datastore config """

    local: Optional[LocalOnlineStoreConfig] = None
    """ LocalOnlineStoreConfig: Optional local online store config """


class RegistryConfig(FeastBaseModel):
    """ Metadata Store Configuration. Configuration that relates to reading from and writing to the Feast registry."""

    path: StrictStr
    """ str: Path to metadata store. Can be a local path, or remote object storage path, e.g. gcs://foo/bar """

    cache_ttl_seconds: StrictInt = 600
    """int: The cache TTL is the amount of time registry state will be cached in memory. If this TTL is exceeded then
     the registry will be refreshed when any feature store method asks for access to registry state. The TTL can be
     set to infinity by setting TTL to 0 seconds, which means the cache will only be loaded once and will never
     expire. Users can manually refresh the cache by calling feature_store.refresh_registry() """


class RepoConfig(FeastBaseModel):
    """ Repo config. Typically loaded from `feature_store.yaml` """

    registry: Union[StrictStr, RegistryConfig]
    """ str: Path to metadata store. Can be a local path, or remote object storage path, e.g. gcs://foo/bar """

    project: StrictStr
    """ str: Feast project id. This can be any alphanumeric string up to 16 characters.
        You can have multiple independent feature repositories deployed to the same cloud
        provider account, as long as they have different project ids.
    """

    provider: StrictStr
    """ str: local or gcp """

    online_store: Optional[OnlineStoreConfig] = None
    """ OnlineStoreConfig: Online store configuration (optional depending on provider) """

    def get_registry_config(self):
        if isinstance(self.registry, str):
            return RegistryConfig(path=self.registry)
        else:
            return self.registry


# This is the JSON Schema for config validation. We use this to have nice detailed error messages
# for config validation, something that bindr unfortunately doesn't provide out of the box.
#
# The schema should match the namedtuple structure above. It could technically even be inferred from
# the types above automatically; but for now we choose a more tedious but less magic path of
# providing the schema manually.

config_schema = {
    "type": "object",
    "properties": {
        "project": {"type": "string"},
        "registry": {"type": "string"},
        "provider": {"type": "string"},
        "online_store": {
            "type": "object",
            "properties": {
                "local": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "additionalProperties": False,
                },
                "datastore": {
                    "type": "object",
                    "properties": {"project_id": {"type": "string"}},
                    "additionalProperties": False,
                },
            },
            "additionalProperties": False,
        },
    },
    "required": ["project"],
    "additionalProperties": False,
}


class FeastConfigError(Exception):
    def __init__(self, error_message, config_path):
        self._error_message = error_message
        self._config_path = config_path
        super().__init__(self._error_message)

    def __str__(self) -> str:
        return f"{self._error_message}\nat {self._config_path}"

    def __repr__(self) -> str:
        return (
            f"FeastConfigError({repr(self._error_message)}, {repr(self._config_path)})"
        )


def load_repo_config(repo_path: Path) -> RepoConfig:
    config_path = repo_path / "feature_store.yaml"

    with open(config_path) as f:
        raw_config = yaml.safe_load(f)
        try:
            return RepoConfig(**raw_config)
        except ValidationError as e:
            raise FeastConfigError(e, config_path)
