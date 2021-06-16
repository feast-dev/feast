import importlib
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, StrictInt, StrictStr, ValidationError, root_validator
from pydantic.error_wrappers import ErrorWrapper
from pydantic.typing import Dict, Literal, Optional, Union

from feast import errors
from feast.telemetry import log_exceptions

# This dict exists so that:
# - existing values for the online store type in featurestore.yaml files continue to work in a backwards compatible way
# - first party and third party implementations can use the same class loading code path.
ONLINE_CONFIG_CLASS_FOR_TYPE = {
    "sqlite": "feast.infra.online_stores.sqlite.SqliteOnlineStore",
    "datastore": "feast.infra.online_stores.datastore.DatastoreOnlineStore",
    "redis": "feast.infra.online_stores.redis.RedisOnlineStore",
}


class FeastBaseModel(BaseModel):
    """ Feast Pydantic Configuration Class """

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"


class FeastConfigBaseModel(BaseModel):
    """ Feast Pydantic Configuration Class """

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"


class FileOfflineStoreConfig(FeastBaseModel):
    """ Offline store config for local (file-based) store """

    type: Literal["file"] = "file"
    """ Offline store type selector"""


class BigQueryOfflineStoreConfig(FeastBaseModel):
    """ Offline store config for GCP BigQuery """

    type: Literal["bigquery"] = "bigquery"
    """ Offline store type selector"""

    dataset: StrictStr = "feast"
    """ (optional) BigQuery Dataset name for temporary tables """


OfflineStoreConfig = Union[FileOfflineStoreConfig, BigQueryOfflineStoreConfig]


class RegistryConfig(FeastBaseModel):
    """ Metadata Store Configuration. Configuration that relates to reading from and writing to the Feast registry."""

    path: StrictStr
    """ str: Path to metadata store. Can be a local path, or remote object storage path, e.g. a GCS URI """

    cache_ttl_seconds: StrictInt = 600
    """int: The cache TTL is the amount of time registry state will be cached in memory. If this TTL is exceeded then
     the registry will be refreshed when any feature store method asks for access to registry state. The TTL can be
     set to infinity by setting TTL to 0 seconds, which means the cache will only be loaded once and will never
     expire. Users can manually refresh the cache by calling feature_store.refresh_registry() """


class RepoConfig(FeastBaseModel):
    """ Repo config. Typically loaded from `feature_store.yaml` """

    registry: Union[StrictStr, RegistryConfig] = "data/registry.db"
    """ str: Path to metadata store. Can be a local path, or remote object storage path, e.g. a GCS URI """

    project: StrictStr
    """ str: Feast project id. This can be any alphanumeric string up to 16 characters.
        You can have multiple independent feature repositories deployed to the same cloud
        provider account, as long as they have different project ids.
    """

    provider: StrictStr
    """ str: local or gcp """

    online_store: Any
    """ OnlineStoreConfig: Online store configuration (optional depending on provider) """

    offline_store: OfflineStoreConfig = FileOfflineStoreConfig()
    """ OfflineStoreConfig: Offline store configuration (optional depending on provider) """

    repo_path: Optional[Path] = None

    def get_registry_config(self):
        if isinstance(self.registry, str):
            return RegistryConfig(path=self.registry)
        else:
            return self.registry

    @root_validator(pre=True)
    @log_exceptions
    def _validate_online_store_config(cls, values):
        # This method will validate whether the online store configurations are set correctly. This explicit validation
        # is necessary because Pydantic Unions throw very verbose and cryptic exceptions. We also use this method to
        # impute the default online store type based on the selected provider. For the time being this method should be
        # considered tech debt until we can implement https://github.com/samuelcolvin/pydantic/issues/619 or a more
        # granular configuration system

        # Set empty online_store config if it isn't set explicitly
        if "online_store" not in values:
            values["online_store"] = dict()

        # Skip if we aren't creating the configuration from a dict
        if not isinstance(values["online_store"], Dict):
            return values

        # Make sure that the provider configuration is set. We need it to set the defaults
        assert "provider" in values

        # Set the default type
        # This is only direct reference to a provider or online store that we should have
        # for backwards compatibility.
        if "type" not in values["online_store"]:
            if values["provider"] == "local":
                values["online_store"]["type"] = "sqlite"
            elif values["provider"] == "gcp":
                values["online_store"]["type"] = "datastore"

        online_store_type = values["online_store"]["type"]

        # Validate the dict to ensure one of the union types match
        try:
            online_config_class = get_online_config_from_type(online_store_type)
            online_config_class(**values["online_store"])
        except ValidationError as e:
            raise ValidationError(
                [ErrorWrapper(e, loc="online_store")], model=RepoConfig,
            )

        return values

    @root_validator(pre=True)
    def _validate_offline_store_config(cls, values):
        # Set empty offline_store config if it isn't set explicitly
        if "offline_store" not in values:
            values["offline_store"] = dict()

        # Skip if we aren't creating the configuration from a dict
        if not isinstance(values["offline_store"], Dict):
            return values

        # Make sure that the provider configuration is set. We need it to set the defaults
        assert "provider" in values

        # Set the default type
        if "type" not in values["offline_store"]:
            if values["provider"] == "local":
                values["offline_store"]["type"] = "file"
            elif values["provider"] == "gcp":
                values["offline_store"]["type"] = "bigquery"

        offline_store_type = values["offline_store"]["type"]

        # Make sure the user hasn't provided the wrong type
        assert offline_store_type in ["file", "bigquery"]

        # Validate the dict to ensure one of the union types match
        try:
            if offline_store_type == "file":
                FileOfflineStoreConfig(**values["offline_store"])
            elif offline_store_type == "bigquery":
                BigQueryOfflineStoreConfig(**values["offline_store"])
            else:
                raise ValidationError(
                    f"Invalid offline store type {offline_store_type}"
                )
        except ValidationError as e:
            raise ValidationError(
                [ErrorWrapper(e, loc="offline_store")], model=FileOfflineStoreConfig,
            )

        return values


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


def create_repo_config(**data) -> RepoConfig:
    rc = RepoConfig(**data)
    if isinstance(rc.online_store, Dict):
        rc.online_store = get_online_config_from_type(rc.online_store["type"])(
            **rc.online_store
        )
    return rc


def get_online_config_from_type(online_store_type: str):
    if online_store_type in ONLINE_CONFIG_CLASS_FOR_TYPE:
        online_store_type = ONLINE_CONFIG_CLASS_FOR_TYPE[online_store_type]
    module_name, class_name = online_store_type.rsplit(".", 1)

    if not class_name.endswith("OnlineStore"):
        raise errors.FeastOnlineStoreConfigInvalidName(class_name)
    config_class_name = f"{class_name}Config"

    # Try importing the module that contains the custom provider
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        # The original exception can be anything - either module not found,
        # or any other kind of error happening during the module import time.
        # So we should include the original error as well in the stack trace.
        raise errors.FeastModuleImportError(
            module_name, module_type="OnlineStore"
        ) from e

    # Try getting the provider class definition
    try:
        online_store_config_class = getattr(module, config_class_name)
    except AttributeError:
        # This can only be one type of error, when class_name attribute does not exist in the module
        # So we don't have to include the original exception here
        raise errors.FeastClassImportError(
            module_name, config_class_name, class_type="OnlineStoreConfig"
        ) from None
    return online_store_config_class


def load_repo_config(repo_path: Path) -> RepoConfig:
    config_path = repo_path / "feature_store.yaml"

    with open(config_path) as f:
        raw_config = yaml.safe_load(f)
        try:
            c = create_repo_config(**raw_config)
            c.repo_path = repo_path
            return c
        except ValidationError as e:
            raise FeastConfigError(e, config_path)
