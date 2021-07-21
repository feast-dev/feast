from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, StrictInt, StrictStr, ValidationError, root_validator
from pydantic.error_wrappers import ErrorWrapper
from pydantic.typing import Dict, Optional, Union

from feast.importer import get_class_from_type
from feast.usage import log_exceptions

# These dict exists so that:
# - existing values for the online store type in featurestore.yaml files continue to work in a backwards compatible way
# - first party and third party implementations can use the same class loading code path.
ONLINE_STORE_CLASS_FOR_TYPE = {
    "sqlite": "feast.infra.online_stores.sqlite.SqliteOnlineStore",
    "datastore": "feast.infra.online_stores.datastore.DatastoreOnlineStore",
    "redis": "feast.infra.online_stores.redis.RedisOnlineStore",
    "dynamodb": "feast.infra.online_stores.dynamodb.DynamoDBOnlineStore",
}

OFFLINE_STORE_CLASS_FOR_TYPE = {
    "file": "feast.infra.offline_stores.file.FileOfflineStore",
    "bigquery": "feast.infra.offline_stores.bigquery.BigQueryOfflineStore",
    "redshift": "feast.infra.offline_stores.redshift.RedshiftOfflineStore",
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
    """ str: local or gcp or aws """

    online_store: Any
    """ OnlineStoreConfig: Online store configuration (optional depending on provider) """

    offline_store: Any
    """ OfflineStoreConfig: Offline store configuration (optional depending on provider) """

    repo_path: Optional[Path] = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        if isinstance(self.online_store, Dict):
            self.online_store = get_online_config_from_type(self.online_store["type"])(
                **self.online_store
            )
        elif isinstance(self.online_store, str):
            self.online_store = get_online_config_from_type(self.online_store)()

        if isinstance(self.offline_store, Dict):
            self.offline_store = get_offline_config_from_type(
                self.offline_store["type"]
            )(**self.offline_store)
        elif isinstance(self.offline_store, str):
            self.offline_store = get_offline_config_from_type(self.offline_store)()

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
            elif values["provider"] == "aws":
                values["online_store"]["type"] = "dynamodb"

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
            elif values["provider"] == "aws":
                values["offline_store"]["type"] = "file"

        offline_store_type = values["offline_store"]["type"]

        # Validate the dict to ensure one of the union types match
        try:
            offline_config_class = get_offline_config_from_type(offline_store_type)
            offline_config_class(**values["offline_store"])
        except ValidationError as e:
            raise ValidationError(
                [ErrorWrapper(e, loc="offline_store")], model=RepoConfig,
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


def get_data_source_class_from_type(data_source_type: str):
    module_name, config_class_name = data_source_type.rsplit(".", 1)
    return get_class_from_type(module_name, config_class_name, "Source")


def get_online_config_from_type(online_store_type: str):
    if online_store_type in ONLINE_STORE_CLASS_FOR_TYPE:
        online_store_type = ONLINE_STORE_CLASS_FOR_TYPE[online_store_type]
    else:
        assert online_store_type.endswith("OnlineStore")
    module_name, online_store_class_type = online_store_type.rsplit(".", 1)
    config_class_name = f"{online_store_class_type}Config"

    return get_class_from_type(module_name, config_class_name, config_class_name)


def get_offline_config_from_type(offline_store_type: str):
    if offline_store_type in OFFLINE_STORE_CLASS_FOR_TYPE:
        offline_store_type = OFFLINE_STORE_CLASS_FOR_TYPE[offline_store_type]
    else:
        assert offline_store_type.endswith("OfflineStore")
    module_name, offline_store_class_type = offline_store_type.rsplit(".", 1)
    config_class_name = f"{offline_store_class_type}Config"

    return get_class_from_type(module_name, config_class_name, config_class_name)


def load_repo_config(repo_path: Path) -> RepoConfig:
    config_path = repo_path / "feature_store.yaml"

    with open(config_path) as f:
        raw_config = yaml.safe_load(f)
        try:
            c = RepoConfig(**raw_config)
            c.repo_path = repo_path
            return c
        except ValidationError as e:
            raise FeastConfigError(e, config_path)
