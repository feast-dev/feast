from pathlib import Path

import yaml
from pydantic import (
    BaseModel,
    PositiveInt,
    StrictInt,
    StrictStr,
    ValidationError,
    root_validator,
)
from pydantic.error_wrappers import ErrorWrapper
from pydantic.typing import Dict, Literal, Optional, Union

from feast.telemetry import log_exceptions


class FeastBaseModel(BaseModel):
    """ Feast Pydantic Configuration Class """

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"


class SqliteOnlineStoreConfig(FeastBaseModel):
    """ Online store config for local (SQLite-based) store """

    type: Literal["sqlite"] = "sqlite"
    """ Online store type selector"""

    path: StrictStr = "data/online.db"
    """ (optional) Path to sqlite db """


class DatastoreOnlineStoreConfig(FeastBaseModel):
    """ Online store config for GCP Datastore """

    type: Literal["datastore"] = "datastore"
    """ Online store type selector"""

    project_id: Optional[StrictStr] = None
    """ (optional) GCP Project Id """

    namespace: Optional[StrictStr] = None
    """ (optional) Datastore namespace """

    write_concurrency: Optional[PositiveInt] = 40
    """ (optional) Amount of threads to use when writing batches of feature rows into Datastore"""

    write_batch_size: Optional[PositiveInt] = 50
    """ (optional) Amount of feature rows per batch being written into Datastore"""


OnlineStoreConfig = Union[DatastoreOnlineStoreConfig, SqliteOnlineStoreConfig]


class FileOfflineStoreConfig(FeastBaseModel):
    """ Offline store config for local (file-based) store """

    type: Literal["file"] = "file"
    """ Offline store type selector"""


class BigQueryOfflineStoreConfig(FeastBaseModel):
    """ Offline store config for GCP BigQuery """

    type: Literal["bigquery"] = "bigquery"
    """ Offline store type selector"""

    dataset: Optional[StrictStr] = "feast"
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

    online_store: OnlineStoreConfig = SqliteOnlineStoreConfig()
    """ OnlineStoreConfig: Online store configuration (optional depending on provider) """

    offline_store: OfflineStoreConfig = FileOfflineStoreConfig()
    """ OfflineStoreConfig: Offline store configuration (optional depending on provider) """

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
        if "type" not in values["online_store"]:
            if values["provider"] == "local":
                values["online_store"]["type"] = "sqlite"
            elif values["provider"] == "gcp":
                values["online_store"]["type"] = "datastore"

        online_store_type = values["online_store"]["type"]

        # Make sure the user hasn't provided the wrong type
        assert online_store_type in ["datastore", "sqlite"]

        # Validate the dict to ensure one of the union types match
        try:
            if online_store_type == "sqlite":
                SqliteOnlineStoreConfig(**values["online_store"])
            elif online_store_type == "datastore":
                DatastoreOnlineStoreConfig(**values["online_store"])
            else:
                raise ValueError(f"Invalid online store type {online_store_type}")
        except ValidationError as e:
            raise ValidationError(
                [ErrorWrapper(e, loc="online_store")], model=SqliteOnlineStoreConfig,
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


def load_repo_config(repo_path: Path) -> RepoConfig:
    config_path = repo_path / "feature_store.yaml"

    with open(config_path) as f:
        raw_config = yaml.safe_load(f)
        try:
            return RepoConfig(**raw_config)
        except ValidationError as e:
            raise FeastConfigError(e, config_path)
