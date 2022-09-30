import logging
import os
import warnings
from pathlib import Path
from typing import Any

import yaml
from pydantic import (
    BaseModel,
    Field,
    StrictInt,
    StrictStr,
    ValidationError,
    root_validator,
    validator,
)
from pydantic.error_wrappers import ErrorWrapper
from pydantic.typing import Dict, Optional, Union

from feast.errors import (
    FeastFeatureServerTypeInvalidError,
    FeastFeatureServerTypeSetError,
    FeastOfflineStoreInvalidName,
    FeastOnlineStoreInvalidName,
    FeastProviderNotSetError,
)
from feast.importer import import_class
from feast.usage import log_exceptions

warnings.simplefilter("once", RuntimeWarning)

_logger = logging.getLogger(__name__)

# These dict exists so that:
# - existing values for the online store type in featurestore.yaml files continue to work in a backwards compatible way
# - first party and third party implementations can use the same class loading code path.
BATCH_ENGINE_CLASS_FOR_TYPE = {
    "local": "feast.infra.materialization.local_engine.LocalMaterializationEngine",
    "snowflake.engine": "feast.infra.materialization.snowflake_engine.SnowflakeMaterializationEngine",
    "lambda": "feast.infra.materialization.aws_lambda.lambda_engine.LambdaMaterializationEngine",
    "bytewax": "feast.infra.materialization.contrib.bytewax.bytewax_materialization_engine.BytewaxMaterializationEngine",
    "spark.engine": "feast.infra.materialization.contrib.spark.spark_materialization_engine.SparkMaterializationEngine",
}

ONLINE_STORE_CLASS_FOR_TYPE = {
    "sqlite": "feast.infra.online_stores.sqlite.SqliteOnlineStore",
    "datastore": "feast.infra.online_stores.datastore.DatastoreOnlineStore",
    "redis": "feast.infra.online_stores.redis.RedisOnlineStore",
    "dynamodb": "feast.infra.online_stores.dynamodb.DynamoDBOnlineStore",
    "snowflake.online": "feast.infra.online_stores.snowflake.SnowflakeOnlineStore",
    "bigtable": "feast.infra.online_stores.bigtable.BigtableOnlineStore",
    "postgres": "feast.infra.online_stores.contrib.postgres.PostgreSQLOnlineStore",
    "hbase": "feast.infra.online_stores.contrib.hbase_online_store.hbase.HbaseOnlineStore",
    "cassandra": "feast.infra.online_stores.contrib.cassandra_online_store.cassandra_online_store.CassandraOnlineStore",
    "mysql": "feast.infra.online_stores.contrib.mysql_online_store.mysql.MySQLOnlineStore",
}

OFFLINE_STORE_CLASS_FOR_TYPE = {
    "file": "feast.infra.offline_stores.file.FileOfflineStore",
    "bigquery": "feast.infra.offline_stores.bigquery.BigQueryOfflineStore",
    "redshift": "feast.infra.offline_stores.redshift.RedshiftOfflineStore",
    "snowflake.offline": "feast.infra.offline_stores.snowflake.SnowflakeOfflineStore",
    "spark": "feast.infra.offline_stores.contrib.spark_offline_store.spark.SparkOfflineStore",
    "trino": "feast.infra.offline_stores.contrib.trino_offline_store.trino.TrinoOfflineStore",
    "postgres": "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.PostgreSQLOfflineStore",
    "athena": "feast.infra.offline_stores.contrib.athena_offline_store.athena.AthenaOfflineStore",
    "mssql": "feast.infra.offline_stores.contrib.mssql_offline_store.mssql.MsSqlServerOfflineStore",
}

FEATURE_SERVER_CONFIG_CLASS_FOR_TYPE = {
    "aws_lambda": "feast.infra.feature_servers.aws_lambda.config.AwsLambdaFeatureServerConfig",
    "gcp_cloudrun": "feast.infra.feature_servers.gcp_cloudrun.config.GcpCloudRunFeatureServerConfig",
    "local": "feast.infra.feature_servers.local_process.config.LocalFeatureServerConfig",
}

FEATURE_SERVER_TYPE_FOR_PROVIDER = {
    "aws": "aws_lambda",
    "gcp": "gcp_cloudrun",
    "local": "local",
}


class FeastBaseModel(BaseModel):
    """Feast Pydantic Configuration Class"""

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"


class FeastConfigBaseModel(BaseModel):
    """Feast Pydantic Configuration Class"""

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"


class RegistryConfig(FeastBaseModel):
    """Metadata Store Configuration. Configuration that relates to reading from and writing to the Feast registry."""

    registry_type: StrictStr = "file"
    """ str: Provider name or a class name that implements RegistryStore.
        If specified, registry_store_type should be redundant."""

    registry_store_type: Optional[StrictStr]
    """ str: Provider name or a class name that implements RegistryStore. """

    path: StrictStr
    """ str: Path to metadata store. Can be a local path, or remote object storage path, e.g. a GCS URI """

    cache_ttl_seconds: StrictInt = 600
    """int: The cache TTL is the amount of time registry state will be cached in memory. If this TTL is exceeded then
     the registry will be refreshed when any feature store method asks for access to registry state. The TTL can be
     set to infinity by setting TTL to 0 seconds, which means the cache will only be loaded once and will never
     expire. Users can manually refresh the cache by calling feature_store.refresh_registry() """

    s3_additional_kwargs: Optional[Dict[str, str]]
    """ Dict[str, str]: Extra arguments to pass to boto3 when writing the registry file to S3. """


class RepoConfig(FeastBaseModel):
    """Repo config. Typically loaded from `feature_store.yaml`"""

    registry: Union[StrictStr, RegistryConfig] = "data/registry.db"
    """ str: Path to metadata store. Can be a local path, or remote object storage path, e.g. a GCS URI """

    project: StrictStr
    """ str: Feast project id. This can be any alphanumeric string up to 16 characters.
        You can have multiple independent feature repositories deployed to the same cloud
        provider account, as long as they have different project ids.
    """

    provider: StrictStr
    """ str: local or gcp or aws """

    _online_config: Any = Field(alias="online_store")
    """ OnlineStoreConfig: Online store configuration (optional depending on provider) """

    _offline_config: Any = Field(alias="offline_store")
    """ OfflineStoreConfig: Offline store configuration (optional depending on provider) """

    _batch_engine_config: Any = Field(alias="batch_engine")
    """ BatchMaterializationEngine: Batch materialization configuration (optional depending on provider)"""

    feature_server: Optional[Any]
    """ FeatureServerConfig: Feature server configuration (optional depending on provider) """

    flags: Any
    """ Flags (deprecated field): Feature flags for experimental features """

    repo_path: Optional[Path] = None

    go_feature_serving: Optional[bool] = False
    """ If True, use the Go feature server instead of the Python feature server. """

    go_feature_retrieval: Optional[bool] = False
    """ If True, use the embedded Go code to retrieve features instead of the Python SDK. """

    entity_key_serialization_version: StrictInt = 1
    """ Entity key serialization version: This version is used to control what serialization scheme is
    used when writing data to the online store.
    A value <= 1 uses the serialization scheme used by feast up to Feast 0.22.
    A value of 2 uses a newer serialization scheme, supported as of Feast 0.23.
    The main difference between the two scheme is that the serialization scheme v1 stored `long` values as `int`s,
    which would result in errors trying to serialize a range of values.
    v2 fixes this error, but v1 is kept around to ensure backwards compatibility - specifically the ability to read
    feature values for entities that have already been written into the online store.
    """

    coerce_tz_aware: Optional[bool] = True
    """ If True, coerces entity_df timestamp columns to be timezone aware (to UTC by default). """

    def __init__(self, **data: Any):
        super().__init__(**data)

        self._offline_store = None
        if "offline_store" in data:
            self._offline_config = data["offline_store"]
        else:
            if data["provider"] == "local":
                self._offline_config = "file"
            elif data["provider"] == "gcp":
                self._offline_config = "bigquery"
            elif data["provider"] == "aws":
                self._offline_config = "redshift"
            elif data["provider"] == "azure":
                self._offline_config = "mssql"

        self._online_store = None
        if "online_store" in data:
            self._online_config = data["online_store"]
        else:
            if data["provider"] == "local":
                self._online_config = "sqlite"
            elif data["provider"] == "gcp":
                self._online_config = "datastore"
            elif data["provider"] == "aws":
                self._online_config = "dynamodb"

        self._batch_engine = None
        if "batch_engine" in data:
            self._batch_engine_config = data["batch_engine"]
        elif "batch_engine_config" in data:
            self._batch_engine_config = data["batch_engine_config"]
        else:
            # Defaults to using local in-process materialization engine.
            self._batch_engine_config = "local"

        if isinstance(self.feature_server, Dict):
            self.feature_server = get_feature_server_config_from_type(
                self.feature_server["type"]
            )(**self.feature_server)

        if self.entity_key_serialization_version <= 1:
            warnings.warn(
                "`entity_key_serialization_version` is either not specified in the feature_store.yaml, "
                "or is specified to a value <= 1."
                "This serialization version may cause errors when trying to write fields with the `Long` data type"
                " into the online store. Specifying `entity_key_serialization_version` to 2 is recommended for"
                " new projects. ",
                RuntimeWarning,
            )

    def get_registry_config(self):
        if isinstance(self.registry, str):
            return RegistryConfig(path=self.registry)
        else:
            return self.registry

    @property
    def offline_store(self):
        if not self._offline_store:
            if isinstance(self._offline_config, Dict):
                self._offline_store = get_offline_config_from_type(
                    self._offline_config["type"]
                )(**self._offline_config)
            elif isinstance(self._offline_config, str):
                self._offline_store = get_offline_config_from_type(
                    self._offline_config
                )()
            elif self._offline_config:
                self._offline_store = self._offline_config
        return self._offline_store

    @property
    def online_store(self):
        if not self._online_store:
            if isinstance(self._online_config, Dict):
                self._online_store = get_online_config_from_type(
                    self._online_config["type"]
                )(**self._online_config)
            elif isinstance(self._online_config, str):
                self._online_store = get_online_config_from_type(self._online_config)()
            elif self._online_config:
                self._online_store = self._online_config

        return self._online_store

    @property
    def batch_engine(self):
        if not self._batch_engine:
            if isinstance(self._batch_engine_config, Dict):
                self._batch_engine = get_batch_engine_config_from_type(
                    self._batch_engine_config["type"]
                )(**self._batch_engine_config)
            elif isinstance(self._batch_engine_config, str):
                self._batch_engine = get_batch_engine_config_from_type(
                    self._batch_engine_config
                )()
            elif self._batch_engine_config:
                self._batch_engine = self._batch_engine

        return self._batch_engine

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

        # Skip if we aren't creating the configuration from a dict or online store is null or it is a string like "None" or "null"
        if not isinstance(values["online_store"], Dict):
            if isinstance(values["online_store"], str) and values[
                "online_store"
            ].lower() in {"none", "null"}:
                values["online_store"] = None
            return values

        # Make sure that the provider configuration is set. We need it to set the defaults
        if "provider" not in values:
            raise FeastProviderNotSetError()

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
                [ErrorWrapper(e, loc="online_store")],
                model=RepoConfig,
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
        if "provider" not in values:
            raise FeastProviderNotSetError()

        # Set the default type
        if "type" not in values["offline_store"]:
            if values["provider"] == "local":
                values["offline_store"]["type"] = "file"
            elif values["provider"] == "gcp":
                values["offline_store"]["type"] = "bigquery"
            elif values["provider"] == "aws":
                values["offline_store"]["type"] = "redshift"
            if values["provider"] == "azure":
                values["offline_store"]["type"] = "mssql"

        offline_store_type = values["offline_store"]["type"]

        # Validate the dict to ensure one of the union types match
        try:
            offline_config_class = get_offline_config_from_type(offline_store_type)
            offline_config_class(**values["offline_store"])
        except ValidationError as e:
            raise ValidationError(
                [ErrorWrapper(e, loc="offline_store")],
                model=RepoConfig,
            )

        return values

    @root_validator(pre=True)
    def _validate_feature_server_config(cls, values):
        # Having no feature server is the default.
        if "feature_server" not in values:
            return values

        # Skip if we aren't creating the configuration from a dict
        if not isinstance(values["feature_server"], Dict):
            return values

        # Make sure that the provider configuration is set. We need it to set the defaults
        if "provider" not in values:
            raise FeastProviderNotSetError()

        default_type = FEATURE_SERVER_TYPE_FOR_PROVIDER.get(values["provider"])
        defined_type = values["feature_server"].get("type", default_type)
        # Make sure that the type is either not set, or set correctly, since it's defined by the provider
        if defined_type not in (default_type, "local"):
            raise FeastFeatureServerTypeSetError(defined_type)
        values["feature_server"]["type"] = defined_type

        # Validate the dict to ensure one of the union types match
        try:
            feature_server_config_class = get_feature_server_config_from_type(
                defined_type
            )
            feature_server_config_class(**values["feature_server"])
        except ValidationError as e:
            raise ValidationError(
                [ErrorWrapper(e, loc="feature_server")],
                model=RepoConfig,
            )

        return values

    @validator("project")
    def _validate_project_name(cls, v):
        from feast.repo_operations import is_valid_name

        if not is_valid_name(v):
            raise ValueError(
                f"Project name, {v}, should only have "
                f"alphanumerical values and underscores but not start with an underscore."
            )
        return v

    @validator("flags")
    def _validate_flags(cls, v):
        if not isinstance(v, Dict):
            return

        _logger.warning(
            "Flags are no longer necessary in Feast. Experimental features will log warnings instead."
        )

        return v

    def write_to_path(self, repo_path: Path):
        config_path = repo_path / "feature_store.yaml"
        with open(config_path, mode="w") as f:
            yaml.dump(
                yaml.safe_load(
                    self.json(
                        exclude={"repo_path"},
                        exclude_unset=True,
                    )
                ),
                f,
                sort_keys=False,
            )

    class Config:
        allow_population_by_field_name = True


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
    return import_class(module_name, config_class_name, "DataSource")


def get_batch_engine_config_from_type(batch_engine_type: str):
    if batch_engine_type in BATCH_ENGINE_CLASS_FOR_TYPE:
        batch_engine_type = BATCH_ENGINE_CLASS_FOR_TYPE[batch_engine_type]
    else:
        assert batch_engine_type.endswith("Engine")
    module_name, batch_engine_class_type = batch_engine_type.rsplit(".", 1)
    config_class_name = f"{batch_engine_class_type}Config"

    return import_class(module_name, config_class_name, config_class_name)


def get_online_config_from_type(online_store_type: str):
    if online_store_type in ONLINE_STORE_CLASS_FOR_TYPE:
        online_store_type = ONLINE_STORE_CLASS_FOR_TYPE[online_store_type]
    elif not online_store_type.endswith("OnlineStore"):
        raise FeastOnlineStoreInvalidName(online_store_type)
    module_name, online_store_class_type = online_store_type.rsplit(".", 1)
    config_class_name = f"{online_store_class_type}Config"

    return import_class(module_name, config_class_name, config_class_name)


def get_offline_config_from_type(offline_store_type: str):
    if offline_store_type in OFFLINE_STORE_CLASS_FOR_TYPE:
        offline_store_type = OFFLINE_STORE_CLASS_FOR_TYPE[offline_store_type]
    elif not offline_store_type.endswith("OfflineStore"):
        raise FeastOfflineStoreInvalidName(offline_store_type)
    module_name, offline_store_class_type = offline_store_type.rsplit(".", 1)
    config_class_name = f"{offline_store_class_type}Config"

    return import_class(module_name, config_class_name, config_class_name)


def get_feature_server_config_from_type(feature_server_type: str):
    # We do not support custom feature servers right now.
    if feature_server_type not in FEATURE_SERVER_CONFIG_CLASS_FOR_TYPE:
        raise FeastFeatureServerTypeInvalidError(feature_server_type)

    feature_server_type = FEATURE_SERVER_CONFIG_CLASS_FOR_TYPE[feature_server_type]
    module_name, config_class_name = feature_server_type.rsplit(".", 1)
    return import_class(module_name, config_class_name, config_class_name)


def load_repo_config(repo_path: Path, fs_yaml_file: Path) -> RepoConfig:
    config_path = fs_yaml_file

    with open(config_path) as f:
        raw_config = yaml.safe_load(os.path.expandvars(f.read()))
        try:
            c = RepoConfig(**raw_config)
            c.repo_path = repo_path
            return c
        except ValidationError as e:
            raise FeastConfigError(e, config_path)
