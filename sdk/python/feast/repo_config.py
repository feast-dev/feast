from pathlib import Path
from typing import NamedTuple, Optional

import yaml
from bindr import bind
from jsonschema import ValidationError, validate


class LocalOnlineStoreConfig(NamedTuple):
    """ Online store config for local (SQLite-based) online store """

    path: str
    """ str: Path to sqlite db """


class DatastoreOnlineStoreConfig(NamedTuple):
    """ Online store config for GCP Datastore """

    project_id: str
    """ str: GCP Project Id """


class OnlineStoreConfig(NamedTuple):
    datastore: Optional[DatastoreOnlineStoreConfig] = None
    """ DatastoreOnlineStoreConfig: Optional DatastoreConfig """
    local: Optional[LocalOnlineStoreConfig] = None
    """ LocalOnlineStoreConfig: Optional local online store config """


class RepoConfig(NamedTuple):
    """ Repo config. Typically loaded from `feature_store.yaml` """

    metadata_store: str
    """ str: Path to metadata store. Can be a local path, or remote object storage path, e.g. gcs://foo/bar """
    project: str
    """ str: Feast project id. This can be any alphanumeric string up to 16 characters.
        You can have multiple independent feature repositories deployed to the same cloud
        provider account, as long as they have different project ids.
    """
    provider: str
    """ str: local or gcp """
    online_store: Optional[OnlineStoreConfig] = None
    """ OnlineStoreConfig: Online store configuration (optional depending on provider) """


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
        "metadata_store": {"type": "string"},
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
    def __init__(self, error_message, error_path, config_path):
        self._error_message = error_message
        self._error_path = error_path
        self._config_path = config_path
        super().__init__(self._error_message)

    def __str__(self) -> str:
        if self._error_path:
            return f'{self._error_message} under {"->".join(self._error_path)} in {self._config_path}'
        else:
            return f"{self._error_message} in {self._config_path}"

    def __repr__(self) -> str:
        return f"FeastConfigError({repr(self._error_message)}, {repr(self._error_path)}, {repr(self._config_path)})"


def load_repo_config(repo_path: Path) -> RepoConfig:
    config_path = repo_path / "feature_store.yaml"
    with open(config_path) as f:
        raw_config = yaml.safe_load(f)
        try:
            validate(raw_config, config_schema)
            return bind(RepoConfig, raw_config)
        except ValidationError as e:
            raise FeastConfigError(e.message, e.absolute_path, config_path)
