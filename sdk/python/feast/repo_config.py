from pathlib import Path
from typing import NamedTuple, Optional

import yaml
from bindr import bind


class LocalOnlineStoreConfig(NamedTuple):
    path: str


class DatastoreOnlineStoreConfig(NamedTuple):
    project_id: str


class OnlineStoreConfig(NamedTuple):
    datastore: Optional[DatastoreOnlineStoreConfig] = None
    local: Optional[LocalOnlineStoreConfig] = None


class RepoConfig(NamedTuple):
    metadata_store: str = "./metadata_store"
    project: str = "default"
    provider: str = "local"
    online_store: OnlineStoreConfig = OnlineStoreConfig()


def load_repo_config(repo_path: Path) -> RepoConfig:
    with open(repo_path / "feature_store.yaml") as f:
        raw_config = yaml.safe_load(f)
        return bind(RepoConfig, raw_config)
