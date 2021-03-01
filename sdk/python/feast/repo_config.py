from pathlib import Path
from typing import NamedTuple, Optional

import yaml
from bindr import bind


class FirestoreConfig(NamedTuple):
    dummy: Optional[str] = None


class OnlineStoreConfig(NamedTuple):
    type: str
    firestore: Optional[FirestoreConfig] = None


class RepoConfig(NamedTuple):
    metadata_store: str
    project: str
    provider: str
    online_store: OnlineStoreConfig


def load_repo_config(repo_path: Path) -> RepoConfig:
    with open(repo_path / "feature_store.yaml") as f:
        raw_config = yaml.safe_load(f)
        return bind(RepoConfig, raw_config)
