import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Type, Union

from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.file import (
    FileDataSourceCreator,
)
from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class RegistryLocation(Enum):
    Local = 1
    S3 = 2


@dataclass(frozen=False)
class IntegrationTestRepoConfig:
    """
    This class should hold all possible parameters that may need to be varied by individual tests.
    """

    provider: str = "local"
    online_store: Optional[Union[str, Dict]] = "sqlite"

    offline_store_creator: Type[DataSourceCreator] = FileDataSourceCreator
    online_store_creator: Optional[Type[OnlineStoreCreator]] = None

    batch_engine: Optional[Union[str, Dict]] = "local"
    registry_location: RegistryLocation = RegistryLocation.Local

    full_feature_names: bool = True
    infer_features: bool = False
    python_feature_server: bool = False

    def __repr__(self) -> str:
        if not self.online_store_creator:
            if isinstance(self.online_store, str):
                online_store_type = self.online_store
            elif isinstance(self.online_store, dict):
                if self.online_store["type"] == "redis":
                    online_store_type = self.online_store.get("redis_type", "redis")
                else:
                    online_store_type = self.online_store["type"]
            elif self.online_store:
                online_store_type = self.online_store.__name__
            else:
                online_store_type = "none"
        else:
            online_store_type = self.online_store_creator.__name__

        return ":".join(
            [
                f"{self.provider.upper()}",
                f"{self.offline_store_creator.__name__.split('.')[-1].replace('DataSourceCreator', '')}",
                online_store_type,
                f"python_fs:{self.python_feature_server}",
            ]
        )

    def __hash__(self):
        return int(hashlib.sha1(repr(self).encode()).hexdigest(), 16)

    def __eq__(self, other):
        if not isinstance(other, IntegrationTestRepoConfig):
            return False

        return (
            self.provider == other.provider
            and self.online_store == other.online_store
            and self.offline_store_creator == other.offline_store_creator
            and self.online_store_creator == other.online_store_creator
            and self.python_feature_server == other.python_feature_server
        )
