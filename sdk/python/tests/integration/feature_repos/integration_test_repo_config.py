from dataclasses import dataclass
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


@dataclass(frozen=False)
class IntegrationTestRepoConfig:
    """
    This class should hold all possible parameters that may need to be varied by individual tests.
    """

    provider: str = "local"
    online_store: Optional[Union[str, Dict]] = "sqlite"

    offline_store_creator: Type[DataSourceCreator] = FileDataSourceCreator
    online_store_creator: Optional[Type[OnlineStoreCreator]] = None

    full_feature_names: bool = True
    infer_features: bool = False
    python_feature_server: bool = False
    go_feature_retrieval: bool = False

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
            ]
        )
