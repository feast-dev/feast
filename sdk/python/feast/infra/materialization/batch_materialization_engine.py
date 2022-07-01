import dataclasses
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable, List, Optional

from tqdm import tqdm

from feast import RepoConfig
from feast.base_feature_view import BaseFeatureView
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.registry import BaseRegistry


@dataclasses.dataclass
class MaterializationTask:
    project: str
    feature_view: BaseFeatureView
    start_time: datetime
    end_time: datetime
    tqdm_builder: Callable[[int], tqdm]


class MaterializationJob(ABC):
    task: MaterializationTask

    @abstractmethod
    def status(self) -> str:
        ...

    @abstractmethod
    def should_be_retried(self) -> str:
        ...

    @abstractmethod
    def job_id(self) -> str:
        ...

    @abstractmethod
    def url(self) -> Optional[str]:
        ...


class BatchMaterializationEngine(ABC):
    def __init__(
        self,
        *,
        repo_config: RepoConfig,
        registry: BaseRegistry,
        offline_store: OfflineStore,
        online_store: OnlineStore,
        **kwargs,
    ):
        self.repo_config = repo_config
        self.registry = registry
        self.offline_store = offline_store
        self.online_store = online_store

    @abstractmethod
    def materialize(self, tasks: List[MaterializationTask]) -> List[MaterializationJob]:
        ...
