import dataclasses
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable, List, Optional, Sequence, Union

from tqdm import tqdm

from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView


@dataclasses.dataclass
class MaterializationTask:
    project: str
    feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView]
    start_time: datetime
    end_time: datetime
    tqdm_builder: Callable[[int], tqdm]


class MaterializationJob(ABC):
    task: MaterializationTask

    @abstractmethod
    def status(self) -> str:
        ...

    @abstractmethod
    def should_be_retried(self) -> bool:
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
        offline_store: OfflineStore,
        online_store: OnlineStore,
        **kwargs,
    ):
        self.repo_config = repo_config
        self.offline_store = offline_store
        self.online_store = online_store

    @abstractmethod
    def update(
        self,
        project: str,
        views_to_delete: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
    ):
        ...

    @abstractmethod
    def materialize(
        self, registry, tasks: List[MaterializationTask]
    ) -> List[MaterializationJob]:
        ...

    @abstractmethod
    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        ...
