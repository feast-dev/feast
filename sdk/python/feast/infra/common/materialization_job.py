import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional, Union

from tqdm import tqdm

from feast import BatchFeatureView, FeatureView, StreamFeatureView


@dataclass
class MaterializationTask:
    """
    A MaterializationTask represents a unit of data that needs to be materialized from an
    offline store to an online store.
    """

    project: str
    feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView]
    start_time: datetime
    end_time: datetime
    only_latest: bool = True
    tqdm_builder: Union[None, Callable[[int], tqdm]] = None
    disable_event_timestamp: bool = False


class MaterializationJobStatus(enum.Enum):
    WAITING = 1
    RUNNING = 2
    AVAILABLE = 3
    ERROR = 4
    CANCELLING = 5
    CANCELLED = 6
    SUCCEEDED = 7
    PAUSED = 8
    RETRYING = 9


class MaterializationJob(ABC):
    """
    A MaterializationJob represents an ongoing or executed process that materializes data as per the
    definition of a materialization task.
    """

    task: MaterializationTask

    @abstractmethod
    def status(self) -> MaterializationJobStatus: ...

    @abstractmethod
    def error(self) -> Optional[BaseException]: ...

    @abstractmethod
    def should_be_retried(self) -> bool: ...

    @abstractmethod
    def job_id(self) -> str: ...

    @abstractmethod
    def url(self) -> Optional[str]: ...
