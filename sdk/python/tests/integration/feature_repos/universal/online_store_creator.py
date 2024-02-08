from abc import ABC, abstractmethod
from feast.repo_config import FeastConfigBaseModel


class OnlineStoreCreator(ABC):
    def __init__(self, project_name: str, **kwargs):
        self.project_name = project_name

    @abstractmethod
    def create_online_store(self) -> FeastConfigBaseModel:
        raise NotImplementedError

    @abstractmethod
    def teardown(self):
        raise NotImplementedError
