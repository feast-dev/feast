from abc import ABC, abstractmethod


class OnlineStoreCreator(ABC):
    def __init__(self, project_name: str, **kwargs):
        self.project_name = project_name

    @abstractmethod
    def create_online_store(self):
        ...

    @abstractmethod
    def teardown(self):
        ...
