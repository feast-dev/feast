from abc import ABC

from feast.repo_config import FeastConfigBaseModel


class OnlineStoreCreator(ABC):
    def __init__(self, project_name: str, **kwargs):
        self.project_name = project_name

    def create_online_store(self) -> FeastConfigBaseModel:
        ...

    def teardown(self):
        ...
