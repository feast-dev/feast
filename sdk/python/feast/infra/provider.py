import abc
from typing import List

from feast import FeatureTable
from feast.repo_config import RepoConfig


class Provider(abc.ABC):
    @abc.abstractmethod
    def update_infra(
        self,
        project: str,
        tables_to_delete: List[FeatureTable],
        tables_to_keep: List[FeatureTable],
    ):
        """
        Reconcile cloud resources with the objects declared in the feature repo.

        Args:
            tables_to_delete: Tables that were deleted from the feature repo, so provider needs to
                clean up the corresponding cloud resources.
            tables_to_keep: Tables that are still in the feature repo. Depending on implementation,
                provider may or may not need to update the corresponding resources.
        """
        ...

    @abc.abstractmethod
    def teardown_infra(self, project: str, tables: List[FeatureTable]):
        """
        Tear down all cloud resources for a repo.

        Args:
            tables: Tables that are declared in the feature repo.
        """
        ...


def get_provider(config: RepoConfig) -> Provider:
    if config.provider == "gcp":
        from feast.infra.gcp import Gcp

        return Gcp(config.online_store.firestore)
    elif config.provider == "local":
        from feast.infra.local_sqlite import LocalSqlite

        assert config.online_store.local is not None
        return LocalSqlite(config.online_store.local)
    else:
        raise ValueError(config)
