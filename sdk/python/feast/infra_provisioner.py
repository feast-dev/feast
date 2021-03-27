import abc
from typing import Sequence, Union

from feast import FeatureTable, FeatureView
from feast.repo_config import RepoConfig


class InfraProvisioner(abc.ABC):
    """
    An abstract interface for cloud infrastructure provisioner. Provisioner
    is responsible for creating cloud resources (tables, endpoints) that Feast objects
    may require.
    """

    @abc.abstractmethod
    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_have: Sequence[Union[FeatureTable, FeatureView]],
        partial: bool,
    ):
        """
        Reconcile cloud resources with the objects declared in the feature repo.

        Args:
            tables_to_delete: Tables that were deleted from the feature repo, so
                provisioner needs to clean up the corresponding cloud resources.
            tables_to_have: Tables that are still in the feature repo. Depending on
                implementation, provisioner may or may not need to update the
                corresponding resources, or create new ones if the tables were just
                created.
            partial: if true, then tables_to_delete and tables_to_have are *not*
                exhaustive lists. There may be other tables that are not touched by
                this update.
        """
        ...

    @abc.abstractmethod
    def teardown_infra(
        self, project: str, tables: Sequence[Union[FeatureTable, FeatureView]]
    ):
        """
        Tear down all cloud resources for a repo.

        Args:
            tables: Tables that are declared in the feature repo.
        """
        ...


def get_provisioner(config: RepoConfig) -> InfraProvisioner:
    if config.provider == "gcp":
        from feast.infra.gcp import Gcp

        return Gcp(config.online_store.datastore if config.online_store else None)
    elif config.provider == "local":
        from feast.infra.local_sqlite import LocalSqlite

        assert config.online_store is not None
        assert config.online_store.local is not None
        return LocalSqlite(config.online_store.local)
    else:
        raise ValueError(config)
