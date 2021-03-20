import abc
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

from feast import FeatureTable, FeatureView
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig


class Provider(abc.ABC):
    @abc.abstractmethod
    def update_infra(
        self,
        project: str,
        tables_to_delete: List[Union[FeatureTable, FeatureView]],
        tables_to_keep: List[Union[FeatureTable, FeatureView]],
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
    def teardown_infra(
        self, project: str, tables: List[Union[FeatureTable, FeatureView]]
    ):
        """
        Tear down all cloud resources for a repo.

        Args:
            tables: Tables that are declared in the feature repo.
        """
        ...

    @abc.abstractmethod
    def online_write_batch(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        data: List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime]],
        created_ts: datetime,
    ) -> None:
        """
        Write a batch of feature rows to the online store. This is a low level interface, not
        expected to be used by the users directly.

        If a tz-naive timestamp is passed to this method, it is assumed to be UTC.

        Args:
            project: Feast project name
            table: Feast FeatureTable
            data: a list of triplets containing Feature data. Each triplet contains an Entity Key,
                a dict containing feature values, and event timestamp for the row.
            created_ts: the created timestamp (typically set to current time), same value used for
                all rows.
        """
        ...

    @abc.abstractmethod
    def online_read(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        entity_key: EntityKeyProto,
    ) -> Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]:
        """
        Read feature values given an Entity Key. This is a low level interface, not
        expected to be used by the users directly.

        Returns:
            A tuple of event_ts for the row, and the feature data as a dict from feature names
            to values. Values are returned as Value proto message.
        """
        ...


def get_provider(config: RepoConfig) -> Provider:
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
