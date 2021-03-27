import abc
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from feast import FeatureTable, FeatureView
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig


class OnlineStore(abc.ABC):
    @abc.abstractmethod
    def online_write_batch(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Write a batch of feature rows to the online store. This is a low level interface, not
        expected to be used by the users directly.

        If a tz-naive timestamp is passed to this method, it is assumed to be UTC.

        Args:
            project: Feast project name
            table: Feast FeatureTable
            data: a list of quadruplets containing Feature data. Each quadruplet contains an Entity Key,
                a dict containing feature values, an event timestamp for the row, and
                the created timestamp for the row if it exists.
            progress: Optional function to be called once every mini-batch of rows is written to
                the online store. Can be used to display progress.
        """
        ...

    @abc.abstractmethod
    def online_read(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Read feature values given an Entity Key. This is a low level interface, not
        expected to be used by the users directly.

        Returns:
            Data is returned as a list, one item per entity key. Each item in the list is a tuple
            of event_ts for the row, and the feature data as a dict from feature names to values.
            Values are returned as Value proto message.
        """
        ...


def get_online_store(config: RepoConfig) -> OnlineStore:
    if config.provider == "gcp":
        from feast.infra.gcp import OnlineStoreDatastore

        return OnlineStoreDatastore(
            config.online_store.datastore if config.online_store else None
        )
    elif config.provider == "local":
        from feast.infra.local_sqlite import OnlineStoreSqlite

        assert config.online_store is not None
        assert config.online_store.local is not None
        return OnlineStoreSqlite(config.online_store.local)
    else:
        raise ValueError(config)
