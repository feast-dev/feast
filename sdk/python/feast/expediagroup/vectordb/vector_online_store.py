from abc import abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from feast import Entity, RepoConfig
from feast.expediagroup.vectordb.vector_feature_view import VectorFeatureView
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


class VectorOnlineStore(OnlineStore):
    """
    Abstraction for vector database implementations of the online store interface. Any online store implementation of
    a vector database should inherit this class.
    """

    @abstractmethod
    def online_write_batch(
        self,
        config: RepoConfig,
        table: VectorFeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        # to be implemented in inheriting class
        pass

    @abstractmethod
    def online_read(
        self,
        config: RepoConfig,
        table: VectorFeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        # to be implemented in inheriting class
        pass

    @abstractmethod
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[VectorFeatureView],
        tables_to_keep: Sequence[VectorFeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        # to be implemented in inheriting class
        pass

    @abstractmethod
    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[VectorFeatureView],
        entities: Sequence[Entity],
    ):
        # to be implemented in inheriting class
        pass
