from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic.typing import Literal

from feast import Entity, RepoConfig
from feast.expediagroup.vectordb.vector_feature_view import VectorFeatureView
from feast.expediagroup.vectordb.vector_online_store import VectorOnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel


class MilvusOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for the Milvus online store"""

    type: Literal["milvus"] = "milvus"
    """Online store type selector"""

    host: str
    """ the host URL """

    port: int = 19530
    """ the port to connect to a Milvus instance. Should be the one used for GRPC (default: 19530) """


class MilvusOnlineStore(VectorOnlineStore):
    def online_write_batch(
        self,
        config: RepoConfig,
        table: VectorFeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7971"
        )

    def online_read(
        self,
        config: RepoConfig,
        table: VectorFeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7972"
        )

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[VectorFeatureView],
        tables_to_keep: Sequence[VectorFeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7970"
        )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[VectorFeatureView],
        entities: Sequence[Entity],
    ):
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7974"
        )
