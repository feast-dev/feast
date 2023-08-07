import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic.typing import Literal
from pymilvus import (
    Collection,
    connections,
    utility,
)

from feast import Entity, RepoConfig
from feast.expediagroup.vectordb.vector_feature_view import VectorFeatureView
from feast.expediagroup.vectordb.vector_online_store import VectorOnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel

logger = logging.getLogger(__name__)


class MilvusOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for the Milvus online store"""

    type: Literal["milvus"] = "milvus"
    """Online store type selector"""

    host: str
    """ the host URL """

    port: int = 19530
    """ the port to connect to a Milvus instance. Should be the one used for GRPC (default: 19530) """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Establish the Milvus connection using the provided host and port
        connections.connect(host=self.host, port=self.port, use_secure=True)


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
        for table_to_keep in tables_to_keep:
            try:
                Collection(name=table_to_keep.name, schema=table_to_keep.schema)
                logger.info(
                    f"Collection {table_to_keep.name} has been updated successfully."
                )
            except Exception as e:
                logger.error(f"Collection update failed due to {e}")

        for table_to_delete in tables_to_delete:
            collection_available = utility.has_collection(table_to_delete.name)
            try:
                if collection_available:
                    utility.drop_collection(table_to_delete.name)
                    logger.info(
                        f"Collection {table_to_keep.name} has been deleted successfully."
                    )
                else:
                    return logger.error(
                        "Collection does not exist or is already deleted."
                    )
            except Exception as e:
                logger.error(f"Collection deletion failed due to {e}")

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[VectorFeatureView],
        entities: Sequence[Entity],
    ):
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7974"
        )
