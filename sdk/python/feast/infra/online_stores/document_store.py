from abc import abstractmethod
from datetime import datetime
from feast.feature_view import FeatureView
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig, FeastConfigBaseModel
from infra.online_stores.online_store import OnlineStore
from typing import Optional, List, Tuple, Dict
import numpy as np


class DocumentStoreIndexConfig(FeastConfigBaseModel):
    embedding_type: Optional[str]


class DocumentStore(OnlineStore):
    index: Optional[str]

    @abstractmethod
    def online_search(self,
                      config: RepoConfig,
                      table: FeatureView,
                      requested_feature: str,
                      embeddings: np.ndarray,
                      top_k: int,
                      ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        raise NotImplementedError(
            "You have to implement this!"
        )

    @abstractmethod
    def create_index(self,
                     config: RepoConfig,
                     index: str,
                     index_config: DocumentStoreIndexConfig
                     ):
        raise NotImplementedError(
            "You have to implement this!"
        )
