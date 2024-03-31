from abc import abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np

from feast.feature_view import FeatureView
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.infra.online_stores.online_store import OnlineStore


class DocumentStoreIndexConfig(FeastConfigBaseModel):
    index_type: Optional[str]
    embedding_type: Optional[str]


class DocumentStore(OnlineStore):

    @abstractmethod
    def online_search(
            self,
            config: RepoConfig,
            table: FeatureView,
            requested_feature: str,
            embeddings: np.ndarray,
            top_k: int,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        raise NotImplementedError("You have to implement this!")