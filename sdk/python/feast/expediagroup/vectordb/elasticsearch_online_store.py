import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from bidict import bidict
from elasticsearch import Elasticsearch
from pydantic.typing import Literal

from feast import Entity, FeatureView, RepoConfig
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.types import (
    Bool,
    Bytes,
    FeastType,
    Float32,
    Float64,
    Int32,
    Int64,
    Invalid,
    String,
    UnixTimestamp,
)

logger = logging.getLogger(__name__)

TYPE_MAPPING = bidict(
    {
        Bytes: "binary",
        Int32: "integer",
        Int64: "long",
        Float32: "float",
        Float64: "double",
        Bool: "boolean",
        String: "text",
        UnixTimestamp: "date_nanos",
        Invalid: None,
    }
)


class ElasticsearchOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for the Elasticsearch online store"""

    type: Literal["elasticsearch"] = "elasticsearch"
    """Online store type selector"""

    endpoint: str
    """ the http endpoint URL """

    username: str
    """ username to connect to Elasticsearch """

    password: str
    """ password to connect to Elasticsearch """

    token: str
    """ bearer token for authentication """


class ElasticsearchConnectionManager:
    def __init__(self, online_config: RepoConfig):
        self.online_config = online_config

    def __enter__(self):
        # Connecting to Elasticsearch
        logger.info(
            f"Connecting to Elasticsearch with endpoint {self.online_config.endpoint}"
        )
        if len(self.online_config.token) > 0:
            self.client = Elasticsearch(
                self.online_config.endpoint, bearer_auth=self.online_config.token
            )
        else:
            self.client = Elasticsearch(
                self.online_config.endpoint,
                basic_auth=(self.online_config.username, self.online_config.password),
            )
        return self.client

    def __exit__(self, exc_type, exc_value, traceback):
        # Disconnecting from Elasticsearch
        logger.info("Closing the connection to Elasticsearch")
        self.client.transport.close()


class ElasticsearchOnlineStore(OnlineStore):
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        with ElasticsearchConnectionManager(config) as es:
            pass

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        pass

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        with ElasticsearchConnectionManager(config) as es:
            for fv in tables_to_delete:
                resp = es.indices.exists(index=fv.name)
                if resp.body:
                    es.indices.delete(index=fv.name)
            for fv in tables_to_keep:
                resp = es.indices.exists(index=fv.name)
                if not resp.body:
                    self._create_index(es, fv)

    def _create_index(self, es, fv):
        index_mapping = {"properties": {}}
        for feature in fv.schema:
            is_primary = True if feature.name in fv.join_keys else False
            if "index_type" in feature.tags:
                dimensions = int(feature.tags.get("dimensions", "0"))
                metric_type = feature.tags.get("metric_type", "l2_norm")
                index_mapping["properties"][feature.name] = {
                    "type": "dense_vector",
                    "dims": dimensions,
                    "index": True,
                    "similarity": metric_type,
                }
                index_params = json.loads(feature.tags.get("index_params", "{}"))
                if len(index_params) > 0:
                    index_params["type"] = feature.tags.get(
                        "index_type", "hnsw"
                    ).lower()
                    index_mapping["properties"][feature.name][
                        "index_options"
                    ] = index_params
            else:
                t = self.get_data_type(feature.dtype)
                t = "keyword" if is_primary and t == "text" else t
                index_mapping["properties"][feature.name] = {"type": t}
                if is_primary:
                    index_mapping["properties"][feature.name]["index"] = True
        es.indices.create(index=fv.name, mappings=index_mapping)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        pass

    def get_data_type(self, t: FeastType) -> str:
        return TYPE_MAPPING.get(t)
