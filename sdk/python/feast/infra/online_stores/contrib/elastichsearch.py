from __future__ import absolute_import

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pytz
from elasticsearch import Elasticsearch, helpers

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import get_list_val_str, serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel


class ElasticsearchOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the Elasticsearch online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """

    type: str = "elasticsearch"

    host: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: Optional[int] = None
    index: Optional[str] = None


class ElasticsearchOnlineStore(OnlineStore):
    _client: Optional[Elasticsearch] = None

    _index: Optional[str] = None

    def _get_client(self, config: RepoConfig) -> Elasticsearch:
        online_store_config = config.online_store
        assert isinstance(online_store_config, ElasticsearchOnlineStoreConfig)

        if not self._client:
            self._client = Elasticsearch(
                hosts=[
                    {
                        "host": online_store_config.host or "localhost",
                        "port": online_store_config.port or 9200,
                    }
                ],
                http_auth=(online_store_config.user, online_store_config.password),
            )

    def create_index(self, config: RepoConfig, table: FeatureView):
        pass

    def _bulk_batch_actions(self, batch):
        for row in batch:
            yield {
                "_index": self._index,
                "_id": f"{row['entity_key']}_{row['feature_name']}_{row['timestamp']}",
                "_source": row,
            }

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        insert_values = []
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            timestamp = _to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = _to_naive_utc(created_ts)
            for feature_name, value in values.items():
                vector_val = get_list_val_str(value)
                insert_values.append(
                    {
                        "entity_key": entity_key_bin,
                        "feature_name": feature_name,
                        "feature_value": value,
                        "timestamp": timestamp,
                        "created_ts": created_ts,
                        "vector_value": vector_val,
                    }
                )

        batch_size = config.online_config.batch_size
        for i in range(0, len(insert_values), batch_size):
            batch = insert_values[i : i + batch_size]
            actions = self._bulk_batch_actions(batch)
            helpers.bulk(self._client, actions)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        if not requested_features:
            body = {
                "_source": {"excludes": ["vector_value"]},
                "query": {"match": {"entity_key": entity_keys}},
            }
        else:
            body = {
                "_source": {"excludes": ["vector_value"]},
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"entity_key": entity_keys}},
                            {"terms": {"feature_name": requested_features}},
                        ]
                    }
                },
            }
        self._client.search(index=self._index, body=body)

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        # implement the update method
        for table in tables_to_delete:
            self._client.delete_by_query(index=table.name)
        for table in tables_to_keep:
            self.create_index(config, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        pass

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_feature: str,
        embedding: List[float],
        top_k: int,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        result: List[
            Tuple[
                Optional[datetime],
                Optional[ValueProto],
                Optional[ValueProto],
                Optional[ValueProto],
            ]
        ] = []
        reponse = self._client.search(
            index=self._index,
            knn={
                "field": requested_feature,
                "query_vector": embedding,
                "k": top_k,
            },
        )
        rows = reponse["hits"]["hits"][0:top_k]
        for row in rows:
            (
                entity_key,
                feature_value,
                timestamp,
                created_ts,
                vector_value,
            ) = row["_source"]
            feature_value_proto = ValueProto()
            feature_value_proto.ParseFromString(feature_value)

            vector_value_proto = ValueProto(string_val=vector_value)
            vector_value_proto.ParseFromString(vector_value)
            result.append(
                (
                    timestamp,
                    feature_value_proto,
                    None,
                    vector_value_proto,
                )
            )
        return result


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
