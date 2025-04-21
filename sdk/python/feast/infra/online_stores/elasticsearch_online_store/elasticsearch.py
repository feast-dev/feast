from __future__ import absolute_import

import base64
import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from elasticsearch import Elasticsearch, helpers

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import (
    get_list_val_str,
    serialize_entity_key,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.utils import (
    _build_retrieve_online_document_record,
    _get_feature_view_vector_field_metadata,
    to_naive_utc,
)


class ElasticSearchOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """
    Configuration for the ElasticSearch online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """

    type: str = "elasticsearch"

    host: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: Optional[int] = None
    index: Optional[str] = None
    scheme: Optional[str] = "http"

    # The number of rows to write in a single batch
    write_batch_size: Optional[int] = 40


class ElasticSearchOnlineStore(OnlineStore):
    _client: Optional[Elasticsearch] = None

    def _get_client(self, config: RepoConfig) -> Elasticsearch:
        online_store_config = config.online_store
        assert isinstance(online_store_config, ElasticSearchOnlineStoreConfig)

        user = online_store_config.user if online_store_config.user is not None else ""
        password = (
            online_store_config.password
            if online_store_config.password is not None
            else ""
        )

        if self._client:
            return self._client
        else:
            self._client = Elasticsearch(
                hosts=[
                    {
                        "host": online_store_config.host or "localhost",
                        "port": online_store_config.port or 9200,
                        "scheme": online_store_config.scheme or "http",
                    }
                ],
                basic_auth=(user, password),
            )
            return self._client

    def _bulk_batch_actions(self, table: FeatureView, batch: List[Dict[str, Any]]):
        for row in batch:
            yield {
                "_index": table.name,
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
            encoded_entity_key = base64.b64encode(entity_key_bin).decode("utf-8")
            timestamp = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)
            for feature_name, value in values.items():
                encoded_value = base64.b64encode(value.SerializeToString()).decode(
                    "utf-8"
                )
                vector_val = json.loads(get_list_val_str(value))
                insert_values.append(
                    {
                        "entity_key": encoded_entity_key,
                        "feature_name": feature_name,
                        "feature_value": encoded_value,
                        "timestamp": timestamp,
                        "created_ts": created_ts,
                        "vector_value": vector_val,
                    }
                )

        batch_size = config.online_store.write_batch_size
        for i in range(0, len(insert_values), batch_size):
            batch = insert_values[i : i + batch_size]
            actions = self._bulk_batch_actions(table, batch)
            helpers.bulk(self._get_client(config), actions)

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
        response = self._get_client(config).search(index=table.name, body=body)
        results: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for hit in response["hits"]["hits"]:
            results.append(
                (
                    hit["_source"]["timestamp"],
                    {hit["_source"]["feature_name"]: hit["_source"]["feature_value"]},
                )
            )
        return results

    def create_index(self, config: RepoConfig, table: FeatureView):
        """
        Create an index in ElasticSearch for the given table.
        TODO: This method can be exposed to users to customize the indexing functionality.
        Args:
            config: Feast repo configuration object.
            table: FeatureView table for which the index needs to be created.
        """
        vector_field_length = getattr(
            _get_feature_view_vector_field_metadata(table), "vector_len", 512
        )

        index_mapping = {
            "properties": {
                "entity_key": {"type": "binary"},
                "feature_name": {"type": "keyword"},
                "feature_value": {"type": "binary"},
                "timestamp": {"type": "date"},
                "created_ts": {"type": "date"},
                "vector_value": {
                    "type": "dense_vector",
                    "dims": vector_field_length,
                    "index": "true",
                    "similarity": config.online_store.similarity,
                },
            }
        }
        self._get_client(config).indices.create(
            index=table.name, mappings=index_mapping
        )

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
            self._get_client(config).delete_by_query(index=table.name)
        for table in tables_to_keep:
            self.create_index(config, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        project = config.project
        try:
            for table in tables:
                self._get_client(config).indices.delete(index=table.name)
        except Exception as e:
            logging.exception(f"Error deleting index in project {project}: {e}")
            raise

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: List[float],
        top_k: int,
        *args,
        **kwargs,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        result: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[ValueProto],
                Optional[ValueProto],
                Optional[ValueProto],
            ]
        ] = []
        response = self._get_client(config).search(
            index=table.name,
            knn={
                "field": "vector_value",
                "query_vector": embedding,
                "k": top_k,
            },
        )
        rows = response["hits"]["hits"][0:top_k]
        for row in rows:
            entity_key = row["_source"]["entity_key"]
            feature_value = row["_source"]["feature_value"]
            vector_value = row["_source"]["vector_value"]
            timestamp = row["_source"]["timestamp"]
            distance = row["_score"]
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")

            result.append(
                _build_retrieve_online_document_record(
                    entity_key,
                    base64.b64decode(feature_value),
                    str(vector_value),
                    distance,
                    timestamp,
                    config.entity_key_serialization_version,
                )
            )
        return result
