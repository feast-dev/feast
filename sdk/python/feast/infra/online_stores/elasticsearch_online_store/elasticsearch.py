from __future__ import absolute_import

import base64
import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from elasticsearch import Elasticsearch, helpers

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import (
    deserialize_entity_key,
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
    vector_field_path: Optional[str] = "embedding.vector_value"

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
                "_id": f"{row['entity_key']}_{row['timestamp']}",
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
        grouped_docs: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "features": {},
                "vector_value": None,
                "timestamp": None,
                "created_ts": None,
            }
        )
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            encoded_entity_key = base64.b64encode(entity_key_bin).decode("utf-8")
            timestamp = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)

            doc_key = f"{encoded_entity_key}_{timestamp}"

            for feature_name, value in values.items():
                doc = _encode_feature_value(value)
                grouped_docs[doc_key]["features"][feature_name] = doc
                grouped_docs[doc_key]["timestamp"] = timestamp
                grouped_docs[doc_key]["created_ts"] = created_ts
                grouped_docs[doc_key]["entity_key"] = encoded_entity_key

                insert_values = [
                    {
                        "entity_key": document["entity_key"],
                        "timestamp": document["timestamp"],
                        "created_ts": document["created_ts"],
                        **(document["features"] or {}),
                    }
                    for document in grouped_docs.values()
                ]

        batch_size = config.online_store.write_batch_size
        for i in range(0, len(insert_values), batch_size):
            batch = insert_values[i : i + batch_size]
            actions = self._bulk_batch_actions(table, batch)
            helpers.bulk(self._get_client(config), actions, refresh="wait_for")

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        encoded_entity_keys = [
            base64.b64encode(
                serialize_entity_key(ek, config.entity_key_serialization_version)
            ).decode("utf-8")
            for ek in entity_keys
        ]

        # Determine which fields to retrieve
        includes = ["timestamp", "created_ts", "entity_key"]
        if requested_features:
            includes.extend(requested_features)
        else:
            includes.append("*")

        body = {
            "_source": {"includes": includes, "excludes": ["*.vector_value"]},
            "query": {
                "bool": {"filter": [{"terms": {"entity_key": encoded_entity_keys}}]}
            },
        }

        response = self._get_client(config).search(index=table.name, body=body)

        results = []

        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            timestamp = source.get("timestamp")
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")

            features: Dict[str, ValueProto] = {}

            fields_to_extract = (
                requested_features if requested_features else source.keys()
            )

            for feature_name in fields_to_extract:
                if feature_name in ("timestamp", "created_ts", "entity_key"):
                    continue
                feature_obj = source.get(feature_name)
                if not feature_obj or "feature_value" not in feature_obj:
                    continue
                try:
                    features[feature_name] = _to_value_proto(feature_obj)
                except Exception as e:
                    raise ValueError(
                        f"Failed to parse feature '{feature_name}' from hit: {e}"
                    )

            results.append((timestamp, features if features else None))

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
            _get_feature_view_vector_field_metadata(table), "vector_length", 512
        )

        index_mapping = {
            "dynamic_templates": [
                {
                    "feature_objects": {
                        "match_mapping_type": "object",
                        "match": "*",
                        "mapping": {
                            "type": "object",
                            "properties": {
                                "feature_value": {"type": "binary"},
                                "value_text": {"type": "text"},
                                "vector_value": {
                                    "type": "dense_vector",
                                    "dims": vector_field_length,
                                    "index": True,
                                    "similarity": config.online_store.similarity,
                                },
                            },
                        },
                    }
                }
            ],
            "properties": {
                "entity_key": {"type": "keyword"},
                "timestamp": {"type": "date"},
                "created_ts": {"type": "date"},
            },
        }

        self._get_client(config).indices.create(
            index=table.name,
            mappings=index_mapping,
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
        vector_field_path = (
            config.online_store.vector_field_path or "embedding.vector_value"
        )
        query = {
            "script_score": {
                "query": {
                    "bool": {"filter": [{"exists": {"field": vector_field_path}}]}
                },
                "script": {
                    "source": f"cosineSimilarity(params.query_vector, '{vector_field_path}') + 1.0",
                    "params": {"query_vector": embedding},
                },
            }
        }
        body = {"size": top_k, "_source": True, "query": query}
        response = self._get_client(config).search(index=table.name, body=body)
        rows = response["hits"]["hits"][0:top_k]
        for row in rows:
            entity_key = row["_source"]["entity_key"]
            source = row["_source"]
            distance = row["_score"]

            timestamp_str = source.get("timestamp")
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")

            for feature_name in requested_features:
                feature_data = source.get(feature_name, {})
                feature_value = feature_data.get("feature_value")
                vector_value = feature_data.get("vector_value")
                result.append(
                    _build_retrieve_online_document_record(
                        base64.b64decode(entity_key),
                        base64.b64decode(feature_value),
                        str(vector_value),
                        distance,
                        timestamp,
                        config.entity_key_serialization_version,
                    )
                )
        return result

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """
        Retrieve documents using vector similarity or keyword search from Elasticsearch.
        """
        result: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []
        if not config.online_store.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config")

        if embedding is None and query_string is None:
            raise ValueError("Either embedding or query_string must be provided")

        es_index = table.name
        body: Dict[str, Any] = {
            "size": top_k,
        }
        composite_key_name = _get_composite_key_name(table)

        source_fields = requested_features.copy()
        source_fields += ["entity_key", "timestamp"]
        source_fields += composite_key_name
        body["_source"] = source_fields

        if embedding:
            similarity = (distance_metric or config.online_store.similarity).lower()
            vector_field_path = (
                config.online_store.vector_field_path or "embedding.vector_value"
            )
            if similarity == "cosine":
                script = f"cosineSimilarity(params.query_vector, '{vector_field_path}') + 1.0"
            elif similarity == "dot_product":
                script = f"dotProduct(params.query_vector, '{vector_field_path}')"
            elif similarity in ("l2", "l2_norm", "euclidean"):
                script = f"1 / (1 + l2norm(params.query_vector, '{vector_field_path}'))"
            else:
                raise ValueError(
                    f"Unsupported similarity/distance_metric: {similarity}"
                )

        # Hybrid search
        if embedding and query_string:
            body["query"] = {
                "script_score": {
                    "query": {
                        "bool": {
                            "must": [
                                {"query_string": {"query": f'"{query_string}"'}},
                                {"exists": {"field": vector_field_path}},
                            ]
                        }
                    },
                    "script": {
                        "source": script,
                        "params": {"query_vector": embedding},
                    },
                }
            }
        # Vector search only
        elif embedding:
            body["query"] = {
                "script_score": {
                    "query": {
                        "bool": {"filter": [{"exists": {"field": vector_field_path}}]}
                    },
                    "script": {"source": script, "params": {"query_vector": embedding}},
                }
            }
        # Keyword search only
        elif query_string:
            body["query"] = {"query_string": {"query": f'"{query_string}"'}}

        response = self._get_client(config).search(index=es_index, body=body)

        rows = response["hits"]["hits"][0:top_k]
        for row in rows:
            entity_key = row["_source"]["entity_key"]
            entity_key_proto = deserialize_entity_key(
                base64.b64decode(entity_key),
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            timestamp = row["_source"]["timestamp"]
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")

            # Create feature dict with all requested features
            feature_dict = {"distance": _to_value_proto(float(row["_score"]))}
            if query_string is not None:
                feature_dict["text_rank"] = _to_value_proto(float(row["_score"]))
            join_key_values = _extract_join_keys(entity_key_proto)
            feature_dict.update(join_key_values)

            for feature in requested_features:
                if feature in ("distance", "text_rank"):
                    continue
                value = row["_source"].get(feature, None)
                if value is not None:
                    feature_dict[feature] = _to_value_proto(value)

            result.append((timestamp, entity_key_proto, feature_dict))
        return result


def _to_value_proto(value: Any) -> ValueProto:
    """
    Convert a value to a ValueProto object.
    """
    val_proto = ValueProto()
    if isinstance(value, ValueProto):
        return value
    if isinstance(value, float):
        val_proto.float_val = value
    elif isinstance(value, str):
        val_proto.string_val = value
    elif isinstance(value, int):
        val_proto.int64_val = value
    elif isinstance(value, bool):
        val_proto.bool_val = value
    elif isinstance(value, list) and all(isinstance(v, float) for v in value):
        val_proto.float_list_val.val.extend(value)
    elif isinstance(value, dict) and "feature_value" in value:
        try:
            raw_bytes = base64.b64decode(value["feature_value"])
            val_proto.ParseFromString(raw_bytes)
        except Exception as e:
            raise ValueError(f"Failed to decode feature_value from dict: {e}")
    else:
        raise ValueError(f"Unsupported type for ValueProto: {type(value)}")
    return val_proto


def _encode_feature_value(value: ValueProto) -> Dict[str, Any]:
    """
    Encode a ValueProto into a dictionary for Elasticsearch storage.
    """
    encoded_value = base64.b64encode(value.SerializeToString()).decode("utf-8")
    result = {"feature_value": encoded_value}
    vector_val = get_list_val_str(value)

    if vector_val:
        result["vector_value"] = json.loads(vector_val)
    if value.HasField("string_val"):
        result["value_text"] = value.string_val
    elif value.HasField("bytes_val"):
        result["value_text"] = value.bytes_val.decode("utf-8")
    elif value.HasField("int64_val"):
        result["value_text"] = str(value.int64_val)
    elif value.HasField("double_val"):
        result["value_text"] = str(value.double_val)
    return result


def _get_composite_key_name(table: FeatureView) -> List[str]:
    return [field.name for field in table.entity_columns]


def _extract_join_keys(entity_key_proto) -> Dict[str, ValueProto]:
    join_keys = entity_key_proto.join_keys
    entity_values = entity_key_proto.entity_values
    return {
        join_keys[i]: _to_value_proto(entity_values[i])
        for i in range(len(join_keys))
        if i < len(entity_values)
    }
