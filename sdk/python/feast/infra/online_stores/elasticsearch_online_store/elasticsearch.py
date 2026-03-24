from __future__ import absolute_import

import base64
import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from elasticsearch import Elasticsearch, helpers

from feast import Entity, FeatureView, RepoConfig
from feast.filter_models import ComparisonFilter, CompoundFilter
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
    enable_openai_compatible_store: Optional[bool] = False


logger = logging.getLogger(__name__)

_NUMERIC_COMPARISON_OPS = {"gt", "gte", "lt", "lte"}


def _filters_contain_numeric_comparison(
    filter_obj: Union[ComparisonFilter, CompoundFilter],
) -> bool:
    if isinstance(filter_obj, ComparisonFilter):
        return filter_obj.type in _NUMERIC_COMPARISON_OPS and isinstance(
            filter_obj.value, (int, float)
        )
    if isinstance(filter_obj, CompoundFilter):
        return any(_filters_contain_numeric_comparison(f) for f in filter_obj.filters)
    return False


class ElasticSearchOnlineStore(OnlineStore):
    _client: Optional[Elasticsearch] = None
    _index_value_num_cache: Optional[Dict[str, bool]] = None

    def _index_has_value_num(self, config: RepoConfig, index_name: str) -> bool:
        """Check the actual ES index mapping for the value_num field.

        Caches the result per index so we only hit ES once.
        """
        if self._index_value_num_cache is None:
            self._index_value_num_cache = {}
        if index_name in self._index_value_num_cache:
            return self._index_value_num_cache[index_name]
        try:
            mapping = self._get_client(config).indices.get_mapping(index=index_name)
            templates = (
                mapping.get(index_name, {})
                .get("mappings", {})
                .get("dynamic_templates", [])
            )
            for tmpl in templates:
                for _, tmpl_body in tmpl.items():
                    props = tmpl_body.get("mapping", {}).get("properties", {})
                    if "value_num" in props:
                        self._index_value_num_cache[index_name] = True
                        return True
        except Exception as e:
            logging.warning(
                "Failed to check index mapping for value_num on '%s': %s: %s",
                index_name,
                type(e).__name__,
                e,
            )
        self._index_value_num_cache[index_name] = False
        return False

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
        include_value_num = self._index_has_value_num(config, table.name)
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
                doc = _encode_feature_value(value, include_value_num=include_value_num)
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

        feature_properties: Dict[str, Any] = {
            "feature_value": {"type": "binary"},
            "value_text": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "vector_value": {
                "type": "dense_vector",
                "dims": vector_field_length,
                "index": True,
                "similarity": config.online_store.similarity,
            },
        }

        if getattr(config.online_store, "enable_openai_compatible_store", False):
            feature_properties["value_num"] = {"type": "double"}

        index_mapping = {
            "dynamic_templates": [
                {
                    "feature_objects": {
                        "match_mapping_type": "object",
                        "match": "*",
                        "mapping": {
                            "type": "object",
                            "properties": feature_properties,
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
        distance_metric: Optional[str] = None,
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

    def _translate_filters(
        self,
        filters: Optional[Union[ComparisonFilter, CompoundFilter]],
        has_value_num: bool = False,
    ) -> List[Dict[str, Any]]:
        """Translate filter objects into Elasticsearch Query DSL filter clauses.

        Returns a list of ES filter clause dicts suitable for insertion into
        a ``bool.filter`` array.  Returns an empty list when no filters are
        provided.
        """
        if filters is None:
            return []
        return [self._translate_single_filter(filters, has_value_num=has_value_num)]

    def _translate_single_filter(
        self,
        filter_obj: Union[ComparisonFilter, CompoundFilter],
        has_value_num: bool = False,
    ) -> Dict[str, Any]:
        if isinstance(filter_obj, ComparisonFilter):
            return self._translate_comparison_filter(
                filter_obj, has_value_num=has_value_num
            )
        elif isinstance(filter_obj, CompoundFilter):
            return self._translate_compound_filter(
                filter_obj, has_value_num=has_value_num
            )
        raise ValueError(f"Unknown filter type: {type(filter_obj)}")

    def _translate_comparison_filter(
        self,
        f: ComparisonFilter,
        has_value_num: bool = False,
    ) -> Dict[str, Any]:
        """Translate a ComparisonFilter to an ES Query DSL clause."""
        is_numeric = isinstance(f.value, (int, float)) and not isinstance(f.value, bool)
        is_numeric_list = (
            isinstance(f.value, list)
            and f.value
            and isinstance(f.value[0], (int, float))
            and not isinstance(f.value[0], bool)
        )

        if has_value_num and (is_numeric or is_numeric_list):
            field = f"{f.key}.value_num"
            exact_field = field
            fmt_val = f.value
            fmt_list = f.value if is_numeric_list else None
        else:
            exact_field = f"{f.key}.value_text.keyword"
            field = f"{f.key}.value_text"
            fmt_val = str(f.value)
            fmt_list = [str(v) for v in f.value] if isinstance(f.value, list) else None

        if f.type == "eq":
            return {"term": {exact_field: fmt_val}}
        elif f.type == "ne":
            return {"bool": {"must_not": [{"term": {exact_field: fmt_val}}]}}
        elif f.type in ("gt", "gte", "lt", "lte"):
            return {"range": {field: {f.type: fmt_val}}}
        elif f.type == "in":
            if not isinstance(f.value, list):
                raise ValueError(
                    f"'in' filter requires a list value, got {type(f.value)}"
                )
            return {"terms": {exact_field: fmt_list}}
        elif f.type == "nin":
            if not isinstance(f.value, list):
                raise ValueError(
                    f"'nin' filter requires a list value, got {type(f.value)}"
                )
            return {"bool": {"must_not": [{"terms": {exact_field: fmt_list}}]}}
        raise ValueError(f"Unsupported comparison operator: {f.type}")

    def _translate_compound_filter(
        self,
        f: CompoundFilter,
        has_value_num: bool = False,
    ) -> Dict[str, Any]:
        clauses = [
            self._translate_single_filter(sub, has_value_num=has_value_num)
            for sub in f.filters
        ]
        if f.type == "and":
            return {"bool": {"must": clauses}}
        else:
            return {"bool": {"should": clauses, "minimum_should_match": 1}}

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
        filters: Optional[Union[ComparisonFilter, CompoundFilter]] = None,
        include_feature_view_version_metadata: bool = False,
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

        has_value_num = self._index_has_value_num(config, es_index)

        if (
            filters
            and _filters_contain_numeric_comparison(filters)
            and not has_value_num
        ):
            logger.warning(
                "Numeric comparison filters (gt, gte, lt, lte) are being used "
                "but this index does not have a 'value_num' field. Numeric "
                "fields are stored as text, which causes lexicographic "
                "comparison instead of numeric comparison (e.g. '9' > '100'). "
                "To fix this, set 'enable_openai_compatible_store: true' in "
                "your online_store config, then teardown and re-apply your "
                "feature store to recreate indices with the value_num field."
            )

        metadata_filters = self._translate_filters(filters, has_value_num=has_value_num)

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

        if embedding and query_string:
            bool_clause: Dict[str, Any] = {
                "must": [
                    {"query_string": {"query": f'"{query_string}"'}},
                    {"exists": {"field": vector_field_path}},
                ]
            }
            if metadata_filters:
                bool_clause["filter"] = metadata_filters
            body["query"] = {
                "script_score": {
                    "query": {"bool": bool_clause},
                    "script": {
                        "source": script,
                        "params": {"query_vector": embedding},
                    },
                }
            }
        elif embedding:
            filter_clauses: List[Dict[str, Any]] = [
                {"exists": {"field": vector_field_path}}
            ]
            filter_clauses.extend(metadata_filters)
            body["query"] = {
                "script_score": {
                    "query": {"bool": {"filter": filter_clauses}},
                    "script": {"source": script, "params": {"query_vector": embedding}},
                }
            }
        elif query_string:
            if metadata_filters:
                body["query"] = {
                    "bool": {
                        "must": [
                            {"query_string": {"query": f'"{query_string}"'}},
                        ],
                        "filter": metadata_filters,
                    }
                }
            else:
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
    if isinstance(value, bool):
        val_proto.bool_val = value
    elif isinstance(value, float):
        val_proto.float_val = value
    elif isinstance(value, str):
        val_proto.string_val = value
    elif isinstance(value, int):
        val_proto.int64_val = value
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


def _encode_feature_value(
    value: ValueProto,
    include_value_num: bool = False,
) -> Dict[str, Any]:
    """
    Encode a ValueProto into a dictionary for Elasticsearch storage.
    """
    encoded_value = base64.b64encode(value.SerializeToString()).decode("utf-8")
    result: Dict[str, Any] = {"feature_value": encoded_value}
    vector_val = get_list_val_str(value)

    if vector_val:
        result["vector_value"] = json.loads(vector_val)
    if value.HasField("string_val"):
        result["value_text"] = value.string_val
    elif value.HasField("bytes_val"):
        result["value_text"] = value.bytes_val.decode("utf-8")
    elif value.HasField("int64_val"):
        result["value_text"] = str(value.int64_val)
        if include_value_num:
            result["value_num"] = value.int64_val
    elif value.HasField("int32_val"):
        result["value_text"] = str(value.int32_val)
        if include_value_num:
            result["value_num"] = value.int32_val
    elif value.HasField("double_val"):
        result["value_text"] = str(value.double_val)
        if include_value_num:
            result["value_num"] = value.double_val
    elif value.HasField("float_val"):
        result["value_text"] = str(value.float_val)
        if include_value_num:
            result["value_num"] = value.float_val
    elif value.HasField("bool_val"):
        result["value_text"] = str(value.bool_val)
        if include_value_num:
            result["value_num"] = 1.0 if value.bool_val else 0.0
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
