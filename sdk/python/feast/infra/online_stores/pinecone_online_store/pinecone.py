import base64
import logging
import os
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import StrictStr

from feast import Entity
from feast.feature_view import FeatureView
from feast.filter_models import (
    ComparisonFilter,
    CompoundFilter,
    FilterTranslator,
    FilterType,
)
from feast.infra.infra_object import InfraObject
from feast.infra.key_encoding_utils import (
    deserialize_entity_key,
    serialize_entity_key,
)
from feast.infra.online_stores.helpers import compute_table_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.utils import (
    _serialize_vector_to_float_list,
    to_naive_utc,
)

logger = logging.getLogger(__name__)

PINECONE_METRIC_MAPPING = {
    "cosine": "cosine",
    "COSINE": "cosine",
    "l2": "euclidean",
    "L2": "euclidean",
    "euclidean": "euclidean",
    "EUCLIDEAN": "euclidean",
    "dot": "dotproduct",
    "DOT": "dotproduct",
    "dotproduct": "dotproduct",
    "DOTPRODUCT": "dotproduct",
    "IP": "dotproduct",
}

# Max metadata size per vector in Pinecone (40 KB)
_MAX_METADATA_BYTES = 40_960

# Pinecone upsert batch size limit
_UPSERT_BATCH_SIZE = 100

# Max wait time for index readiness (seconds)
_INDEX_READY_TIMEOUT = 300

_PINECONE_COMPARISON_OPS = {
    "eq": "$eq",
    "ne": "$ne",
    "gt": "$gt",
    "gte": "$gte",
    "lt": "$lt",
    "lte": "$lte",
    "in": "$in",
    "nin": "$nin",
}


class PineconeFilterTranslator(FilterTranslator):
    """Translates Feast filters into Pinecone metadata filter dicts."""

    def translate(self, filters: FilterType) -> Optional[Dict[str, Any]]:
        if filters is None:
            return None
        return self._dispatch(filters)

    def translate_comparison(self, f: ComparisonFilter) -> Dict[str, Any]:
        op = _PINECONE_COMPARISON_OPS.get(f.type)
        if op is None:
            raise ValueError(f"Unsupported comparison operator: {f.type}")
        if f.type in ("in", "nin") and not isinstance(f.value, list):
            raise ValueError(
                f"'{f.type}' filter requires a list value, got {type(f.value)}"
            )
        return {f.key: {op: f.value}}

    def translate_compound(self, f: CompoundFilter) -> Dict[str, Any]:
        if not f.filters:
            return {}
        clauses = [self._dispatch(sub) for sub in f.filters if sub is not None]
        clauses = [c for c in clauses if c]
        if not clauses:
            return {}
        if len(clauses) == 1:
            return clauses[0]
        operator = "$and" if f.type == "and" else "$or"
        return {operator: clauses}


class PineconeOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """
    Configuration for the Pinecone online store.

    NOTE: The class *must* end with the ``OnlineStoreConfig`` suffix.
    """

    type: Literal["pinecone"] = "pinecone"

    api_key: Optional[StrictStr] = None
    """Pinecone API key.  Falls back to the ``PINECONE_API_KEY`` env var."""

    index_name: Optional[StrictStr] = "feast-online"
    """Name of the Pinecone index to use."""

    namespace: Optional[StrictStr] = None
    """Default Pinecone namespace.  When ``None``, the namespace is derived
    from the project name and feature view (``{project}_{fv_name}``)."""

    embedding_dim: Optional[int] = 128
    """Dimensionality of vectors stored in the index."""

    metric: Optional[StrictStr] = "cosine"
    """Distance metric: ``cosine``, ``euclidean``, or ``dotproduct``."""

    cloud: Optional[StrictStr] = "aws"
    """Cloud provider for serverless indexes (``aws``, ``gcp``, ``azure``)."""

    region: Optional[StrictStr] = "us-east-1"
    """Cloud region for serverless indexes."""

    vector_enabled: Optional[bool] = True
    """Whether vector similarity search is enabled."""

    batch_size: Optional[int] = _UPSERT_BATCH_SIZE
    """Number of vectors per upsert request."""


class PineconeOnlineStore(OnlineStore):
    """
    Pinecone implementation of the Feast online store interface.

    Stores feature values as Pinecone vectors with metadata.  Each feature
    view is mapped to a Pinecone namespace within a single index.
    """

    _client: Optional[Any] = None
    _index: Optional[Any] = None

    def _get_api_key(self, config: RepoConfig) -> str:
        online_config = config.online_store
        assert isinstance(online_config, PineconeOnlineStoreConfig)
        api_key = online_config.api_key or os.environ.get("PINECONE_API_KEY")
        if not api_key:
            raise ValueError(
                "Pinecone API key is required. Set it in feature_store.yaml "
                "(online_store.api_key) or via the PINECONE_API_KEY env var."
            )
        return api_key

    def _get_client(self, config: RepoConfig) -> Any:
        if self._client is not None:
            return self._client

        from pinecone import Pinecone

        api_key = self._get_api_key(config)
        self._client = Pinecone(api_key=api_key)
        return self._client

    def _get_index(self, config: RepoConfig) -> Any:
        if self._index is not None:
            return self._index

        online_config = config.online_store
        assert isinstance(online_config, PineconeOnlineStoreConfig)
        client = self._get_client(config)
        self._index = client.Index(online_config.index_name)
        return self._index

    def _get_namespace(self, config: RepoConfig, table: FeatureView) -> str:
        online_config = config.online_store
        assert isinstance(online_config, PineconeOnlineStoreConfig)
        if online_config.namespace:
            return online_config.namespace
        return _table_id(
            config.project,
            table,
            config.registry.enable_online_feature_view_versioning,
        )

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[
                EntityKeyProto,
                Dict[str, ValueProto],
                datetime,
                Optional[datetime],
            ]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        index = self._get_index(config)
        namespace = self._get_namespace(config, table)
        online_config = config.online_store
        assert isinstance(online_config, PineconeOnlineStoreConfig)

        vector_fields = {f.name for f in table.schema if f.vector_index}

        # De-duplicate: keep only the latest event per entity key.
        unique_entities: Dict[
            str,
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]],
        ] = {}
        for entity_key, values_dict, timestamp, created_ts in data:
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()

            existing = unique_entities.get(entity_key_str)
            if existing is None or existing[2] < timestamp:
                unique_entities[entity_key_str] = (
                    entity_key,
                    values_dict,
                    timestamp,
                    created_ts,
                )

        vectors_to_upsert = []
        for entity_key_str, (
            entity_key,
            values_dict,
            timestamp,
            created_ts,
        ) in unique_entities.items():
            timestamp_utc = to_naive_utc(timestamp)
            created_ts_utc = to_naive_utc(created_ts) if created_ts else None

            metadata: Dict[str, Any] = {
                "event_ts": int(timestamp_utc.timestamp() * 1e6),
                "created_ts": int(created_ts_utc.timestamp() * 1e6)
                if created_ts_utc
                else 0,
                "entity_key": entity_key_str,
            }

            embedding: Optional[List[float]] = None

            for feature_name, value_proto in values_dict.items():
                if feature_name in vector_fields:
                    embedding = _extract_vector(value_proto)
                else:
                    metadata[feature_name] = _proto_value_to_metadata(value_proto)

            if embedding is None:
                embedding = [0.0] * (online_config.embedding_dim or 128)

            vectors_to_upsert.append(
                {
                    "id": entity_key_str,
                    "values": embedding,
                    "metadata": metadata,
                }
            )

            if progress:
                progress(1)

        batch_size = online_config.batch_size or _UPSERT_BATCH_SIZE
        for i in range(0, len(vectors_to_upsert), batch_size):
            batch = vectors_to_upsert[i : i + batch_size]
            index.upsert(vectors=batch, namespace=namespace)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        index = self._get_index(config)
        namespace = self._get_namespace(config, table)

        feature_type_map = {f.name: f.dtype for f in table.features}
        if getattr(table, "write_to_online_store", False):
            feature_type_map.update({f.name: f.dtype for f in table.schema})

        vector_fields = {f.name for f in table.schema if f.vector_index}

        ids = []
        for entity_key in entity_keys:
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            ids.append(entity_key_str)

        try:
            fetch_response = index.fetch(ids=ids, namespace=namespace)
        except Exception:
            logger.exception("Error fetching from Pinecone")
            return [(None, None)] * len(entity_keys)

        fetched_vectors = fetch_response.get("vectors", {})
        if hasattr(fetch_response, "vectors"):
            fetched_vectors = fetch_response.vectors

        result_list: List[
            Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]
        ] = []
        for vector_id in ids:
            record = fetched_vectors.get(vector_id)
            if record is None:
                result_list.append((None, None))
                continue

            metadata = record.get("metadata", {})
            if hasattr(record, "metadata"):
                metadata = record.metadata or {}
            values = record.get("values", [])
            if hasattr(record, "values"):
                values = record.values or []

            event_ts_us = metadata.get("event_ts", 0)
            res_ts = datetime.fromtimestamp(event_ts_us / 1e6) if event_ts_us else None

            res: Dict[str, ValueProto] = {}
            features_to_read = (
                requested_features
                if requested_features
                else list(feature_type_map.keys())
            )

            for feature_name in features_to_read:
                if feature_name in vector_fields and values:
                    val = _serialize_vector_to_float_list(values)
                    res[feature_name] = val
                elif feature_name in metadata:
                    val = _metadata_to_proto_value(
                        metadata[feature_name], feature_type_map.get(feature_name)
                    )
                    res[feature_name] = val

            result_list.append((res_ts, res if res else None))

        return result_list

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ) -> None:
        online_config = config.online_store
        assert isinstance(online_config, PineconeOnlineStoreConfig)
        client = self._get_client(config)

        index_name = online_config.index_name
        existing_indexes = {idx.name for idx in client.list_indexes()}

        if index_name not in existing_indexes:
            from pinecone import ServerlessSpec

            metric = PINECONE_METRIC_MAPPING.get(
                online_config.metric or "cosine", "cosine"
            )
            client.create_index(
                name=index_name,
                dimension=online_config.embedding_dim or 128,
                metric=metric,
                spec=ServerlessSpec(
                    cloud=online_config.cloud or "aws",
                    region=online_config.region or "us-east-1",
                ),
            )
            self._wait_for_index_ready(client, index_name or "feast-online")

        # Reset index reference so it re-connects to the (possibly new) index.
        self._index = None

        for table in tables_to_delete:
            self._delete_namespace(config, table)

    def plan(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        return []

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        for table in tables:
            self._delete_namespace(config, table)

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
        online_config = config.online_store
        assert isinstance(online_config, PineconeOnlineStoreConfig)

        if not online_config.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config")

        if embedding is None and query_string is None:
            raise ValueError("Either embedding or query_string must be provided")

        if embedding is None:
            raise ValueError(
                "Pinecone requires a query embedding for similarity search. "
                "Text-only keyword search is not supported."
            )

        index = self._get_index(config)
        namespace = self._get_namespace(config, table)

        entity_name_type_map = {k.name: k.dtype for k in table.entity_columns}
        vector_fields = {f.name for f in table.schema if f.vector_index}

        filter_parts: List[Dict[str, Any]] = []
        metadata_filter = PineconeFilterTranslator().translate(filters)
        if metadata_filter:
            filter_parts.append(metadata_filter)

        if query_string is not None:
            from feast.types import PrimitiveFeastType
            from feast.types import ValueType as FeastValueType

            string_fields = [
                f.name
                for f in table.features
                if isinstance(f.dtype, PrimitiveFeastType)
                and f.dtype.to_value_type() == FeastValueType.STRING
                and f.name in requested_features
            ]
            if string_fields:
                filter_parts.append(
                    {"$or": [{field: {"$eq": query_string}} for field in string_fields]}
                )

        if not filter_parts:
            pinecone_filter = None
        elif len(filter_parts) == 1:
            pinecone_filter = filter_parts[0]
        else:
            pinecone_filter = {"$and": filter_parts}

        query_response = index.query(
            vector=embedding,
            top_k=top_k,
            namespace=namespace,
            include_metadata=True,
            include_values=True,
            filter=pinecone_filter,
        )

        matches = query_response.get("matches", [])
        if hasattr(query_response, "matches"):
            matches = query_response.matches or []

        result_list: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []

        for match in matches:
            metadata = match.get("metadata", {})
            if hasattr(match, "metadata"):
                metadata = match.metadata or {}
            values = match.get("values", [])
            if hasattr(match, "values"):
                values = match.values or []
            score = match.get("score", 0.0)
            if hasattr(match, "score"):
                score = match.score

            event_ts_us = metadata.get("event_ts", 0)
            res_ts = datetime.fromtimestamp(event_ts_us / 1e6) if event_ts_us else None

            entity_key_hex = metadata.get("entity_key")
            entity_key_proto = None
            if entity_key_hex:
                try:
                    entity_key_bytes = bytes.fromhex(entity_key_hex)
                    entity_key_proto = deserialize_entity_key(entity_key_bytes)
                except Exception:
                    pass

            res: Dict[str, ValueProto] = {}
            for feature_name in requested_features:
                if feature_name in vector_fields and values:
                    val = _serialize_vector_to_float_list(embedding)
                    res[feature_name] = val
                elif feature_name in entity_name_type_map:
                    from feast.types import PrimitiveFeastType

                    feat_type = entity_name_type_map[feature_name]
                    if feat_type == PrimitiveFeastType.STRING:
                        raw_val = metadata.get(feature_name, "")
                        res[feature_name] = ValueProto(string_val=str(raw_val))
                    elif feat_type in (
                        PrimitiveFeastType.INT64,
                        PrimitiveFeastType.INT32,
                    ):
                        raw_val = metadata.get(feature_name, 0)
                        res[feature_name] = ValueProto(int64_val=int(raw_val))
                    elif feat_type == PrimitiveFeastType.BYTES:
                        raw_val = metadata.get(feature_name, "")
                        try:
                            res[feature_name] = ValueProto(
                                bytes_val=base64.b64decode(raw_val)
                            )
                        except Exception:
                            res[feature_name] = ValueProto(string_val=str(raw_val))
                elif feature_name in metadata:
                    val = _metadata_to_proto_value(metadata[feature_name], None)
                    res[feature_name] = val

            res["distance"] = ValueProto(float_val=score)
            result_list.append((res_ts, entity_key_proto, res if res else None))

        return result_list

    def _delete_namespace(self, config: RepoConfig, table: FeatureView) -> None:
        """Delete all vectors in the namespace for the given feature view."""
        try:
            index = self._get_index(config)
            namespace = self._get_namespace(config, table)
            index.delete(delete_all=True, namespace=namespace)
        except Exception:
            logger.exception("Error deleting namespace for feature view %s", table.name)

    @staticmethod
    def _wait_for_index_ready(client: Any, index_name: str) -> None:
        deadline = time.time() + _INDEX_READY_TIMEOUT
        while time.time() < deadline:
            desc = client.describe_index(index_name)
            status = getattr(desc, "status", {})
            if isinstance(status, dict):
                ready = status.get("ready", False)
            else:
                ready = getattr(status, "ready", False)
            if ready:
                return
            time.sleep(2)
        logger.warning(
            "Pinecone index '%s' did not become ready within %ds",
            index_name,
            _INDEX_READY_TIMEOUT,
        )


def _table_id(project: str, table: FeatureView, enable_versioning: bool = False) -> str:
    return compute_table_id(project, table, enable_versioning)


def _extract_vector(value_proto: ValueProto) -> Optional[List[float]]:
    """Extract a float list from a ValueProto that contains a vector."""
    if value_proto.HasField("float_list_val"):
        return list(value_proto.float_list_val.val)
    if value_proto.HasField("double_list_val"):
        return [float(v) for v in value_proto.double_list_val.val]
    if value_proto.HasField("int32_list_val"):
        return [float(v) for v in value_proto.int32_list_val.val]
    if value_proto.HasField("int64_list_val"):
        return [float(v) for v in value_proto.int64_list_val.val]
    return None


def _proto_value_to_metadata(value_proto: ValueProto) -> Any:
    """Convert a ValueProto to a JSON-serializable value for Pinecone metadata."""
    if value_proto.HasField("string_val"):
        return value_proto.string_val
    if value_proto.HasField("int32_val"):
        return value_proto.int32_val
    if value_proto.HasField("int64_val"):
        return value_proto.int64_val
    if value_proto.HasField("float_val"):
        return value_proto.float_val
    if value_proto.HasField("double_val"):
        return value_proto.double_val
    if value_proto.HasField("bool_val"):
        return value_proto.bool_val

    if value_proto.HasField("float_list_val"):
        return list(value_proto.float_list_val.val)
    if value_proto.HasField("double_list_val"):
        return list(value_proto.double_list_val.val)
    if value_proto.HasField("int32_list_val"):
        return list(value_proto.int32_list_val.val)
    if value_proto.HasField("int64_list_val"):
        return list(value_proto.int64_list_val.val)

    if value_proto.HasField("bytes_val"):
        return base64.b64encode(value_proto.bytes_val).decode("utf-8")

    return base64.b64encode(value_proto.SerializeToString()).decode("utf-8")


def _metadata_to_proto_value(metadata_value: Any, feast_type: Any) -> ValueProto:
    """Convert a Pinecone metadata value back to a ValueProto."""
    val = ValueProto()

    if feast_type is not None:
        from feast.type_map import VALUE_TYPE_TO_PROTO_VALUE_MAP
        from feast.types import from_feast_type

        value_type = from_feast_type(feast_type)
        proto_attr = VALUE_TYPE_TO_PROTO_VALUE_MAP.get(value_type)

        if proto_attr:
            if proto_attr in ("int32_val", "int64_val"):
                setattr(val, proto_attr, int(metadata_value))
                return val
            elif proto_attr in ("float_val", "double_val"):
                setattr(val, proto_attr, float(metadata_value))
                return val
            elif proto_attr == "bool_val":
                setattr(val, proto_attr, bool(metadata_value))
                return val
            elif proto_attr == "string_val":
                setattr(val, proto_attr, str(metadata_value))
                return val
            elif proto_attr == "bytes_val":
                if isinstance(metadata_value, str):
                    setattr(val, proto_attr, base64.b64decode(metadata_value))
                else:
                    setattr(val, proto_attr, metadata_value)
                return val
            elif proto_attr in (
                "float_list_val",
                "double_list_val",
                "int32_list_val",
                "int64_list_val",
            ):
                if isinstance(metadata_value, list):
                    getattr(val, proto_attr).val.extend(metadata_value)
                    return val

    if isinstance(metadata_value, bool):
        val.bool_val = metadata_value
    elif isinstance(metadata_value, int):
        val.int64_val = metadata_value
    elif isinstance(metadata_value, float):
        val.double_val = metadata_value
    elif isinstance(metadata_value, str):
        val.string_val = metadata_value
    elif isinstance(metadata_value, list):
        if metadata_value and isinstance(metadata_value[0], (int, float)):
            val.float_list_val.val.extend([float(v) for v in metadata_value])
        else:
            val.string_val = str(metadata_value)
    else:
        val.string_val = str(metadata_value)

    return val
