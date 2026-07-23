from __future__ import absolute_import

import base64
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from qdrant_client import QdrantClient, models

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
from feast.types import PrimitiveFeastType
from feast.utils import (
    _build_retrieve_online_document_record,
    _get_feature_view_vector_field_metadata,
    to_naive_utc,
)
from feast.value_type import ValueType

SCROLL_SIZE = 1000

DISTANCE_MAPPING = {
    "cosine": models.Distance.COSINE,
    "l2": models.Distance.EUCLID,
    "dot": models.Distance.DOT,
    "l1": models.Distance.MANHATTAN,
}

logger = logging.getLogger(__name__)


class QdrantOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """
    Configuration for the Qdrant online store.
    """

    type: str = "qdrant"

    location: Optional[str] = None
    url: Optional[str] = None
    port: Optional[int] = 6333
    grpc_port: int = 6334
    prefer_grpc: bool = False
    https: Optional[bool] = None
    api_key: Optional[str] = None
    prefix: Optional[str] = None
    timeout: Optional[int] = None
    host: Optional[str] = None
    path: Optional[str] = None

    # The name of the vector to use.
    # Defaults to the single, unnamed vector
    # Reference: https://qdrant.tech/documentation/concepts/vectors/#named-vectors
    vector_name: str = ""
    # The number of point to write in a single request
    write_batch_size: Optional[int] = 64
    # Await for the upload results to be applied on the server side.
    # If `true`, each request will explicitly wait for the confirmation of completion. Might be slower.
    # If `false`, each reequest will return immediately after receiving an acknowledgement.
    upload_wait: bool = True
    vector_enabled: Optional[bool] = True
    # Opt-in hybrid keyword search via Qdrant sparse vectors + RRF fusion.
    text_search_enabled: Optional[bool] = False
    sparse_vector_name: str = "sparse"
    sparse_embedding_model: str = "Qdrant/bm25"
    # STRING feature to index for sparse/hybrid search. Defaults to the first string feature.
    text_feature: Optional[str] = None


def _dense_vector_using(config: QdrantOnlineStoreConfig) -> Optional[str]:
    return config.vector_name or None


def _entity_key_bytes_from_payload(raw: Any) -> Optional[bytes]:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, str):
        try:
            return base64.b64decode(raw, validate=True)
        except Exception:
            return raw.encode("utf-8")
    return None


def _parse_timestamp(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        raise ValueError(f"Unsupported timestamp format: {raw!r}")
    return None


def _decode_feature_value(encoded_value: Any) -> ValueProto:
    value_proto = ValueProto()
    if isinstance(encoded_value, str):
        value_proto.ParseFromString(base64.b64decode(encoded_value))
    elif isinstance(encoded_value, bytes):
        value_proto.ParseFromString(encoded_value)
    return value_proto


def _string_from_value_proto(value: ValueProto) -> Optional[str]:
    if value.HasField("string_val"):
        return value.string_val
    return None


def _get_string_feature_names(table: FeatureView) -> List[str]:
    return [
        field.name
        for field in table.features
        if isinstance(field.dtype, PrimitiveFeastType)
        and field.dtype.to_value_type() == ValueType.STRING
    ]


def _resolve_text_feature(
    table: FeatureView, config: QdrantOnlineStoreConfig
) -> Optional[str]:
    if config.text_feature:
        return config.text_feature
    string_fields = _get_string_feature_names(table)
    return string_fields[0] if string_fields else None


class QdrantOnlineStore(OnlineStore):
    _client: Optional[QdrantClient] = None

    def _get_client(self, config: RepoConfig) -> QdrantClient:
        if self._client:
            return self._client
        online_store_config = config.online_store
        assert isinstance(online_store_config, QdrantOnlineStoreConfig), (
            "Invalid type for online store config"
        )

        assert online_store_config.similarity and (
            online_store_config.similarity.lower() in DISTANCE_MAPPING
        ), f"Unsupported distance metric {online_store_config.similarity}"

        self._client = QdrantClient(
            location=online_store_config.location,
            url=online_store_config.url,
            port=online_store_config.port,
            grpc_port=online_store_config.grpc_port,
            prefer_grpc=online_store_config.prefer_grpc,
            https=online_store_config.https,
            api_key=online_store_config.api_key,
            prefix=online_store_config.prefix,
            timeout=online_store_config.timeout,
            host=online_store_config.host,
            path=online_store_config.path,
        )
        return self._client

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        online_store_config = config.online_store
        assert isinstance(online_store_config, QdrantOnlineStoreConfig)

        text_feature: Optional[str] = None
        points: List[models.PointStruct] = []

        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            entity_key_payload = base64.b64encode(entity_key_bin).decode("ascii")

            timestamp = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)
            for feature_name, value in values.items():
                encoded_value = base64.b64encode(value.SerializeToString()).decode(
                    "utf-8"
                )
                vector_val = get_list_val_str(value)
                vector: Dict[str, Any] = {}
                if vector_val:
                    dense_name = online_store_config.vector_name
                    vector[dense_name] = json.loads(vector_val)
                if online_store_config.text_search_enabled:
                    if text_feature is None:
                        text_feature = _resolve_text_feature(table, online_store_config)
                    if text_feature and feature_name == text_feature:
                        text_content = _string_from_value_proto(value)
                        if text_content:
                            vector[online_store_config.sparse_vector_name] = (
                                models.Document(
                                    text=text_content,
                                    model=online_store_config.sparse_embedding_model,
                                )
                            )

                points.append(
                    models.PointStruct(
                        id=uuid.uuid4().hex,
                        payload={
                            "entity_key": entity_key_payload,
                            "feature_name": feature_name,
                            "feature_value": encoded_value,
                            "timestamp": timestamp,
                            "created_ts": created_ts,
                        },
                        vector=vector,
                    )
                )

        self._get_client(config).upload_points(
            collection_name=table.name,
            batch_size=online_store_config.write_batch_size or 64,
            points=points,
            wait=online_store_config.upload_wait,
        )

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        conditions: List[models.Condition] = []
        if entity_keys:
            conditions.append(
                models.FieldCondition(
                    key="entity_key",
                    match=models.MatchAny(any=entity_keys),  # type: ignore
                )
            )

        if requested_features:
            conditions.append(
                models.FieldCondition(
                    key="feature_name", match=models.MatchAny(any=requested_features)
                )
            )
        points = []
        next_offset = None
        stop_scrolling = False
        while not stop_scrolling:
            records, next_offset = self._get_client(config).scroll(
                collection_name=config.online_store.collection_name,
                limit=SCROLL_SIZE,
                offset=next_offset,
                with_payload=True,
                scroll_filter=models.Filter(must=conditions),
            )
            stop_scrolling = next_offset is None

            points.extend(records)

        results = []
        for point in points:
            assert isinstance(point.payload, Dict), "Invalid value of payload"
            results.append(
                (
                    point.payload["timestamp"],
                    {point.payload["feature_name"]: point.payload["feature_value"]},
                )
            )

        return results  # type: ignore

    def create_collection(self, config: RepoConfig, table: FeatureView):
        """
        Create a collection in Qdrant for the given table.
        Args:
            config: Feast repo configuration object.
            table: FeatureView table for which the index needs to be created.
        """
        online_store_config = config.online_store
        assert isinstance(online_store_config, QdrantOnlineStoreConfig)

        vector_field_length = getattr(
            _get_feature_view_vector_field_metadata(table), "vector_length", 512
        )
        similarity = online_store_config.similarity or "cosine"

        client: QdrantClient = self._get_client(config)

        create_kwargs: Dict[str, Any] = {
            "collection_name": table.name,
            "vectors_config": {
                online_store_config.vector_name: models.VectorParams(
                    size=vector_field_length,
                    distance=DISTANCE_MAPPING[similarity.lower()],
                )
            },
        }
        if online_store_config.text_search_enabled:
            create_kwargs["sparse_vectors_config"] = {
                online_store_config.sparse_vector_name: models.SparseVectorParams(
                    modifier=models.Modifier.IDF
                )
            }

        client.create_collection(**create_kwargs)
        client.create_payload_index(
            collection_name=table.name,
            field_name="entity_key",
            field_schema=models.PayloadSchemaType.KEYWORD,
        )
        client.create_payload_index(
            collection_name=table.name,
            field_name="feature_name",
            field_schema=models.PayloadSchemaType.KEYWORD,
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
        for table in tables_to_delete:
            self._get_client(config).delete_collection(collection_name=table.name)
        for table in tables_to_keep:
            self.create_collection(config, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        project = config.project
        try:
            for table in tables:
                self._get_client(config).delete_collection(collection_name=table.name)
        except Exception as e:
            logging.exception(f"Error deleting collection in project {project}: {e}")
            raise

    def _vector_field_name(self, table: FeatureView) -> str:
        vector_metadata = _get_feature_view_vector_field_metadata(table)
        if vector_metadata is None:
            raise ValueError(
                f"Feature view '{table.name}' has no fields with vector_index=True."
            )
        return vector_metadata.name

    def _fetch_features_by_entity_keys(
        self,
        client: QdrantClient,
        collection_name: str,
        entity_keys: List[bytes],
        requested_features: List[str],
    ) -> Dict[bytes, Dict[str, ValueProto]]:
        if not entity_keys or not requested_features:
            return {}

        features_by_entity: Dict[bytes, Dict[str, ValueProto]] = {}
        next_offset = None
        stop_scrolling = False
        entity_key_filters = [
            base64.b64encode(entity_key).decode("ascii") for entity_key in entity_keys
        ]
        scroll_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="entity_key",
                    match=models.MatchAny(any=entity_key_filters),
                ),
                models.FieldCondition(
                    key="feature_name",
                    match=models.MatchAny(any=requested_features),
                ),
            ]
        )
        while not stop_scrolling:
            records, next_offset = client.scroll(
                collection_name=collection_name,
                limit=SCROLL_SIZE,
                offset=next_offset,
                with_payload=True,
                scroll_filter=scroll_filter,
            )
            stop_scrolling = next_offset is None
            for point in records:
                payload = point.payload or {}
                entity_key_bin = _entity_key_bytes_from_payload(
                    payload.get("entity_key")
                )
                feature_name = payload.get("feature_name")
                feature_value = payload.get("feature_value")
                if (
                    entity_key_bin is None
                    or not isinstance(feature_name, str)
                    or feature_value is None
                ):
                    continue
                features_by_entity.setdefault(entity_key_bin, {})[feature_name] = (
                    _decode_feature_value(feature_value)
                )
        return features_by_entity

    def _build_v2_result(
        self,
        entity_key_bin: bytes,
        payload: Dict[str, Any],
        requested_features: List[str],
        features_by_entity: Dict[bytes, Dict[str, ValueProto]],
        distance: Optional[float],
        text_rank: Optional[float],
        entity_key_serialization_version: int,
    ) -> Tuple[
        Optional[datetime],
        Optional[EntityKeyProto],
        Optional[Dict[str, ValueProto]],
    ]:
        entity_key_proto = deserialize_entity_key(
            entity_key_bin,
            entity_key_serialization_version=entity_key_serialization_version,
        )
        timestamp = _parse_timestamp(payload.get("timestamp"))
        feature_dict = dict(features_by_entity.get(entity_key_bin, {}))
        for feature_name in requested_features:
            feature_dict.setdefault(feature_name, ValueProto())
        if distance is not None:
            feature_dict["distance"] = ValueProto(float_val=float(distance))
        if text_rank is not None:
            feature_dict["text_rank"] = ValueProto(float_val=float(text_rank))
        return timestamp, entity_key_proto, feature_dict

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
        include_feature_view_version_metadata: bool = False,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """Retrieve documents from Qdrant with all requested features per entity.

        Qdrant stores one point per feature, so this method searches on the
        vector (and/or sparse) field and then joins the remaining features via
        a scroll query keyed on entity_key.

        Three search modes are supported:

        * **Dense only** (``embedding`` provided, no ``query_string``): standard
          nearest-neighbour search on the named dense vector.
        * **Sparse / text only** (``query_string`` provided, no ``embedding``):
          BM25 keyword search on the sparse vector field.
        * **Hybrid** (both ``embedding`` *and* ``query_string``): two prefetch
          queries (dense + sparse) fused via Reciprocal Rank Fusion (RRF).
          The returned ``distance`` value is the **fused RRF rank score**, not a
          raw cosine similarity or BM25 relevance.  ``text_rank`` is not
          populated in hybrid mode because individual sub-scores are unavailable
          after fusion.

        Args:
            config: Feast repo configuration.
            table: The FeatureView whose collection to search.
            requested_features: Feature names to return for each hit entity.
            embedding: Dense query vector (required unless ``query_string`` given).
            top_k: Maximum number of unique entities to return.
            distance_metric: Optional metric name for validation.  The actual
                metric used is always the one configured on the collection at
                creation time (``online_store.similarity``).
            query_string: Text query for sparse / hybrid search (requires
                ``text_search_enabled: true`` in the online store config).
            include_feature_view_version_metadata: Reserved for future use.

        Returns:
            A list of ``(timestamp, entity_key_proto, feature_dict)`` tuples
            ordered by descending relevance.  ``feature_dict`` includes a
            ``distance`` entry (float) and, for text-only search, a
            ``text_rank`` entry.
        """
        online_store_config = config.online_store
        assert isinstance(online_store_config, QdrantOnlineStoreConfig)

        if embedding is None and query_string is None:
            raise ValueError("Either embedding or query_string must be provided")

        if embedding is not None and not online_store_config.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config")

        if query_string is not None and not online_store_config.text_search_enabled:
            raise ValueError(
                "Text search requires text_search_enabled: true in the Qdrant "
                "online store config. Recreate the collection with feast apply after "
                "enabling it."
            )

        metric = (distance_metric or online_store_config.similarity or "cosine").lower()
        if metric not in DISTANCE_MAPPING:
            raise ValueError(f"Unsupported distance metric: {metric}")
        configured = (online_store_config.similarity or "cosine").lower()
        if distance_metric is not None and metric != configured:
            logger.warning(
                "distance_metric=%r differs from the collection's configured "
                "similarity=%r. Qdrant always uses the metric set at collection "
                "creation time; the distance_metric parameter has no effect on "
                "the query.",
                distance_metric,
                configured,
            )

        client = self._get_client(config)
        collection_name = table.name
        vector_field_name = self._vector_field_name(table)
        dense_using = _dense_vector_using(online_store_config)
        vector_feature_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="feature_name",
                    match=models.MatchValue(value=vector_field_name),
                )
            ]
        )

        sparse_score_by_entity: Dict[bytes, float] = {}
        dense_score_by_entity: Dict[bytes, float] = {}
        fused_score_by_entity: Dict[bytes, float] = {}
        hit_order: List[bytes] = []
        hit_payload_by_entity: Dict[bytes, Dict[str, Any]] = {}

        if embedding is not None and query_string is not None:
            prefetches = [
                models.Prefetch(
                    query=embedding,
                    using=dense_using,
                    filter=vector_feature_filter,
                    limit=top_k,
                ),
                models.Prefetch(
                    query=models.Document(
                        text=query_string,
                        model=online_store_config.sparse_embedding_model,
                    ),
                    using=online_store_config.sparse_vector_name,
                    limit=top_k,
                ),
            ]
            # Qdrant stores one point per feature. In RRF fusion the same
            # entity can appear twice — once from the embedding point and once
            # from the text_field point. We request top_k * 2 points so that
            # after deduplication we still have up to top_k unique entities.
            # This factor assumes exactly 2 point types participate in fusion
            # (the dense embedding and the sparse text_field).
            response = client.query_points(
                collection_name=collection_name,
                prefetch=prefetches,
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                limit=top_k * 2,
                with_payload=True,
            )
            points = response.points
        elif embedding is not None:
            response = client.query_points(
                collection_name=collection_name,
                query=embedding,
                query_filter=vector_feature_filter,
                limit=top_k,
                with_payload=True,
                using=dense_using,
            )
            points = response.points
        else:
            assert query_string is not None
            response = client.query_points(
                collection_name=collection_name,
                query=models.Document(
                    text=query_string,
                    model=online_store_config.sparse_embedding_model,
                ),
                using=online_store_config.sparse_vector_name,
                limit=top_k,
                with_payload=True,
            )
            points = response.points

        is_hybrid = embedding is not None and query_string is not None

        for point in points:
            payload = point.payload or {}
            entity_key_bin = _entity_key_bytes_from_payload(payload.get("entity_key"))
            if entity_key_bin is None:
                continue
            if entity_key_bin not in hit_order:
                hit_order.append(entity_key_bin)
            hit_payload_by_entity.setdefault(entity_key_bin, payload)
            if point.score is not None:
                score = float(point.score)
                if is_hybrid:
                    # RRF fusion produces a single fused rank score per point,
                    # not the original cosine/BM25 sub-scores. Keep the first
                    # (highest) fused score per entity.
                    fused_score_by_entity.setdefault(entity_key_bin, score)
                else:
                    if embedding is not None:
                        dense_score_by_entity.setdefault(entity_key_bin, score)
                    if query_string is not None:
                        sparse_score_by_entity.setdefault(entity_key_bin, score)

        hit_order = hit_order[:top_k]

        if not hit_order:
            return []

        features_by_entity = self._fetch_features_by_entity_keys(
            client,
            collection_name,
            hit_order,
            requested_features,
        )

        results: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []
        for entity_key_bin in hit_order:
            payload = hit_payload_by_entity[entity_key_bin]
            if is_hybrid:
                distance = fused_score_by_entity.get(entity_key_bin)
                text_rank = None
            else:
                distance = dense_score_by_entity.get(entity_key_bin)
                text_rank = (
                    sparse_score_by_entity.get(entity_key_bin)
                    if query_string is not None
                    else None
                )
            results.append(
                self._build_v2_result(
                    entity_key_bin,
                    payload,
                    requested_features,
                    features_by_entity,
                    distance,
                    text_rank,
                    config.entity_key_serialization_version,
                )
            )
        return results

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: List[float],
        top_k: int,
        distance_metric: Optional[str] = "cosine",
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

        if distance_metric and distance_metric.lower() not in DISTANCE_MAPPING:
            raise ValueError(f"Unsupported distance metric: {distance_metric}")
        points = (
            self._get_client(config)
            .query_points(
                collection_name=table.name,
                query=embedding,
                limit=top_k,
                with_payload=True,
                with_vectors=True,
                using=config.online_store.vector_name or None,
            )
            .points
        )
        for point in points:
            payload = point.payload or {}
            entity_key_bin = _entity_key_bytes_from_payload(payload.get("entity_key"))
            if entity_key_bin is None:
                continue
            feature_value = str(payload.get("feature_value"))
            timestamp = _parse_timestamp(payload.get("timestamp"))
            if timestamp is None:
                continue
            distance = point.score
            vector_value = str(
                point.vector[config.online_store.vector_name]
                if isinstance(point.vector, Dict)
                else point.vector
            )

            result.append(
                _build_retrieve_online_document_record(
                    entity_key_bin,
                    base64.b64decode(feature_value),
                    vector_value,
                    distance,
                    timestamp,
                    config.entity_key_serialization_version,
                )
            )
        return result
