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

SCROLL_SIZE = 1000

DISTANCE_MAPPING = {
    "cosine": models.Distance.COSINE,
    "l2": models.Distance.EUCLID,
    "dot": models.Distance.DOT,
    "l1": models.Distance.MANHATTAN,
}


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
        points = []
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )

            timestamp = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)
            for feature_name, value in values.items():
                encoded_value = base64.b64encode(value.SerializeToString()).decode(
                    "utf-8"
                )
                vector_val = get_list_val_str(value)
                if vector_val:
                    vector = {config.online_store.vector_name: json.loads(vector_val)}
                else:
                    vector = {}
                points.append(
                    models.PointStruct(
                        id=uuid.uuid4().hex,
                        payload={
                            "entity_key": entity_key_bin,
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
            batch_size=config.online_store.write_batch_size,
            points=points,
            wait=True,
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

        vector_field_length = getattr(
            _get_feature_view_vector_field_metadata(table), "vector_length", 512
        )

        client: QdrantClient = self._get_client(config)

        client.create_collection(
            collection_name=table.name,
            vectors_config={
                config.online_store.vector_name: models.VectorParams(
                    size=vector_field_length,
                    distance=DISTANCE_MAPPING[config.online_store.similarity.lower()],
                )
            },
        )
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
            entity_key = str(payload.get("entity_key"))
            feature_value = str(payload.get("feature_value"))
            timestamp_str = str(payload.get("timestamp"))
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
            distance = point.score
            vector_value = str(
                point.vector[config.online_store.vector_name]
                if isinstance(point.vector, Dict)
                else point.vector
            )

            result.append(
                _build_retrieve_online_document_record(
                    entity_key,
                    base64.b64decode(feature_value),
                    vector_value,
                    distance,
                    timestamp,
                    config.entity_key_serialization_version,
                )
            )
        return result
