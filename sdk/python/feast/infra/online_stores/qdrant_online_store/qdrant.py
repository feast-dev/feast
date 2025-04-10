from __future__ import absolute_import

import base64
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, cast

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
from feast.utils import _build_retrieve_online_document_record, to_naive_utc

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
    # Enable text indexing for faster text searches
    enable_text_index: bool = True
    # Search parameters for HNSW
    hnsw_ef: int = 128


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
            entity_key_str = base64.b64encode(entity_key_bin).decode("utf-8")

            timestamp = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)
            for feature_name, value in values.items():
                encoded_value = base64.b64encode(value.SerializeToString()).decode(
                    "utf-8"
                )

                payload = {
                    "entity_key": entity_key_str,
                    "feature_name": feature_name,
                    "feature_value": encoded_value,
                    "timestamp": timestamp,
                    "created_ts": created_ts,
                }

                if config.online_store.enable_text_index and value.HasField(
                    "string_val"
                ):
                    payload["text_value"] = value.string_val

                # Extract vector value if this is a vector feature
                vector_val = get_list_val_str(value)
                if vector_val:
                    vector = {config.online_store.vector_name: json.loads(vector_val)}
                elif feature_name == config.online_store.vector_name:
                    # Fallback to the previous method if vector_name matches feature_name
                    vector_val = list(value.float_list_val.val)
                    vector = {config.online_store.vector_name: vector_val}
                else:
                    # Default empty vector for non-vector features
                    vector = {
                        config.online_store.vector_name: [0.0]
                        * config.online_store.vector_len
                    }
                
                points.append(
                    models.PointStruct(
                        id=uuid.uuid4().hex,
                        payload=payload,
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
            # Convert entity keys to the string format stored in Qdrant
            entity_key_strs = []
            for entity_key in entity_keys:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                entity_key_str = base64.b64encode(entity_key_bin).decode("utf-8")
                entity_key_strs.append(entity_key_str)
                
            conditions.append(
                models.FieldCondition(
                    key="entity_key",
                    match=models.MatchAny(any=entity_key_strs),
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

        client: QdrantClient = self._get_client(config)

        client.create_collection(
            collection_name=table.name,
            vectors_config={
                config.online_store.vector_name: models.VectorParams(
                    size=config.online_store.vector_len,
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

        if config.online_store.enable_text_index:
            try:
                client.create_payload_index(
                    collection_name=table.name,
                    field_name="text_value",
                    field_schema=models.PayloadSchemaType.TEXT,
                )
            except Exception:
                logging.warning("Failed to create text index")

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
        except Exception:
            logging.exception(f"Error deleting collection in project {project}")
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
            entity_key_str = payload.get("entity_key")
            if not entity_key_str:
                continue
                
            feature_value = str(payload.get("feature_value"))
            timestamp_str = str(payload.get("timestamp"))
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
            distance = point.score
            vector_value = str(
                point.vector[config.online_store.vector_name]
                if isinstance(point.vector, Dict)
                else point.vector
            )

            # Instead of using _build_retrieve_online_document_record directly, handle the deserialization ourselves
            # to have more control over the process
            entity_key_bin = base64.b64decode(entity_key_str)
            entity_key_proto = deserialize_entity_key(
                entity_key_bin,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            
            feature_value_proto = ValueProto()
            feature_value_proto.ParseFromString(base64.b64decode(feature_value))
            
            vector_proto = ValueProto()
            vector_proto.float_list_val.val.extend(json.loads(vector_value))
            
            distance_proto = ValueProto()
            distance_proto.double_val = distance
            
            result.append(
                (timestamp, entity_key_proto, feature_value_proto, vector_proto, distance_proto)
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
        Retrieves online feature values for the specified embeddings or query string.

        Args:
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            requested_features: The list of features to retrieve.
            embedding: The embeddings to use for retrieval (optional).
            top_k: The number of documents to retrieve.
            distance_metric: Distance metric to use for retrieval (optional).
            query_string: The query string to search for using keyword search (optional).

        Returns:
            List of tuples containing the event timestamp, entity key, and a dictionary of
            feature name to feature values.
        """
        if embedding is None and query_string is None:
            raise ValueError("Either embedding or query_string must be provided")

        if distance_metric and distance_metric.lower() not in DISTANCE_MAPPING:
            raise ValueError(f"Unsupported distance metric: {distance_metric}")

        client = self._get_client(config)
        result = []

        # Vector search
        if embedding is not None:
            search_params = {}
            if distance_metric:
                search_params["search_params"] = models.SearchParams(
                    hnsw_ef=config.online_store.hnsw_ef, exact=False
                )

            filter_conditions = None
            if query_string is not None and config.online_store.enable_text_index:
                # Use text index for efficient search if available
                filter_conditions = models.Filter(
                    must=[
                        models.FieldCondition(
                            key="text_value",
                            match=models.MatchText(text=query_string),
                        ),
                        models.Filter(
                            should=list(
                                [
                                    models.FieldCondition(
                                        key="feature_name",
                                        match=models.MatchValue(value=feat_name),
                                    )
                                    for feat_name in requested_features
                                ]
                            ),
                            must_not=None,
                        ),
                    ]
                )
            elif query_string is not None:
                # Fallback to filtering by feature name only
                text_conditions = []
                for feature_name in requested_features:
                    text_conditions.append(
                        models.FieldCondition(
                            key="feature_name",
                            match=models.MatchValue(value=feature_name),
                        )
                    )

                filter_conditions = models.Filter(
                    must=[
                        models.Filter(
                            should=list(text_conditions),
                            must_not=None,
                        )
                    ]
                )

            points = client.query_points(
                collection_name=table.name,
                query=embedding,
                query_filter=filter_conditions,
                limit=top_k,
                with_payload=True,
                with_vectors=False,
                search_params=search_params,
                using=config.online_store.vector_name or None,
            ).points
        elif query_string is not None:
            if config.online_store.enable_text_index:
                filter_conditions = models.Filter(
                    must=[
                        models.FieldCondition(
                            key="text_value",
                            match=models.MatchText(text=query_string),
                        ),
                        models.Filter(
                            should=list(
                                [
                                    models.FieldCondition(
                                        key="feature_name",
                                        match=models.MatchValue(value=feat_name),
                                    )
                                    for feat_name in requested_features
                                ]
                            ),
                            must_not=None,
                        ),
                    ]
                )

                points = client.query_points(
                    collection_name=table.name,
                    query=[0.0]
                    * config.online_store.vector_len,  # Dummy vector for text-only search
                    query_filter=filter_conditions,
                    limit=top_k,
                    with_payload=True,
                    with_vectors=False,
                    using=config.online_store.vector_name or None,
                ).points
            else:
                # Fallback to scrolling if text index is not enabled
                points = []
                next_offset = None
                stop_scrolling = False

                # Create a filter for specific feature names to reduce data to scan
                feature_filter = models.Filter(
                    should=list(
                        [
                            models.FieldCondition(
                                key="feature_name",
                                match=models.MatchValue(value=feat_name),
                            )
                            for feat_name in requested_features
                        ]
                    )
                )

                while not stop_scrolling:
                    records, next_offset = client.scroll(
                        collection_name=table.name,
                        limit=SCROLL_SIZE,
                        offset=next_offset,
                        with_payload=True,
                        with_vectors=False,
                        scroll_filter=feature_filter,
                    )
                    stop_scrolling = next_offset is None

                    # Filter records that contain the query string
                    for record in records:
                        if record.payload and "feature_name" in record.payload:
                            # Try to decode the feature value and check if it contains the query string
                            try:
                                feature_value = record.payload.get("feature_value", "")
                                if feature_value is not None:
                                    decoded_value = base64.b64decode(
                                        feature_value
                                    ).decode("utf-8", errors="ignore")
                                    if query_string.lower() in decoded_value.lower():
                                        # Cast to ScoredPoint type for compatibility
                                        points.append(record)  # type: ignore

                            except Exception:
                                continue

                    # Limit to top_k results
                    if len(points) >= top_k:
                        points = points[:top_k]
                        break

        # Process and format results
        for point in points:
            payload = point.payload or {}

            # Handle entity_key
            raw_entity_key = payload.get("entity_key")
            if not raw_entity_key:
                continue
                
            entity_key_str = raw_entity_key
            
            # Handle feature name
            feature_name = payload.get("feature_name", "")
            feature_value_encoded = payload.get("feature_value")

            if not all([entity_key_str, feature_name, feature_value_encoded]):
                continue

            # Handle timestamp
            timestamp_val = payload.get("timestamp")
            if timestamp_val is None:
                continue

            # Convert to string explicitly with type checking
            if isinstance(timestamp_val, (str, int, float, datetime)):
                timestamp_str = str(timestamp_val)
                if not timestamp_str:
                    continue

                try:
                    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
                except ValueError:
                    continue
            else:
                continue

            entity_key_proto = EntityKeyProto()
            if isinstance(entity_key_str, str):
                entity_key_bin = base64.b64decode(entity_key_str)
                entity_key_proto = deserialize_entity_key(
                    entity_key_bin,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )

            feature_value_proto = ValueProto()
            if isinstance(feature_value_encoded, str):
                feature_value_proto.ParseFromString(
                    base64.b64decode(feature_value_encoded)
                )

            # Create a dictionary with the feature values
            feature_values = {feature_name: feature_value_proto}

            result.append((timestamp, entity_key_proto, feature_values))

        return_result: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []
        for entry in result:
            return_result.append(entry)
        return return_result
