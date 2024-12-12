"""Milvus online store implementation for Feast."""
from typing import List, Optional, Tuple, Dict, Sequence, Any, Callable
from datetime import datetime
import logging
import base64
import json
import uuid

from pymilvus import Collection, connections, utility, CollectionSchema, FieldSchema, DataType

from feast import RepoConfig, Entity
from feast.feature_view import FeatureView
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.infra.key_encoding_utils import serialize_entity_key, get_list_val_str
from feast.utils import to_naive_utc


class MilvusOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """Online store config for Milvus store"""
    type: str = "milvus"
    host: str = "localhost"
    port: int = 19530
    user: Optional[str] = None
    password: Optional[str] = None
    collection_naming_template: str = "{project}.{table_name}"
    consistency_level: str = "Session"
    batch_size: int = 100


DISTANCE_MAPPING = {
    "cosine": "IP",
    "l2": "L2",
    "dot": "IP",
}


class MilvusOnlineStore(OnlineStore):
    """Online store implementation for Milvus."""

    _client = None

    def _get_client(self, config: RepoConfig):
        """Get or create Milvus client connection.

        Args:
            config: The config for the current feature store.

        Returns:
            A boolean indicating successful connection (Milvus uses global connection state).

        Raises:
            ValueError: If the similarity metric is not supported.
        """
        if not self._client:
            assert isinstance(
                config.online_store, MilvusOnlineStoreConfig
            ), "Invalid type for online store config"

            if config.online_store.similarity and (
                config.online_store.similarity.lower() not in DISTANCE_MAPPING
            ):
                raise ValueError(
                    f"Unsupported distance metric {config.online_store.similarity}"
                )

            try:
                connections.connect(
                    alias="default",
                    host=config.online_store.host,
                    port=config.online_store.port,
                    user=config.online_store.user,
                    password=config.online_store.password,
                )
                self._client = True
            except Exception as e:
                logging.error(f"Error connecting to Milvus: {e}")
                raise

        return self._client

    def _create_collection(self, config: RepoConfig, table: FeatureView):
        """Create a new collection with the proper schema."""
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=36, is_primary=True),
            FieldSchema(name="entity_key", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="feature_name", dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name="feature_value", dtype=DataType.VARCHAR, max_length=2048),
            FieldSchema(name="timestamp", dtype=DataType.VARCHAR, max_length=64),
            FieldSchema(name="created_ts", dtype=DataType.VARCHAR, max_length=64),
            FieldSchema(
                name="vector",
                dtype=DataType.FLOAT_VECTOR,
                dim=config.online_store.vector_len
            ),
        ]

        schema = CollectionSchema(
            fields=fields,
            description=f"Feature store collection for {table.name}"
        )

        collection = Collection(
            name=table.name,
            schema=schema,
            using='default',
        )

        collection.create_index(
            field_name="vector",
            index_params={
                "metric_type": DISTANCE_MAPPING[config.online_store.similarity.lower()],
                "index_type": "IVF_FLAT",
                "params": {"nlist": 1024},
            }
        )
        collection.create_index(
            field_name="entity_key",
            index_params={"index_type": "FLAT"}
        )
        collection.create_index(
            field_name="feature_name",
            index_params={"index_type": "FLAT"}
        )

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """Write a batch of feature values to Milvus."""
        self._get_client(config)

        collection_name = table.name
        if not utility.has_collection(collection_name):
            self._create_collection(config, table)

        collection = Collection(collection_name)
        entities = []

        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )

            timestamp = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)

            for feature_name, value in values.items():
                encoded_value = base64.b64encode(value.SerializeToString()).decode("utf-8")
                vector_val = json.loads(get_list_val_str(value))

                entities.append({
                    "id": str(uuid.uuid4()),
                    "entity_key": entity_key_bin,
                    "feature_name": feature_name,
                    "feature_value": encoded_value,
                    "timestamp": timestamp.isoformat(),
                    "created_ts": created_ts.isoformat() if created_ts else None,
                    "vector": vector_val,
                })

        if entities:
            collection.insert(entities)
            collection.flush()
            if progress:
                progress(len(entities))

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Read feature values for given entity keys from Milvus."""
        self._get_client(config)

        collection_name = table.name
        if not utility.has_collection(collection_name):
            return [(None, None) for _ in entity_keys]

        collection = Collection(collection_name)
        collection.load()

        try:
            expr = None
            if entity_keys:
                entity_key_bins = [
                    serialize_entity_key(
                        key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    )
                    for key in entity_keys
                ]
                expr = f'entity_key in {entity_key_bins}'

            if requested_features:
                feature_expr = f'feature_name in {requested_features}'
                expr = f'{expr} and {feature_expr}' if expr else feature_expr

            results = collection.query(
                expr=expr,
                output_fields=["entity_key", "feature_name", "feature_value", "timestamp"]
            )

            feature_values: Dict[str, Dict[str, ValueProto]] = {}
            timestamps: Dict[str, datetime] = {}

            for record in results:
                entity_key = record["entity_key"]
                feature_name = record["feature_name"]
                feature_value = ValueProto()
                feature_value.ParseFromString(base64.b64decode(record["feature_value"]))

                if entity_key not in feature_values:
                    feature_values[entity_key] = {}
                    timestamps[entity_key] = datetime.fromisoformat(record["timestamp"])

                feature_values[entity_key][feature_name] = feature_value

            return [
                (timestamps.get(key, None), feature_values.get(key, None))
                for key in [
                    serialize_entity_key(
                        key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    )
                    for key in entity_keys
                ]
            ]
        finally:
            collection.release()

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """Handle collection updates and deletions."""
        self._get_client(config)

        for table in tables_to_delete:
            if utility.has_collection(table.name):
                utility.drop_collection(table.name)

        for table in tables_to_keep:
            if not partial:
                if utility.has_collection(table.name):
                    utility.drop_collection(table.name)
            self._create_collection(config, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """Clean up collections and connections."""
        self._get_client(config)

        for table in tables:
            try:
                if utility.has_collection(table.name):
                    utility.drop_collection(table.name)
            except Exception as e:
                logging.error(f"Failed to delete collection {table.name}: {e}")
                raise

        if self._client:
            connections.disconnect("default")
            self._client = None

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_feature: str,
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
        """Retrieve documents from Milvus using vector similarity search."""
        self._get_client(config)

        collection_name = table.name
        if not utility.has_collection(collection_name):
            return []

        collection = Collection(collection_name)
        collection.load()

        try:
            search_metric = None
            if distance_metric:
                if distance_metric.lower() not in DISTANCE_MAPPING:
                    raise ValueError(f"Unsupported distance metric: {distance_metric}")
                search_metric = DISTANCE_MAPPING[distance_metric.lower()]
            else:
                search_metric = DISTANCE_MAPPING[config.online_store.similarity.lower()]

            search_params = {
                "metric_type": search_metric,
                "params": {"nprobe": 10},
            }

            results = collection.search(
                data=[embedding],
                anns_field="vector",
                param=search_params,
                limit=top_k,
                output_fields=[
                    "entity_key",
                    "feature_name",
                    "feature_value",
                    "timestamp",
                    "vector"
                ],
                expr=f'feature_name == "{requested_feature}"'
            )

            output = []
            for hit in results[0]:
                entity_key = hit.entity_key
                feature_value = ValueProto()
                feature_value.ParseFromString(base64.b64decode(hit.feature_value))
                timestamp = datetime.fromisoformat(hit.timestamp)
                distance = hit.score

                entity_key_proto = EntityKeyProto()
                entity_key_proto.ParseFromString(base64.b64decode(entity_key))

                vector_value = ValueProto()
                vector_value.float_list.val.extend(hit.vector)

                distance_value = ValueProto()
                distance_value.double_val = distance

                output.append((
                    timestamp,
                    entity_key_proto,
                    feature_value,
                    vector_value,
                    distance_value
                ))

            return output

        finally:
            collection.release()
