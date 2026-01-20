# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
MongoDB Online Store implementation for Feast.

This module provides an online store implementation using MongoDB, with support for:
- MongoDB Atlas (primary target)
- Core MongoDB (for basic functionality)
- Async operations using PyMongo's AsyncMongoClient
- Vector search via Atlas Vector Search
- TTL-based feature expiration
"""

import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from pydantic import StrictBool, StrictInt, StrictStr

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.helpers import _to_naive_utc
from feast.infra.online_stores.online_store import OnlineStore, SupportedAsyncMethods
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.infra.utils.mongodb.connection_utils import (
    get_async_mongo_client,
    get_collection_name,
    get_mongo_client,
    is_atlas,
    is_atlas_async,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel

try:
    from pymongo import MongoClient
    from pymongo.errors import OperationFailure
    from pymongo import AsyncMongoClient
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("mongodb", str(e))

logger = logging.getLogger(__name__)


class MongoDBOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """
    Configuration for MongoDB Online Store.

    Supports both MongoDB Atlas and core MongoDB deployments.
    """

    type: Literal[
        "mongodb", "feast.infra.online_stores.mongodb_online_store.mongodb.MongoDBOnlineStore"
    ] = "mongodb"
    """Online store type selector"""

    # Connection settings
    connection_string: StrictStr
    """MongoDB connection URI (mongodb:// or mongodb+srv:// for Atlas)"""

    database: StrictStr = "feast"
    """Database name for online store"""

    # Performance settings
    max_pool_size: StrictInt = 50
    """Maximum connection pool size"""

    min_pool_size: StrictInt = 10
    """Minimum connection pool size"""

    # TTL settings
    ttl_seconds: Optional[StrictInt] = None
    """Optional TTL for documents in seconds (uses MongoDB TTL index)"""

    # Atlas-specific features
    atlas_vector_search_enabled: StrictBool = False
    """Enable Atlas Vector Search for retrieve_online_documents_v2"""

    vector_index_name: StrictStr = "vector_index"
    """Name of the Atlas vector search index"""

    vector_similarity_metric: StrictStr = "cosine"
    """Similarity metric: 'cosine', 'euclidean', or 'dotProduct'"""

    # Write concern
    write_concern_w: StrictInt = 1
    """Write concern w parameter (1=primary, 'majority'=majority, etc.)"""

    # Read preference
    read_preference: StrictStr = "primaryPreferred"
    """Read preference: 'primary', 'primaryPreferred', 'secondary', etc."""


class MongoDBOnlineStore(OnlineStore):
    """
    MongoDB implementation of the Feast online store interface.

    This implementation supports:
    - Synchronous and asynchronous operations
    - MongoDB Atlas Vector Search for similarity search
    - TTL-based feature expiration
    - Connection pooling for performance

    Document structure:
        {
            _id: ObjectId,
            entity_key_hash: "md5_hash",
            features: {
                "feature1": <protobuf_binary>,
                "feature2": <protobuf_binary>
            },
            event_timestamp: ISODate,
            created_timestamp: ISODate (optional),
            vector_embedding: [float, ...] (optional, for vector search),
            expire_at: ISODate (optional, for TTL)
        }
    """

    _client: Optional[MongoClient] = None
    _client_async: Optional[AsyncMongoClient] = None

    def _get_client(self, config: MongoDBOnlineStoreConfig) -> MongoClient:
        """Get or create synchronous MongoDB client."""
        if not self._client:
            self._client = get_mongo_client(
                connection_string=config.connection_string,
                max_pool_size=config.max_pool_size,
                min_pool_size=config.min_pool_size,
                write_concern_w=config.write_concern_w,
                read_preference=config.read_preference,
            )
        return self._client

    def _get_client_async(self, config: MongoDBOnlineStoreConfig) -> AsyncMongoClient:
        """Get or create asynchronous MongoDB client."""
        if not self._client_async:
            self._client_async = get_async_mongo_client(
                connection_string=config.connection_string,
                max_pool_size=config.max_pool_size,
                min_pool_size=config.min_pool_size,
                write_concern_w=config.write_concern_w,
                read_preference=config.read_preference,
            )
        return self._client_async

    @staticmethod
    def _compute_entity_key_hash(entity_key_bin: bytes) -> str:
        """Compute MD5 hash of entity key for efficient indexing."""
        return hashlib.md5(entity_key_bin).hexdigest()

    @property
    def async_supported(self) -> SupportedAsyncMethods:
        """Indicate that this online store supports async operations."""
        return SupportedAsyncMethods(read=True, write=True)

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Write a batch of feature rows to MongoDB.

        Args:
            config: Feast repository configuration
            table: Feature view to write to
            data: List of (entity_key, features, event_timestamp, created_timestamp) tuples
            progress: Optional progress callback
        """
        assert config.online_store.type == "mongodb"
        online_config: MongoDBOnlineStoreConfig = config.online_store

        client = self._get_client(online_config)
        db = client[online_config.database]
        collection_name = get_collection_name(config.project, table.name)
        collection = db[collection_name]

        bulk_operations = []
        for entity_key, values, timestamp, created_ts in data:
            # Serialize entity key
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            entity_key_hash = self._compute_entity_key_hash(entity_key_bin)

            # Convert timestamps to naive UTC
            timestamp = _to_naive_utc(timestamp)
            created_ts = _to_naive_utc(created_ts) if created_ts else None

            # Serialize feature values
            features_dict = {}
            for feature_name, val in values.items():
                features_dict[feature_name] = val.SerializeToString()

            # Build document
            document = {
                "entity_key_hash": entity_key_hash,
                "features": features_dict,
                "event_timestamp": timestamp,
            }

            if created_ts:
                document["created_timestamp"] = created_ts

            # Add TTL field if configured
            if online_config.ttl_seconds:
                expire_at = timestamp + timedelta(seconds=online_config.ttl_seconds)
                document["expire_at"] = expire_at

            # Upsert operation
            bulk_operations.append({
                "filter": {"entity_key_hash": entity_key_hash},
                "update": {"$set": document},
                "upsert": True,
            })

            if progress:
                progress(1)

        # Execute bulk write if there are operations
        if bulk_operations:
            from pymongo import UpdateOne
            collection.bulk_write([
                UpdateOne(op["filter"], op["update"], upsert=op["upsert"])
                for op in bulk_operations
            ])

    async def online_write_batch_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """Async version of online_write_batch."""
        assert config.online_store.type == "mongodb"
        online_config: MongoDBOnlineStoreConfig = config.online_store

        client = self._get_client_async(online_config)
        db = client[online_config.database]
        collection_name = get_collection_name(config.project, table.name)
        collection = db[collection_name]

        bulk_operations = []
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            entity_key_hash = self._compute_entity_key_hash(entity_key_bin)
            timestamp = _to_naive_utc(timestamp)
            created_ts = _to_naive_utc(created_ts) if created_ts else None

            features_dict = {
                feature_name: val.SerializeToString()
                for feature_name, val in values.items()
            }

            document = {
                "entity_key_hash": entity_key_hash,
                "features": features_dict,
                "event_timestamp": timestamp,
            }

            if created_ts:
                document["created_timestamp"] = created_ts

            if online_config.ttl_seconds:
                expire_at = timestamp + timedelta(seconds=online_config.ttl_seconds)
                document["expire_at"] = expire_at

            bulk_operations.append({
                "filter": {"entity_key_hash": entity_key_hash},
                "update": {"$set": document},
                "upsert": True,
            })

            if progress:
                progress(1)

        if bulk_operations:
            from pymongo import UpdateOne
            await collection.bulk_write([
                UpdateOne(op["filter"], op["update"], upsert=op["upsert"])
                for op in bulk_operations
            ])

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Read feature values from MongoDB.

        Args:
            config: Feast repository configuration
            table: Feature view to read from
            entity_keys: List of entity keys to read
            requested_features: Optional list of specific features to retrieve

        Returns:
            List of (event_timestamp, features_dict) tuples, one per entity key
        """
        assert config.online_store.type == "mongodb"
        online_config: MongoDBOnlineStoreConfig = config.online_store

        client = self._get_client(online_config)
        db = client[online_config.database]
        collection_name = get_collection_name(config.project, table.name)
        collection = db[collection_name]

        # Compute entity key hashes
        entity_key_hashes = [
            self._compute_entity_key_hash(
                serialize_entity_key(
                    key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
            )
            for key in entity_keys
        ]

        # Query MongoDB
        documents = list(collection.find({"entity_key_hash": {"$in": entity_key_hashes}}))

        # Build lookup map
        doc_map = {doc["entity_key_hash"]: doc for doc in documents}

        # Build results in order of entity_keys
        results = []
        for entity_key_hash in entity_key_hashes:
            doc = doc_map.get(entity_key_hash)

            if not doc:
                results.append((None, None))
                continue

            event_ts = doc.get("event_timestamp")
            features_dict = {}

            for feature_name, feature_bin in doc.get("features", {}).items():
                if requested_features is None or feature_name in requested_features:
                    val = ValueProto()
                    val.ParseFromString(feature_bin)
                    features_dict[feature_name] = val

            results.append((event_ts, features_dict if features_dict else None))

        return results

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Async version of online_read."""
        assert config.online_store.type == "mongodb"
        online_config: MongoDBOnlineStoreConfig = config.online_store

        client = self._get_client_async(online_config)
        db = client[online_config.database]
        collection_name = get_collection_name(config.project, table.name)
        collection = db[collection_name]

        entity_key_hashes = [
            self._compute_entity_key_hash(
                serialize_entity_key(
                    key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
            )
            for key in entity_keys
        ]

        cursor = collection.find({"entity_key_hash": {"$in": entity_key_hashes}})
        documents = await cursor.to_list(length=len(entity_key_hashes))

        doc_map = {doc["entity_key_hash"]: doc for doc in documents}

        results = []
        for entity_key_hash in entity_key_hashes:
            doc = doc_map.get(entity_key_hash)

            if not doc:
                results.append((None, None))
                continue

            event_ts = doc.get("event_timestamp")
            features_dict = {}

            for feature_name, feature_bin in doc.get("features", {}).items():
                if requested_features is None or feature_name in requested_features:
                    val = ValueProto()
                    val.ParseFromString(feature_bin)
                    features_dict[feature_name] = val

            results.append((event_ts, features_dict if features_dict else None))

        return results

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Update MongoDB resources (create/delete collections and indexes).

        Args:
            config: Feast repository configuration
            tables_to_delete: Feature views whose collections should be deleted
            tables_to_keep: Feature views whose collections should be created/updated
            entities_to_delete: Entities to delete (unused for MongoDB)
            entities_to_keep: Entities to keep (unused for MongoDB)
            partial: Whether this is a partial update
        """
        assert config.online_store.type == "mongodb"
        online_config: MongoDBOnlineStoreConfig = config.online_store

        client = self._get_client(online_config)
        db = client[online_config.database]

        # Delete collections for removed feature views
        for table in tables_to_delete:
            collection_name = get_collection_name(config.project, table.name)
            db[collection_name].drop()
            logger.info(f"Dropped MongoDB collection: {collection_name}")

        # Create/update collections and indexes for feature views
        for table in tables_to_keep:
            collection_name = get_collection_name(config.project, table.name)
            collection = db[collection_name]

            # Create unique index on entity_key_hash
            try:
                collection.create_index("entity_key_hash", unique=True, name="idx_entity_key_hash")
                logger.info(f"Created index on entity_key_hash for {collection_name}")
            except OperationFailure as e:
                if "already exists" not in str(e):
                    raise

            # Create TTL index if configured
            if online_config.ttl_seconds:
                try:
                    collection.create_index(
                        "expire_at",
                        expireAfterSeconds=0,  # TTL is encoded in the expire_at field
                        name="idx_ttl"
                    )
                    logger.info(f"Created TTL index for {collection_name}")
                except OperationFailure as e:
                    if "already exists" not in str(e):
                        raise

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Tear down MongoDB resources (drop collections).

        Args:
            config: Feast repository configuration
            tables: Feature views whose collections should be dropped
            entities: Entities (unused for MongoDB)
        """
        assert config.online_store.type == "mongodb"
        online_config: MongoDBOnlineStoreConfig = config.online_store

        client = self._get_client(online_config)
        db = client[online_config.database]

        for table in tables:
            collection_name = get_collection_name(config.project, table.name)
            db[collection_name].drop()
            logger.info(f"Dropped MongoDB collection during teardown: {collection_name}")

    # TODO: Implement vector search methods in next iteration
    # def retrieve_online_documents_v2(...)
    # async def initialize(...)
    # async def close(...)
