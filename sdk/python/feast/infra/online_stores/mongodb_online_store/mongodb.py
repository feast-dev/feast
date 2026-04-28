from __future__ import annotations

import time
from datetime import datetime
from logging import getLogger
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union

try:
    from pymongo import AsyncMongoClient, MongoClient, UpdateOne
    from pymongo.asynchronous.collection import AsyncCollection
    from pymongo.collection import Collection
    from pymongo.driver_info import DriverInfo
    from pymongo.operations import SearchIndexModel
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("mongodb", str(e))

import feast.version
from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import deserialize_entity_key, serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.infra.supported_async_methods import SupportedAsyncMethods
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.type_map import (
    feast_value_type_to_python_type,
    python_values_to_proto_values,
)

logger = getLogger(__name__)

DRIVER_METADATA = DriverInfo(name="Feast", version=feast.version.get_version())


class MongoDBOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """MongoDB configuration.

    For a description of kwargs that may be passed to MongoClient,
    see https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html
    """

    type: Literal["mongodb"] = "mongodb"
    """Online store type selector"""
    connection_string: str = "mongodb://localhost:27017"
    database_name: str = (
        "features"  # todo - consider removing, and using repo_config.project
    )
    collection_suffix: str = "latest"
    client_kwargs: Dict[str, Any] = {}
    # vector_enabled and similarity are inherited from VectorStoreConfig
    vector_index_wait_timeout: int = 60
    """Seconds to wait for a newly created Atlas Search index to become READY."""
    vector_index_wait_poll_interval: float = 1.0
    """Seconds between polls when waiting for an Atlas Search index to become READY."""


class MongoDBOnlineStore(OnlineStore):
    """
    MongoDB implementation of Feast OnlineStore.

    Schema:
      _id: serialized_entity_key (bytes)
      features: { <fv>.<feature>: <native_value> }
      event_timestamps: { "<fv>": datetime }
      created_timestamp: datetime

    For example:
    {
        "_id": b"<serialized_entity_key>",
        "features": {
            "driver_stats": {
                "rating": 4.91,
                "trips_last_7d": 132,
            },
            "pricing": {
                "surge_multiplier": 1.2
            },
        },
        "event_timestamps": {
            "driver_stats": "2026-01-01 12:00:00+00:00",
            "pricing": "2026-01-21 12:00:00+00:00"
        },
        "created_timestamp": "2026-01-21 12:00:00+00:00"
    }
    """

    _client: Optional[MongoClient] = None
    _collection: Optional[Collection] = None
    _client_async: Optional[AsyncMongoClient] = None
    _collection_async: Optional[AsyncCollection] = None

    @staticmethod
    def _build_write_ops(
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
    ) -> List[UpdateOne]:
        """Build the list of UpdateOne upsert operations shared by the sync and async write paths.

        For each row in *data* this method:

        1. Serializes the entity key to bytes using ``serialize_entity_key``.
        2. Converts every ``ValueProto`` feature value to its native Python type
           via ``feast_value_type_to_python_type``.
        3. Constructs a ``$set`` update document that writes feature values under
           ``features.<feature_view_name>.<feature_name>``, the per-view event
           timestamp under ``event_timestamps.<feature_view_name>``, and the
           row-level ``created_timestamp``.
        4. Wraps that in a ``UpdateOne`` with ``upsert=True`` so that existing
           entity documents are updated in-place and new ones are created on first
           write.

        The caller is responsible for executing the returned operations via
        ``collection.bulk_write(ops, ordered=False)`` (sync) or
        ``await collection.bulk_write(ops, ordered=False)`` (async).
        """
        ops = []
        for entity_key, proto_values, event_timestamp, created_timestamp in data:
            entity_id = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            feature_updates = {
                f"features.{table.name}.{field}": feast_value_type_to_python_type(val)
                for field, val in proto_values.items()
            }
            update = {
                "$set": {
                    **feature_updates,
                    f"event_timestamps.{table.name}": event_timestamp,
                    "created_timestamp": created_timestamp,
                },
            }
            ops.append(
                UpdateOne(
                    filter={"_id": entity_id},
                    update=update,
                    upsert=True,
                )
            )
        return ops

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]] = None,
    ) -> None:
        """
        Writes a batch of feature values to the online store.

        data:
          [
            (
              entity_key_bytes,
              { feature_ref: ValueProto },
              event_timestamp,
              created_timestamp,
            )
          ]
        """
        clxn = self._get_collection(config)
        ops = self._build_write_ops(config, table, data)
        if ops:
            clxn.bulk_write(ops, ordered=False)
        if progress:
            progress(len(data))

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Read features for a batch of entities.

        Args:
            config: Feast repo configuration
            table: FeatureView to read from
            entity_keys: List of entity keys to read
            requested_features: Optional list of specific features to read

        Returns:
            List of tuples (event_timestamp, feature_dict) for each entity key
        """
        clxn = self._get_collection(config)

        ids = [
            serialize_entity_key(
                key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            for key in entity_keys
        ]

        query_filter = {"_id": {"$in": ids}}
        projection = {
            "_id": 1,
            f"event_timestamps.{table.name}": 1,
        }
        if requested_features:
            projection.update(
                {f"features.{table.name}.{x}": 1 for x in requested_features}
            )
        else:
            projection[f"features.{table.name}"] = 1

        cursor = clxn.find(query_filter, projection=projection)
        docs = {doc["_id"]: doc for doc in cursor}

        return self._convert_raw_docs_to_proto(ids, docs, table)

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
        """Retrieve documents via MongoDB Atlas Vector Search ($vectorSearch).

        Uses the ``$vectorSearch`` aggregation stage to find the *top_k*
        documents closest to the provided *embedding* vector.  The method
        expects that an Atlas vector search index has already been created for
        the relevant field (see ``update()``).

        Returns a list of 3-tuples ``(event_timestamp, entity_key_proto,
        feature_dict)`` where *feature_dict* includes the requested feature
        values plus a synthetic ``distance`` key with the vector search score.
        """
        if not config.online_store.vector_enabled:
            raise ValueError(
                "Vector search is not enabled in the online store config. "
                "Set vector_enabled=True in MongoDBOnlineStoreConfig."
            )
        if embedding is None:
            raise ValueError(
                "An embedding vector must be provided for MongoDB vector search."
            )

        clxn = self._get_collection(config)

        # Identify the vector field on this feature view
        vector_fields = [f for f in table.features if f.vector_index]
        if not vector_fields:
            raise ValueError(
                f"Feature view '{table.name}' has no fields with vector_index=True."
            )
        vector_field = vector_fields[0]
        path = f"features.{table.name}.{vector_field.name}"
        idx_name = self._vector_search_index_name(table.name, vector_field.name)

        # BSON cannot encode numpy float types — ensure native Python floats.
        query_vector = [float(v) for v in embedding]

        num_candidates = max(top_k * 10, 100)
        pipeline: List[Dict[str, Any]] = [
            {
                "$vectorSearch": {
                    "index": idx_name,
                    "path": path,
                    "queryVector": query_vector,
                    "numCandidates": num_candidates,
                    "limit": top_k,
                }
            },
            {
                "$addFields": {
                    "score": {"$meta": "vectorSearchScore"},
                }
            },
        ]

        results: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []

        for doc in clxn.aggregate(pipeline):
            # Deserialize entity key
            entity_key_bin = doc.get("_id")
            entity_key_proto = (
                deserialize_entity_key(
                    entity_key_bin,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                if entity_key_bin
                else None
            )

            # Event timestamp
            event_ts = doc.get("event_timestamps", {}).get(table.name)

            # Build feature dict from raw doc values
            fv_features = doc.get("features", {}).get(table.name, {})

            # Convert raw values → ValueProto for each requested feature
            feature_dict: Dict[str, ValueProto] = {}
            feature_type_map = {f.name: f.dtype.to_value_type() for f in table.features}

            for feat_name in requested_features:
                raw_val = fv_features.get(feat_name)
                if raw_val is not None:
                    vtype = feature_type_map.get(feat_name)
                    if vtype is not None:
                        protos = python_values_to_proto_values(
                            [raw_val], feature_type=vtype
                        )
                        feature_dict[feat_name] = protos[0]
                    else:
                        # Fall back: try to store as-is
                        feature_dict[feat_name] = _python_value_to_proto(raw_val)

            # Add distance (vector search score)
            score = doc.get("score", 0.0)
            feature_dict["distance"] = ValueProto(float_val=float(score))

            results.append((event_ts, entity_key_proto, feature_dict))

        return results

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """Prepare or update online store.

        With MongoDB, we have a loose schema and lazy creation so there is little to do here.
        Nothing needs to be pre-created for the entities and tables to keep.

        The OnlineStore is a single Collection with the following Document shape.
        {
          "_id": "<serialized_entity_key>",
            "features": {
              "<feature_view_name>": {
                "<feature_name>": value
              }
            }
        }
        We remove any feature views named in tables_to_delete.
        The Entities are serialized in the _id. No schema needs be adjusted.

        When ``vector_enabled`` is set in the online store config, Atlas Vector
        Search indexes are automatically created for feature views containing
        fields with ``vector_index=True`` and dropped for deleted feature views.
        """
        if not isinstance(config.online_store, MongoDBOnlineStoreConfig):
            raise RuntimeError(f"{config.online_store.type = }. It must be mongodb.")

        online_config = config.online_store
        clxn = self._get_collection(repo_config=config)

        # --- Remove deleted feature views (data + vector search indexes) ---
        if tables_to_delete:
            unset_fields = {}
            for fv in tables_to_delete:
                unset_fields[f"features.{fv.name}"] = ""
                unset_fields[f"event_timestamps.{fv.name}"] = ""
            clxn.update_many({}, {"$unset": unset_fields})

            if online_config.vector_enabled:
                self._drop_vector_indexes_for_tables(clxn, tables_to_delete)

        # --- Create vector search indexes for kept feature views ---
        if online_config.vector_enabled:
            self._ensure_vector_indexes(clxn, tables_to_keep, online_config)

        # Note: entities_to_delete contains Entity definitions (metadata), not entity instances.
        # Like other online stores, we don't need to do anything with entities_to_delete here.

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Drop the backing collection and close the client.

        As in update, MongoDB requires very little here.
        """
        if not isinstance(config.online_store, MongoDBOnlineStoreConfig):
            raise RuntimeError(f"{config.online_store.type = }. It must be mongodb.")
        clxn = self._get_collection(repo_config=config)
        clxn.drop()
        if self._client:
            self._client.close()
            self._client = None
            self._collection = None

    async def close(self) -> None:
        """Close the async MongoDB client and release its resources."""
        if self._client_async is not None:
            await self._client_async.close()
            self._client_async = None
            self._collection_async = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    # -- Vector Search helpers ------------------------------------------

    @staticmethod
    def _vector_search_index_name(fv_name: str, field_name: str) -> str:
        """Canonical Atlas vector search index name for a (feature_view, field) pair."""
        return f"{fv_name}__{field_name}__vs_index"

    @staticmethod
    def _vector_search_index_definition(
        path: str,
        num_dimensions: int,
        similarity: str,
    ) -> dict:
        """Return a vector search index definition for ``SearchIndexModel``."""
        return {
            "fields": [
                {
                    "type": "vector",
                    "path": path,
                    "numDimensions": num_dimensions,
                    "similarity": similarity,
                }
            ]
        }

    @staticmethod
    def _wait_for_index_ready(
        collection: Collection,
        index_name: str,
        timeout: int,
        poll_interval: float = 1.0,
    ) -> None:
        """Poll until the named Atlas Search index reaches READY status.

        Raises ``TimeoutError`` if the index does not become queryable
        within *timeout* seconds.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for idx in collection.list_search_indexes(name=index_name):
                if idx.get("status") == "READY" or idx.get("queryable") is True:
                    return
            time.sleep(poll_interval)
        raise TimeoutError(
            f"Atlas search index '{index_name}' did not reach READY "
            f"within {timeout}s. Increase vector_index_wait_timeout in "
            f"MongoDBOnlineStoreConfig if the index needs more time."
        )

    def _drop_vector_indexes_for_tables(
        self,
        collection: Collection,
        tables: Sequence[FeatureView],
    ) -> None:
        """Drop all Atlas vector search indexes belonging to the given feature views."""
        existing = {idx["name"] for idx in collection.list_search_indexes()}
        for fv in tables:
            for field in fv.features:
                idx_name = self._vector_search_index_name(fv.name, field.name)
                if idx_name in existing:
                    logger.info("Dropping vector search index: %s", idx_name)
                    collection.drop_search_index(idx_name)

    def _ensure_vector_indexes(
        self,
        collection: Collection,
        tables: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        online_config: MongoDBOnlineStoreConfig,
    ) -> None:
        """Create Atlas vector search indexes for vector-indexed fields if they don't exist."""
        db = collection.database
        if collection.name not in db.list_collection_names():
            db.create_collection(collection.name)

        existing = {idx["name"] for idx in collection.list_search_indexes()}

        for fv in tables:
            vector_fields = [f for f in fv.features if f.vector_index]
            for field in vector_fields:
                idx_name = self._vector_search_index_name(fv.name, field.name)
                if idx_name in existing:
                    logger.debug("Vector search index '%s' already exists", idx_name)
                    continue

                path = f"features.{fv.name}.{field.name}"
                num_dimensions = field.vector_length
                if not num_dimensions:
                    raise ValueError(
                        f"Field '{field.name}' in feature view '{fv.name}' has "
                        f"vector_index=True but vector_length is not set."
                    )

                similarity = (
                    field.vector_search_metric or online_config.similarity or "cosine"
                )

                definition = self._vector_search_index_definition(
                    path, num_dimensions, similarity
                )
                search_index_model = SearchIndexModel(
                    definition=definition,
                    name=idx_name,
                    type="vectorSearch",
                )
                logger.info(
                    "Creating Atlas vector search index '%s' on path '%s' "
                    "(dims=%d, similarity=%s)",
                    idx_name,
                    path,
                    num_dimensions,
                    similarity,
                )
                collection.create_search_index(model=search_index_model)
                self._wait_for_index_ready(
                    collection,
                    idx_name,
                    online_config.vector_index_wait_timeout,
                    online_config.vector_index_wait_poll_interval,
                )

    # -- Connection helpers ---------------------------------------------

    def _get_client(self, config: RepoConfig):
        """Returns a connection to the server."""
        online_store_config = config.online_store
        if not isinstance(online_store_config, MongoDBOnlineStoreConfig):
            raise ValueError(
                f"config.online_store should be MongoDBOnlineStoreConfig, got {online_store_config}"
            )
        if self._client is None:
            online_config = config.online_store
            self._client = MongoClient(
                online_config.connection_string,
                driver=DRIVER_METADATA,
                **online_config.client_kwargs,
            )
        return self._client

    def _get_collection(self, repo_config: RepoConfig) -> Collection:
        """Returns a connection to the online store collection."""
        if self._collection is None:
            self._client = self._get_client(repo_config)
            assert self._client is not None
            online_config = repo_config.online_store
            db = self._client[online_config.database_name]
            clxn_name = f"{repo_config.project}_{online_config.collection_suffix}"
            self._collection = db[clxn_name]
        return self._collection

    async def _get_client_async(self, config: RepoConfig) -> AsyncMongoClient:
        """Returns an async MongoDB client."""
        if self._client_async is None:
            online_config = config.online_store
            if not isinstance(online_config, MongoDBOnlineStoreConfig):
                raise ValueError(
                    f"config.online_store should be MongoDBOnlineStoreConfig, got {online_config}"
                )
            self._client_async = AsyncMongoClient(
                online_config.connection_string,
                driver=DRIVER_METADATA,
                **online_config.client_kwargs,
            )
        return self._client_async

    async def _get_collection_async(self, repo_config: RepoConfig) -> AsyncCollection:
        """Returns an async connection to the online store collection."""
        if self._collection_async is None:
            self._client_async = await self._get_client_async(repo_config)
            assert self._client_async is not None
            online_config = repo_config.online_store
            db = self._client_async[online_config.database_name]
            clxn_name = f"{repo_config.project}_{online_config.collection_suffix}"
            self._collection_async = db[clxn_name]
        return self._collection_async

    @property
    def async_supported(self) -> SupportedAsyncMethods:
        """Indicates that this online store supports async operations."""
        return SupportedAsyncMethods(read=True, write=True)

    @staticmethod
    def _convert_raw_docs_to_proto(
        ids: list[bytes], docs: dict[bytes, Any], table: FeatureView
    ) -> List[Tuple[Optional[datetime], Optional[dict[str, ValueProto]]]]:
        """Optimized converting values in documents retrieved from MongoDB (BSON) into ValueProto types.

        The conversion itself is done in feast.type_map.python_values_to_proto_values.
        The issue we have is that it is column-oriented, expecting a list of proto values with a single type.
        MongoDB lookups are row-oriented. Plus, we need to ensure ordering of ids.
        So we transform twice to minimize calls to the python/proto converter.

        Luckily, the table, a FeatureView, provides a map from feature name to proto type
        so we don't have to infer types for each feature value.

        Args:
            ids: sorted list of the serialized entity ids requested.
            docs: results of collection find.
            table: The FeatureView of the read, providing the types.
        Returns:
            List of tuples (event_timestamp, feature_dict) for each entity key
        """
        feature_type_map = {
            feature.name: feature.dtype.to_value_type() for feature in table.features
        }

        # Step 1: Extract raw values column-wise (aligned by ordered ids)
        # We need to maintain alignment, so we append None for missing features
        raw_feature_columns: Dict[str, List[Any]] = {
            feature_name: [] for feature_name in feature_type_map
        }

        for entity_id in ids:
            doc = docs.get(entity_id)
            feature_dict = doc.get("features", {}).get(table.name, {}) if doc else {}

            # For each expected feature, append its value or None
            for feature_name in feature_type_map:
                raw_feature_columns[feature_name].append(
                    feature_dict.get(feature_name, None)
                )

        # Step 2: Convert per feature
        proto_feature_columns = {}
        for feature_name, raw_values in raw_feature_columns.items():
            proto_feature_columns[feature_name] = python_values_to_proto_values(
                raw_values,
                feature_type=feature_type_map[feature_name],
            )

        # Step 3: Reassemble row-wise
        results: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        for i, entity_id in enumerate(ids):
            doc = docs.get(entity_id)

            if doc is None:
                results.append((None, None))
                continue

            # Entity document exists (written by some other feature view), but
            # this specific feature view was never written → treat as not found.
            fv_features = doc.get("features", {}).get(table.name)
            if fv_features is None:
                results.append((None, None))
                continue

            ts = doc.get("event_timestamps", {}).get(table.name)

            row_features = {
                feature_name: proto_feature_columns[feature_name][i]
                for feature_name in proto_feature_columns
            }

            results.append((ts, row_features))
        return results

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Asynchronously reads feature values from the online store.

        Args:
            config: Feast repo configuration
            table: FeatureView to read from
            entity_keys: List of entity keys to read
            requested_features: Optional list of specific features to read

        Returns:
            List of tuples (event_timestamp, feature_dict) for each entity key
        """
        clxn = await self._get_collection_async(config)

        # Serialize entity keys
        ids = [
            serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            for entity_key in entity_keys
        ]

        query_filter = {"_id": {"$in": ids}}
        projection = {
            "_id": 1,
            f"event_timestamps.{table.name}": 1,
        }
        if requested_features:
            projection.update(
                {f"features.{table.name}.{x}": 1 for x in requested_features}
            )
        else:
            projection[f"features.{table.name}"] = 1

        cursor = clxn.find(query_filter, projection=projection)
        docs = {doc["_id"]: doc async for doc in cursor}

        # Convert to proto format
        return self._convert_raw_docs_to_proto(ids, docs, table)

    async def online_write_batch_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]] = None,
    ) -> None:
        """
        Asynchronously writes a batch of feature values to the online store.

        Args:
            config: Feast repo configuration
            table: FeatureView to write to
            data: List of tuples (entity_key, features, event_ts, created_ts)
            progress: Optional progress callback
        """
        clxn = await self._get_collection_async(config)
        ops = self._build_write_ops(config, table, data)
        if ops:
            await clxn.bulk_write(ops, ordered=False)
        if progress:
            progress(len(data))


def _python_value_to_proto(value: Any) -> ValueProto:
    """Best-effort conversion of a single Python value to ValueProto."""
    if isinstance(value, float):
        return ValueProto(float_val=value)
    elif isinstance(value, int):
        return ValueProto(int64_val=value)
    elif isinstance(value, str):
        return ValueProto(string_val=value)
    elif isinstance(value, bool):
        return ValueProto(bool_val=value)
    elif isinstance(value, bytes):
        return ValueProto(bytes_val=value)
    elif isinstance(value, list) and all(isinstance(v, float) for v in value):
        proto = ValueProto()
        proto.float_list_val.val.extend(value)
        return proto
    return ValueProto()
