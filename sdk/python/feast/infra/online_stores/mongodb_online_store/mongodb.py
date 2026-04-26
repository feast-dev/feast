from __future__ import annotations

from datetime import datetime
from logging import getLogger
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

try:
    from pymongo import AsyncMongoClient, MongoClient, UpdateOne
    from pymongo.asynchronous.collection import AsyncCollection
    from pymongo.collection import Collection
    from pymongo.driver_info import DriverInfo
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("mongodb", str(e))

import feast.version
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.supported_async_methods import SupportedAsyncMethods
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.type_map import (
    feast_value_type_to_python_type,
    python_values_to_proto_values,
)

logger = getLogger(__name__)

DRIVER_METADATA = DriverInfo(name="Feast", version=feast.version.get_version())


class MongoDBOnlineStoreConfig(FeastConfigBaseModel):
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

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
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
        """
        if not isinstance(config.online_store, MongoDBOnlineStoreConfig):
            raise RuntimeError(f"{config.online_store.type = }. It must be mongodb.")

        clxn = self._get_collection(repo_config=config)

        if tables_to_delete:
            unset_fields = {}
            for fv in tables_to_delete:
                unset_fields[f"features.{fv.name}"] = ""
                unset_fields[f"event_timestamps.{fv.name}"] = ""

            clxn.update_many({}, {"$unset": unset_fields})

        # Note: entities_to_delete contains Entity definitions (metadata), not entity instances.
        # Like other online stores, we don't need to do anything with entities_to_delete here.

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
        registry=None,
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


# TODO
#   - Vector Search (requires atlas image in testcontainers or similar)
