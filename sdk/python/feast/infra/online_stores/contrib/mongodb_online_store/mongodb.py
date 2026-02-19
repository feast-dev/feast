from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.type_map import python_values_to_proto_values, feast_value_type_to_python_type


logger = logging.getLogger(__name__)

class MongoDBOnlineStoreConfig(FeastConfigBaseModel):
    """MongoDB configuration.

    For a description of kwargs that may be passed to MongoClient,
    see https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html
    """

    type: Literal[
        "mongodb", "feast.infra.online_stores.mongodb_online_store.mongodb.MongoDBOnlineStore"
    ] = "mongodb"
    """Online store type selector"""
    connection_string: str = "mongodb://localhost:27017"
    database_name: str = "features"  # todo - consider removing, and using repo_config.project
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
            "driver_stats": 2026-01-01 12:00:00+00:00 }
            "pricing":: 2026-01-21 12:00:00+00:00 }
        "created_timestamp": 2026-01-21 12:00:00+00:00
    }
    """

    _client: Optional[MongoClient] = None
    _collection: Optional[Collection] = None

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]],
        progress: Optional[Callable[[int], Any]] = None,
    ) -> None:
        """
        Writes a batch of feature values to the online store.

        data:
          [
            {(}
              entity_key_bytes,
              { feature_ref: ValueProto },
              { feature_view_name: event_timestamp_unix }
            )
          ]
        """
        clxn = self._get_collection(config)
        ops = []
        for row in data:
            entity_key, proto_values, event_timestamp, created_timestamp = row
            entity_id = serialize_entity_key(entity_key)
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
        if ops:
            clxn.bulk_write(ops, ordered=False)
        if progress:
            progress(1)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Read features for a batch of entities.
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
            projection.update({f"features.{table.name}.{x}": 1 for x in requested_features})
        else:
            projection[f"features.{table.name}"] = 1

        cursor = clxn.find(query_filter, projection=projection)
        docs = {doc["_id"]: doc for doc in cursor}

        # Order and format output
        results: List[Optional[Dict[str, ValueProto]]] = []
        for entity_id in ids:
            doc = docs.get(entity_id)
            if doc is None:
                results.append((None, None))
                continue

            # Extract timestamp
            ts = doc.get("event_timestamps", {}).get(table.name)
            # Extract features
            features_raw = doc.get("features", {}).get(table.name, {})

            features_proto = {k: python_values_to_proto_values([v])[0] for k, v in features_raw.items()}  # todo refactor:  v inefficient
            results.append((ts, features_proto))
        return results

            # todo v inefficient: the method below must infer types. additionally we're iterating over rows
            #  feature.dtype is held in table.feature.dtype.
        """
            Feast’s online read is row-oriented in output, but type conversion is naturally column-oriented.
            Instead of:
                For each entity → convert each feature individually
            You should:
                1.	Gather values for a single feature across all entities.
                2.	Convert them in one call.
                3.	Then reassemble row-wise.
            
            Efficient Pattern
            Assume: 
                •	ids is ordered
                •	requested_features is defined (handle case)
                •	docs is your _id -> doc mapping
            Step 1: Extract raw values column-wise
            
 
            # Step 1: Extract raw values column-wise # (aligned by ordered ids column-wise)
            raw_feature_columns = {feature: [] for feature in requested_features}

            for entity_id in ids:
                doc = docs.get(entity_id)
                feature_dict = (
                    doc.get("features", {}).get(table.name, {})
                    if doc else {}
                )

                for feature in requested_features:
                    raw_feature_columns[feature].append(
                        feature_dict.get(feature)
                    )

            # Step 2: Convert per feature
            # You need feature types. We can get these from the table!
            #   The following will map feature.name to its value type. This is across columns
            #   features_raw contains the columns for a single row (entity)
            #   feature_type_map is done outside the entity loop
            feature_type_map = {
                feature.name: feature.dtype.to_value_type()
                for feature in table.features
            }
            proto_feature_columns = {}
            for feature_name, raw_values in raw_feature_columns.items():
                proto_feature_columns[feature_name] = python_values_to_proto_values(
                    raw_values,
                    feature_type=feature_type_map[feature_name],
                )

            # Step 3: Reassemble row-wise
            results = []

            for i, entity_id in enumerate(ids):
                doc = docs.get(entity_id)

                if doc is None:
                    results.append((None, None))
                    continue

                ts = doc.get("event_timestamps", {}).get(table.name)

                row_features = {
                    feature_name: proto_feature_columns[feature_name][i]
                    for feature_name in requested_features
                }

                results.append((ts, row_features))

        return results
        """
    # ------------------------------------------------------------------

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
        if config.online_store.type != "mongodb":
            raise RuntimeError(f"{config.online_store.type = }. It must be mongodb.")

        clxn = self._get_collection(repo_config=config)

        if tables_to_delete:
            unset_fields = {}
            for fv in tables_to_delete:
                unset_fields[f"features.{fv.name}"] = ""
                unset_fields[f"event_timestamps.{fv.name}"] = ""

            clxn.update_many({}, {"$unset": unset_fields})

        # Delete specific entities
        if entities_to_delete:
            logger.warning(f"CHECK FORM. Can we call to_proto()?: {entities_to_delete = }")
            ids = [serialize_entity_key(e.to_proto()) for e in entities_to_delete]
            clxn.delete_many({"_id": {"$in": ids}})


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
        assert config.online_store.type == "mongodb"
        clxn = self._get_collection(repo_config=config)
        clxn.drop()
        self._get_client(config).close()


    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_client(self, config: RepoConfig):
        """Returns a connection to the server."""
        online_store_config = config.online_store
        if not isinstance(online_store_config, MongoDBOnlineStoreConfig):
            raise ValueError(f"config.online_store should be MongoDBOnlineStoreConfig, got {online_store_config}")
        if self._client is None:
            online_config = config.online_store
            if not isinstance(online_config, MongoDBOnlineStoreConfig):
                logger.warning(f"config.online_store passed to _get_client is not a MongoDBOnlineStoreConfig. It's of type {type(online_config)}")
            self._client = MongoClient(online_config.connection_string, **online_config.client_kwargs)
        return self._client

    def _get_collection(self, repo_config: RepoConfig) -> Collection:
        """Returns a connection to the online store collection."""
        if self._collection is None:
            self._client = self._get_client(repo_config)
            online_config = repo_config.online_store
            db = self._client[online_config.database_name]
            clxn_name = f"{repo_config.project}_{online_config.collection_suffix}"
            if clxn_name not in db.list_collection_names():
                self._collection = db.create_collection(clxn_name)
            self._collection = db[clxn_name]
        return self._collection

# TODO
#   - Implement async API
