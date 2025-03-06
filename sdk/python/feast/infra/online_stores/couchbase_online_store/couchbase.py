import base64
import logging
import warnings
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

import pytz
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import (
    CollectionAlreadyExistsException,
    DocumentNotFoundException,
    ScopeAlreadyExistsException,
)
from couchbase.options import ClusterOptions
from pydantic import StrictStr

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel

logger = logging.getLogger(__name__)
warnings.simplefilter("once", RuntimeWarning)


class CouchbaseOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the Couchbase online store.
    """

    type: Literal["couchbase.online"] = "couchbase.online"

    connection_string: Optional[StrictStr] = None
    user: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    bucket_name: Optional[StrictStr] = None
    kv_port: Optional[int] = None


class CouchbaseOnlineStore(OnlineStore):
    """
    An online store implementation that uses Couchbase.
    """

    _cluster = None

    def _get_conn(self, config: RepoConfig, scope_name: str, collection_name: str):
        """
        Obtain a connection to the Couchbase cluster and get the specific scope and collection.
        """
        online_store_config = config.online_store
        assert isinstance(online_store_config, CouchbaseOnlineStoreConfig)

        if not self._cluster:
            self._cluster = Cluster(
                f"{online_store_config.connection_string or 'couchbase://127.0.0.1'}:{online_store_config.kv_port or '11210'}",
                ClusterOptions(
                    PasswordAuthenticator(
                        online_store_config.user or "Administrator",
                        online_store_config.password or "password",
                    ),
                    network="external",
                ),
            )

            self.bucket = self._cluster.bucket(
                online_store_config.bucket_name or "feast"
            )

        # Get the specific scope and collection
        scope = self.bucket.scope(scope_name)
        self.collection = scope.collection(collection_name)

        return self.collection

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
        Write a batch of feature data to the online Couchbase store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView.
            data: a list of quadruplets containing Feature data. Each
                  quadruplet contains an Entity Key, a dict containing feature
                  values, an event timestamp for the row, and
                  the created timestamp for the row if it exists.
            progress: Optional function to be called once every mini-batch of
                      rows is written to the online store. Can be used to
                      display progress.
        """
        warnings.warn(
            "This online store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        project = config.project
        scope_name = f"{project}_{table.name}_scope"
        collection_name = f"{project}_{table.name}_collection"
        collection = self._get_conn(config, scope_name, collection_name)

        for entity_key, values, timestamp, created_ts in data:
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            timestamp = _to_naive_utc(timestamp).isoformat()  # Convert to ISO format
            if created_ts is not None:
                created_ts = _to_naive_utc(
                    created_ts
                ).isoformat()  # Convert to ISO format

            for feature_name, val in values.items():
                document_id = _document_id(project, table, entity_key_str, feature_name)

                # Serialize the Protobuf to binary and then encode it in base64
                binary_value = val.SerializeToString()
                base64_value = base64.b64encode(binary_value).decode("utf-8")

                # Store metadata and base64-encoded Protobuf binary in JSON-compatible format
                document_content = {
                    "metadata": {
                        "event_ts": timestamp,
                        "created_ts": created_ts,
                        "feature_name": feature_name,
                    },
                    "value": base64_value,  # Store binary as base64 encoded string
                }

                try:
                    collection.upsert(
                        document_id, document_content
                    )  # Upsert the document
                except Exception as e:
                    logger.exception(f"Error upserting document {document_id}: {e}")

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
        Read feature values pertaining to the requested entities from
        the online store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView.
            entity_keys: a list of entity keys that should be read
                         from the FeatureStore.
            requested_features: Optional list of feature names to read.
        """
        warnings.warn(
            "This online store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        project = config.project

        scope_name = f"{project}_{table.name}_scope"
        collection_name = f"{project}_{table.name}_collection"

        collection = self._get_conn(config, scope_name, collection_name)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, Any]]]] = []
        for entity_key in entity_keys:
            entity_key_str = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            ).hex()
            try:
                features = {}
                for feature_name in requested_features or []:
                    document_id = _document_id(
                        project, table, entity_key_str, feature_name
                    )

                    # Fetch metadata and value (base64-encoded binary)
                    doc = collection.get(document_id)
                    content = doc.content_as[dict]  # Get the document content as a dict
                    event_ts_str = content["metadata"]["event_ts"]

                    # Convert event_ts from string (ISO format) to datetime object
                    event_ts = datetime.fromisoformat(event_ts_str)

                    base64_value = content["value"]

                    # Decode base64 back to Protobuf binary and then to ValueProto
                    binary_data = base64.b64decode(base64_value)
                    value = ValueProto()
                    value.ParseFromString(binary_data)  # Parse protobuf data

                    # Add the decoded value to the features dictionary
                    features[feature_name] = value

                result.append((event_ts, features))
            except DocumentNotFoundException:
                result.append((None, None))

        return result

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
        Update schema on DB, by creating and destroying tables accordingly.

        Args:
            config: The RepoConfig for the current FeatureStore.
            tables_to_delete: Tables to delete from the Online Store.
            tables_to_keep: Tables to keep in the Online Store.
            entities_to_delete: Entities to delete from the Online Store.
            entities_to_keep: Entities to keep in the Online Store.
            partial: Whether to partially update the schema.
        """
        warnings.warn(
            "This online store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        project = config.project

        for table in tables_to_keep:
            scope_name = f"{project}_{table.name}_scope"
            collection_name = f"{project}_{table.name}_collection"
            self._get_conn(config, scope_name, collection_name)
            cm = self.bucket.collections()

            # Check and create scope
            try:
                cm.create_scope(scope_name)
                logger.info(f"Created scope: {scope_name}")
            except ScopeAlreadyExistsException:
                logger.error(f"Scope {scope_name} already exists")
            except Exception as e:
                logger.error(f"Error creating scope {scope_name}: {e}")

            # Check and create collection
            try:
                cm.create_collection(scope_name, collection_name)
                logger.info(
                    f"Created collection: {collection_name} in scope: {scope_name}"
                )
            except CollectionAlreadyExistsException:
                logger.error(
                    f"Collection {collection_name} already exists in {scope_name}"
                )
            except Exception as e:
                logger.error(f"Error creating collection {collection_name}: {e}")

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Delete tables from the database.

        Args:
            config: The RepoConfig for the current FeatureStore.
            tables: Tables to delete from the feature repo.
            entities: Entities to delete from the feature repo.
        """
        warnings.warn(
            "This online store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        project = config.project

        for table in tables:
            scope_name = f"{project}_{table.name}_scope"
            collection_name = f"{project}_{table.name}_collection"
            self._get_conn(config, scope_name, collection_name)
            cm = self.bucket.collections()
            try:
                # dropping the scope will also drop the nested collection(s)
                cm.drop_scope(scope_name)
            except Exception as e:
                logger.error(f"Error removing collection or scope: {e}")


def _document_id(
    project: str, table: FeatureView, entity_key_str: str, feature_name: str
) -> str:
    return f"{project}:{table.name}:{entity_key_str}:{feature_name}"


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
