from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import mmh3
from pytz import utc

from feast import FeatureTable, FeatureView
from feast.infra.provider import Provider
from feast.repo_config import DatastoreOnlineStoreConfig
from feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.types.Value_pb2 import Value as ValueProto

from .key_encoding_utils import serialize_entity_key


def _delete_all_values(client, key) -> None:
    """
    Delete all data under the key path in datastore.
    """
    while True:
        query = client.query(kind="Row", ancestor=key)
        entities = list(query.fetch(limit=1000))
        if not entities:
            return

        for entity in entities:
            print("Deleting: {}".format(entity))
            client.delete(entity.key)


def compute_datastore_entity_id(entity_key: EntityKeyProto) -> str:
    """
    Compute Datastore Entity id given Feast Entity Key.

    Remember that Datastore Entity is a concept from the Datastore data model, that has nothing to
    do with the Entity concept we have in Feast.
    """
    return mmh3.hash_bytes(serialize_entity_key(entity_key)).hex()


def _make_tzaware(t: datetime):
    """ We assume tz-naive datetimes are UTC """
    if t.tzinfo is None:
        return t.replace(tzinfo=utc)
    else:
        return t


class Gcp(Provider):
    _gcp_project_id: Optional[str]

    def __init__(self, config: Optional[DatastoreOnlineStoreConfig]):
        if config:
            self._gcp_project_id = config.project_id
        else:
            self._gcp_project_id = None

    def _initialize_client(self):
        from google.cloud import datastore

        if self._gcp_project_id is not None:
            return datastore.Client(self.project_id)
        else:
            return datastore.Client()

    def update_infra(
        self,
        project: str,
        tables_to_delete: List[Union[FeatureTable, FeatureView]],
        tables_to_keep: List[Union[FeatureTable, FeatureView]],
    ):
        from google.cloud import datastore

        client = self._initialize_client()

        for table in tables_to_keep:
            key = client.key("Project", project, "Table", table.name)
            entity = datastore.Entity(key=key)
            entity.update({"created_ts": datetime.utcnow()})
            client.put(entity)

        for table in tables_to_delete:
            _delete_all_values(
                client, client.key("Project", project, "Table", table.name)
            )

            # Delete the table metadata datastore entity
            key = client.key("Project", project, "Table", table.name)
            client.delete(key)

    def teardown_infra(
        self, project: str, tables: List[Union[FeatureTable, FeatureView]]
    ) -> None:
        client = self._initialize_client()

        for table in tables:
            _delete_all_values(
                client, client.key("Project", project, "Table", table.name)
            )

            # Delete the table metadata datastore entity
            key = client.key("Project", project, "Table", table.name)
            client.delete(key)

    def online_write_batch(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        data: List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime]],
        created_ts: datetime,
    ) -> None:
        from google.cloud import datastore

        client = self._initialize_client()

        for entity_key, features, timestamp in data:
            document_id = compute_datastore_entity_id(entity_key)

            key = client.key(
                "Project", project, "Table", table.name, "Row", document_id,
            )
            with client.transaction():
                entity = client.get(key)
                if entity is not None:
                    if entity["event_ts"] > _make_tzaware(timestamp):
                        # Do not overwrite feature values computed from fresher data
                        continue
                    elif entity["event_ts"] == _make_tzaware(timestamp) and entity[
                        "created_ts"
                    ] > _make_tzaware(created_ts):
                        # Do not overwrite feature values computed from the same data, but
                        # computed later than this one
                        continue
                else:
                    entity = datastore.Entity(key=key)

                entity.update(
                    dict(
                        key=entity_key.SerializeToString(),
                        values={k: v.SerializeToString() for k, v in features.items()},
                        event_ts=_make_tzaware(timestamp),
                        created_ts=_make_tzaware(created_ts),
                    )
                )
                client.put(entity)

    def online_read(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        entity_key: EntityKeyProto,
    ) -> Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]:
        client = self._initialize_client()

        document_id = compute_datastore_entity_id(entity_key)
        key = client.key("Project", project, "Table", table.name, "Row", document_id)
        value = client.get(key)
        if value is not None:
            res = {}
            for feature_name, value_bin in value["values"].items():
                val = ValueProto()
                val.ParseFromString(value_bin)
                res[feature_name] = val
            return value["event_ts"], res
        else:
            return None, None
