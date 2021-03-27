import itertools
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import mmh3
from pytz import utc

from feast import FeatureTable, FeatureView
from feast.infra_provisioner import InfraProvisioner
from feast.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import DatastoreOnlineStoreConfig

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


class Gcp(InfraProvisioner):
    _gcp_project_id: Optional[str]

    def __init__(self, config: Optional[DatastoreOnlineStoreConfig]):
        if config:
            self._gcp_project_id = config.project_id
        else:
            self._gcp_project_id = None

    def _initialize_client(self):
        from google.cloud import datastore

        if self._gcp_project_id is not None:
            return datastore.Client(self._gcp_project_id)
        else:
            return datastore.Client()

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_have: Sequence[Union[FeatureTable, FeatureView]],
        partial: bool,
    ):
        from google.cloud import datastore

        client = self._initialize_client()

        for table in tables_to_have:
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
        self, project: str, tables: Sequence[Union[FeatureTable, FeatureView]]
    ) -> None:
        client = self._initialize_client()

        for table in tables:
            _delete_all_values(
                client, client.key("Project", project, "Table", table.name)
            )

            # Delete the table metadata datastore entity
            key = client.key("Project", project, "Table", table.name)
            client.delete(key)


class OnlineStoreDatastore(OnlineStore):
    _gcp_project_id: Optional[str]

    def __init__(self, config: Optional[DatastoreOnlineStoreConfig]):
        if config:
            self._gcp_project_id = config.project_id
        else:
            self._gcp_project_id = None

    def _initialize_client(self):
        from google.cloud import datastore

        if self._gcp_project_id is not None:
            return datastore.Client(self._gcp_project_id)
        else:
            return datastore.Client()

    def online_write_batch(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        client = self._initialize_client()

        pool = ThreadPool(processes=10)
        pool.map(
            lambda b: _write_minibatch(client, project, table, b, progress),
            _to_minibatches(data),
        )

    def online_read(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        client = self._initialize_client()

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for entity_key in entity_keys:
            document_id = compute_datastore_entity_id(entity_key)
            key = client.key(
                "Project", project, "Table", table.name, "Row", document_id
            )
            value = client.get(key)
            if value is not None:
                res = {}
                for feature_name, value_bin in value["values"].items():
                    val = ValueProto()
                    val.ParseFromString(value_bin)
                    res[feature_name] = val
                result.append((value["event_ts"], res))
            else:
                result.append((None, None))
        return result


ProtoBatch = Sequence[
    Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
]


def _to_minibatches(data: ProtoBatch, batch_size=50) -> Iterator[ProtoBatch]:
    """
    Split data into minibatches, making sure we stay under GCP datastore transaction size
    limits.
    """
    iterable = iter(data)

    while True:
        batch = list(itertools.islice(iterable, batch_size))
        if len(batch) > 0:
            yield batch
        else:
            break


def _write_minibatch(
    client,
    project: str,
    table: Union[FeatureTable, FeatureView],
    data: Sequence[
        Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
    ],
    progress: Optional[Callable[[int], Any]],
):
    from google.api_core.exceptions import Conflict
    from google.cloud import datastore

    num_retries_on_conflict = 3
    row_count = 0
    for retry_number in range(num_retries_on_conflict):
        try:
            row_count = 0
            with client.transaction():
                for entity_key, features, timestamp, created_ts in data:
                    document_id = compute_datastore_entity_id(entity_key)

                    key = client.key(
                        "Project", project, "Table", table.name, "Row", document_id,
                    )

                    entity = client.get(key)
                    if entity is not None:
                        if entity["event_ts"] > _make_tzaware(timestamp):
                            # Do not overwrite feature values computed from fresher data
                            continue
                        elif (
                            entity["event_ts"] == _make_tzaware(timestamp)
                            and created_ts is not None
                            and entity["created_ts"] is not None
                            and entity["created_ts"] > _make_tzaware(created_ts)
                        ):
                            # Do not overwrite feature values computed from the same data, but
                            # computed later than this one
                            continue
                    else:
                        entity = datastore.Entity(key=key)

                    entity.update(
                        dict(
                            key=entity_key.SerializeToString(),
                            values={
                                k: v.SerializeToString() for k, v in features.items()
                            },
                            event_ts=_make_tzaware(timestamp),
                            created_ts=(
                                _make_tzaware(created_ts)
                                if created_ts is not None
                                else None
                            ),
                        )
                    )
                    client.put(entity)
                    row_count += 1

                    if progress:
                        progress(1)
        except Conflict:
            if retry_number == num_retries_on_conflict - 1:
                raise
