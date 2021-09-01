# Copyright 2021 The Feast Authors
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
import itertools
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from pydantic import PositiveInt, StrictStr
from pydantic.typing import Literal

from feast import Entity, FeatureTable, utils
from feast.feature_view import FeatureView
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig

try:
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import datastore
    from google.cloud.datastore.client import Key
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError, FeastProviderLoginError

    raise FeastExtrasDependencyImportError("gcp", str(e))


ProtoBatch = Sequence[
    Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
]


class DatastoreOnlineStoreConfig(FeastConfigBaseModel):
    """ Online store config for GCP Datastore """

    type: Literal["datastore"] = "datastore"
    """ Online store type selector"""

    project_id: Optional[StrictStr] = None
    """ (optional) GCP Project Id """

    namespace: Optional[StrictStr] = None
    """ (optional) Datastore namespace """

    write_concurrency: Optional[PositiveInt] = 40
    """ (optional) Amount of threads to use when writing batches of feature rows into Datastore"""

    write_batch_size: Optional[PositiveInt] = 50
    """ (optional) Amount of feature rows per batch being written into Datastore"""


class DatastoreOnlineStore(OnlineStore):
    """
    OnlineStore is an object used for all interaction between Feast and the service used for offline storage of
    features.
    """

    _client: Optional[datastore.Client] = None

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        """
        online_config = config.online_store
        assert isinstance(online_config, DatastoreOnlineStoreConfig)
        client = self._get_client(online_config)
        feast_project = config.project

        for table in tables_to_keep:
            key = client.key("Project", feast_project, "Table", table.name)
            entity = datastore.Entity(
                key=key, exclude_from_indexes=("created_ts", "event_ts", "values")
            )
            entity.update({"created_ts": datetime.utcnow()})
            client.put(entity)

        for table in tables_to_delete:
            _delete_all_values(
                client, client.key("Project", feast_project, "Table", table.name)
            )

            # Delete the table metadata datastore entity
            key = client.key("Project", feast_project, "Table", table.name)
            client.delete(key)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        """
        There's currently no teardown done for Datastore.
        """
        online_config = config.online_store
        assert isinstance(online_config, DatastoreOnlineStoreConfig)
        client = self._get_client(online_config)
        feast_project = config.project

        for table in tables:
            _delete_all_values(
                client, client.key("Project", feast_project, "Table", table.name)
            )

            # Delete the table metadata datastore entity
            key = client.key("Project", feast_project, "Table", table.name)
            client.delete(key)

    def _get_client(self, online_config: DatastoreOnlineStoreConfig):

        if not self._client:
            try:
                self._client = datastore.Client(
                    project=online_config.project_id, namespace=online_config.namespace,
                )
            except DefaultCredentialsError as e:
                raise FeastProviderLoginError(
                    str(e)
                    + '\nIt may be necessary to run "gcloud auth application-default login" if you would like to use your '
                    "local Google Cloud account "
                )
        return self._client

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:

        online_config = config.online_store
        assert isinstance(online_config, DatastoreOnlineStoreConfig)
        client = self._get_client(online_config)

        write_concurrency = online_config.write_concurrency
        write_batch_size = online_config.write_batch_size
        feast_project = config.project

        pool = ThreadPool(processes=write_concurrency)
        pool.map(
            lambda b: self._write_minibatch(client, feast_project, table, b, progress),
            self._to_minibatches(data, batch_size=write_batch_size),
        )

    @staticmethod
    def _to_minibatches(data: ProtoBatch, batch_size) -> Iterator[ProtoBatch]:
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

    @staticmethod
    def _write_minibatch(
        client,
        project: str,
        table: Union[FeatureTable, FeatureView],
        data: Sequence[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ):
        entities = []
        for entity_key, features, timestamp, created_ts in data:
            document_id = compute_entity_id(entity_key)

            key = client.key(
                "Project", project, "Table", table.name, "Row", document_id,
            )

            entity = datastore.Entity(
                key=key, exclude_from_indexes=("created_ts", "event_ts", "values")
            )

            entity.update(
                dict(
                    key=entity_key.SerializeToString(),
                    values={k: v.SerializeToString() for k, v in features.items()},
                    event_ts=utils.make_tzaware(timestamp),
                    created_ts=(
                        utils.make_tzaware(created_ts)
                        if created_ts is not None
                        else None
                    ),
                )
            )
            entities.append(entity)
        with client.transaction():
            client.put_multi(entities)

        if progress:
            progress(len(entities))

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:

        online_config = config.online_store
        assert isinstance(online_config, DatastoreOnlineStoreConfig)
        client = self._get_client(online_config)

        feast_project = config.project

        keys: List[Key] = []
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for entity_key in entity_keys:
            document_id = compute_entity_id(entity_key)
            key = client.key(
                "Project", feast_project, "Table", table.name, "Row", document_id
            )
            keys.append(key)

        # NOTE: get_multi doesn't return values in the same order as the keys in the request.
        # Also, len(values) can be less than len(keys) in the case of missing values.
        values = client.get_multi(keys)
        values_dict = {v.key: v for v in values} if values is not None else {}
        for key in keys:
            if key in values_dict:
                value = values_dict[key]
                res = {}
                for feature_name, value_bin in value["values"].items():
                    val = ValueProto()
                    val.ParseFromString(value_bin)
                    res[feature_name] = val
                result.append((value["event_ts"], res))
            else:
                result.append((None, None))

        return result


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
