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

import mmh3

from feast import FeatureTable, utils
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import DatastoreOnlineStoreConfig, RepoConfig

try:
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import datastore
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError, FeastProviderLoginError

    raise FeastExtrasDependencyImportError("gcp", str(e))


ProtoBatch = Sequence[
    Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
]


class DatastoreOnlineStore(OnlineStore):
    """
    OnlineStore is an object used for all interaction between Feast and the service used for offline storage of
    features.
    """

    @classmethod
    def _initialize_client(cls, online_config: DatastoreOnlineStoreConfig):

        try:
            return datastore.Client(
                project=online_config.project_id, namespace=online_config.namespace,
            )
        except DefaultCredentialsError as e:
            raise FeastProviderLoginError(
                str(e)
                + '\nIt may be necessary to run "gcloud auth application-default login" if you would like to use your '
                "local Google Cloud account "
            )

    @classmethod
    def online_write_batch(
        cls,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:

        online_config = config.online_store
        assert isinstance(online_config, DatastoreOnlineStoreConfig)
        client = cls._initialize_client(online_config)

        write_concurrency = online_config.write_concurrency
        write_batch_size = online_config.write_batch_size
        feast_project = config.project

        pool = ThreadPool(processes=write_concurrency)
        pool.map(
            lambda b: cls._write_minibatch(client, feast_project, table, b, progress),
            cls._to_minibatches(data, batch_size=write_batch_size),
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
            document_id = compute_datastore_entity_id(entity_key)

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

    @classmethod
    def online_read(
        cls,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:

        online_config = config.online_store
        assert isinstance(online_config, DatastoreOnlineStoreConfig)
        client = cls._initialize_client(online_config)

        feast_project = config.project

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for entity_key in entity_keys:
            document_id = compute_datastore_entity_id(entity_key)
            key = client.key(
                "Project", feast_project, "Table", table.name, "Row", document_id
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


def compute_datastore_entity_id(entity_key: EntityKeyProto) -> str:
    """
    Compute Datastore Entity id given Feast Entity Key.

    Remember that Datastore Entity is a concept from the Datastore data model, that has nothing to
    do with the Entity concept we have in Feast.
    """
    return mmh3.hash_bytes(serialize_entity_key(entity_key)).hex()
