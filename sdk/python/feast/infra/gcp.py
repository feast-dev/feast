import itertools
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import mmh3
import pandas
from tqdm import tqdm

from feast import FeatureTable, utils
from feast.entity import Entity
from feast.errors import FeastProviderLoginError
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.helpers import get_offline_store_from_config
from feast.infra.provider import (
    Provider,
    RetrievalJob,
    _convert_arrow_to_proto,
    _get_column_names,
    _run_field_mapping,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry
from feast.repo_config import DatastoreOnlineStoreConfig, RepoConfig

try:
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import datastore
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("gcp", str(e))


class GcpProvider(Provider):
    _gcp_project_id: Optional[str]
    _namespace: Optional[str]

    def __init__(self, config: RepoConfig):
        assert isinstance(config.online_store, DatastoreOnlineStoreConfig)
        self._gcp_project_id = config.online_store.project_id
        self._namespace = config.online_store.namespace
        self._write_concurrency = config.online_store.write_concurrency
        self._write_batch_size = config.online_store.write_batch_size

        assert config.offline_store is not None
        self.offline_store = get_offline_store_from_config(config.offline_store)

    def _initialize_client(self):
        try:
            return datastore.Client(
                project=self._gcp_project_id, namespace=self._namespace
            )
        except DefaultCredentialsError as e:
            raise FeastProviderLoginError(
                str(e)
                + '\nIt may be necessary to run "gcloud auth application-default login" if you would like to use your '
                "local Google Cloud account "
            )

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):

        client = self._initialize_client()

        for table in tables_to_keep:
            key = client.key("Project", project, "Table", table.name)
            entity = datastore.Entity(
                key=key, exclude_from_indexes=("created_ts", "event_ts", "values")
            )
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
        self,
        project: str,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
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
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        client = self._initialize_client()

        pool = ThreadPool(processes=self._write_concurrency)
        pool.map(
            lambda b: _write_minibatch(client, project, table, b, progress),
            _to_minibatches(data, batch_size=self._write_batch_size),
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

    def materialize_single_feature_view(
        self,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: Registry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ) -> None:
        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            event_timestamp_column,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        start_date = utils.make_tzaware(start_date)
        end_date = utils.make_tzaware(end_date)

        table = self.offline_store.pull_latest_from_table_or_query(
            data_source=feature_view.input,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

        if feature_view.input.field_mapping is not None:
            table = _run_field_mapping(table, feature_view.input.field_mapping)

        join_keys = [entity.join_key for entity in entities]
        rows_to_write = _convert_arrow_to_proto(table, feature_view, join_keys)

        with tqdm_builder(len(rows_to_write)) as pbar:
            self.online_write_batch(
                project, feature_view, rows_to_write, lambda x: pbar.update(x)
            )

        feature_view.materialization_intervals.append((start_date, end_date))
        registry.apply_feature_view(feature_view, project)

    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
    ) -> RetrievalJob:
        job = self.offline_store.get_historical_features(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
        )
        return job


ProtoBatch = Sequence[
    Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
]


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

        key = client.key("Project", project, "Table", table.name, "Row", document_id,)

        entity = datastore.Entity(
            key=key, exclude_from_indexes=("created_ts", "event_ts", "values")
        )

        entity.update(
            dict(
                key=entity_key.SerializeToString(),
                values={k: v.SerializeToString() for k, v in features.items()},
                event_ts=utils.make_tzaware(timestamp),
                created_ts=(
                    utils.make_tzaware(created_ts) if created_ts is not None else None
                ),
            )
        )
        entities.append(entity)
    with client.transaction():
        client.put_multi(entities)

    if progress:
        progress(len(entities))


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
