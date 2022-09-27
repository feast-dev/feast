import logging
from concurrent import futures
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

import google
from google.cloud import bigtable
from pydantic import StrictStr
from pydantic.typing import Literal

from feast import Entity, FeatureView, utils
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage

logger = logging.getLogger(__name__)

# Number of mutations per BigTable write operation we're aiming for. The official max is
# 100K; we're being conservative.
MUTATIONS_PER_OP = 50_000
# The Bigtable client library limits the connection pool size to 10. This imposes a
# limitation to the concurrency we can get using a thread pool in each worker.
BIGTABLE_CLIENT_CONNECTION_POOL_SIZE = 10


class BigTableOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for GCP BigTable"""

    type: Literal["bigtable"] = "bigtable"
    """Online store typee selector"""

    project_id: Optional[StrictStr] = None
    """(optional) GCP Project ID"""

    instance: StrictStr
    """The BigTable instance's ID"""

    max_versions: int = 2
    """The number of historical versions of data that will be kept around."""


class BigTableOnlineStore(OnlineStore):
    _client: Optional[bigtable.Client] = None

    feature_column_family: str = "features"

    @log_exceptions_and_usage(online_store="bigtable")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        feature_view = table
        bt_table_name = self._get_table_name(config=config, feature_view=feature_view)

        client = self._get_client(online_config=config.online_store)
        bt_instance = client.instance(instance_id=config.online_store.instance)
        bt_table = bt_instance.table(bt_table_name)
        row_keys = [
            self._compute_row_key(
                entity_key=entity_key,
                feature_view_name=feature_view.name,
                config=config,
            )
            for entity_key in entity_keys
        ]

        batch_result: List[
            Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]
        ] = []

        # TODO: read all the rows in a single call instead of reading them sequentially
        for row_key in row_keys:
            res = {}
            # TODO: use filters to reduce the amount of data transfered and skip
            # unnecessary columns.
            row = bt_table.read_row(row_key)

            if row is None:
                batch_result.append((None, None))
                continue

            row_values = row.cells[self.feature_column_family]
            # TODO: check if we need created_ts anywhere
            row_values.pop(b"created_ts")
            event_ts = datetime.fromisoformat(
                row_values.pop(b"event_ts")[0].value.decode()
            )
            for feature_name, feature_values in row_values.items():
                # We only want to retrieve the latest value for each feature
                feature_value = feature_values[0]
                val = ValueProto()
                val.ParseFromString(feature_value.value)
                res[feature_name.decode()] = val

            batch_result.append((event_ts, res))
        return batch_result

    @log_exceptions_and_usage(online_store="bigtable")
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        feature_view = table
        bt_table_name = self._get_table_name(config=config, feature_view=feature_view)

        client = self._get_client(online_config=config.online_store)
        bt_instance = client.instance(instance_id=config.online_store.instance)
        bt_table = bt_instance.table(bt_table_name)

        # `columns_per_row` is used to calculate the number of rows we are allowed to
        # mutate in one request. Since `MUTATIONS_PER_OP` is set much lower than the max
        # allowed value, the calculation of `columns_per_row` doesn't need to be
        # precise. Feature views can have 1 or 2 timestamp fields: event timestamp and
        # created timestamp. We assume 2 conservatively.
        columns_per_row = len(feature_view.features) + 2  # extra for 2 timestamps
        rows_per_write = MUTATIONS_PER_OP // columns_per_row

        with futures.ThreadPoolExecutor(
            max_workers=BIGTABLE_CLIENT_CONNECTION_POOL_SIZE
        ) as executor:
            fs = []
            while data:
                rows_to_write, data = data[:rows_per_write], data[rows_per_write:]
                fs.append(
                    executor.submit(
                        self._write_rows_to_bt,
                        rows_to_write=rows_to_write,
                        bt_table=bt_table,
                        feature_view_name=feature_view.name,
                        config=config,
                        progress=progress,
                    )
                )
            done_tasks, not_done_tasks = futures.wait(fs)
            for task in done_tasks:
                # If a task raised an exception, this will raise it here as well
                task.result()
            if not_done_tasks:
                raise RuntimeError(
                    f"Not all batches were written to BigTable: {not_done_tasks}"
                )

    def _write_rows_to_bt(
        self,
        rows_to_write: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        bt_table: bigtable.table.Table,
        feature_view_name: str,
        config: RepoConfig,
        progress: Optional[Callable[[int], Any]],
    ):
        rows = []
        for entity_key, features, timestamp, created_ts in rows_to_write:
            bt_row = bt_table.direct_row(
                self._compute_row_key(
                    entity_key=entity_key,
                    feature_view_name=feature_view_name,
                    config=config,
                )
            )

            for feature_name, feature_value in features.items():
                bt_row.set_cell(
                    self.feature_column_family,
                    feature_name.encode(),
                    feature_value.SerializeToString(),
                )
            bt_row.set_cell(
                self.feature_column_family,
                b"event_ts",
                utils.make_tzaware(timestamp).isoformat().encode(),
            )
            bt_row.set_cell(
                self.feature_column_family,
                b"created_ts",
                utils.make_tzaware(created_ts).isoformat().encode()
                if created_ts is not None
                else b"",
            )
            rows.append(bt_row)
        bt_table.mutate_rows(rows)

        if progress:
            progress(len(rows))

    def _compute_row_key(
        self, entity_key: EntityKeyProto, feature_view_name: str, config: RepoConfig
    ) -> str:
        entity_id = compute_entity_id(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        )
        # Even though `entity_id` uniquely identifies an entity, we use the same table
        # for multiple feature_views with the same set of entities. To uniquely identify
        # the row for a feature_view, we suffix the name of the feature_view itself.
        # This also ensures that features for entities from various feature_views are
        # colocated.
        return f"{entity_id}#{feature_view_name}"

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """Creates the appropriate tables and column family in BigTable.

        We use a dedicated table for each entity combination. For example, if a
        FeatureView uses the entities `shop` and `customer`, the resulting table would
        be called `customer-shop` (entities are sorted lexicographically first).
        """
        online_config = config.online_store
        assert isinstance(online_config, BigTableOnlineStoreConfig)
        client = self._get_client(online_config, admin=True)
        bt_instance = client.instance(instance_id=online_config.instance)
        max_versions_gc_rule = bigtable.column_family.MaxVersionsGCRule(
            online_config.max_versions
        )
        # The word "table" in the arguments refers to feature_views (this is for legacy
        # reasons). To reduce confusion with bigtable tables, we use alternate variable
        # names
        feature_views_to_keep = tables_to_keep
        feature_views_to_delete = tables_to_delete
        # Multiple feature views can share the same tables. So just because a feature
        # view has been deleted does not mean that we can just delete the table. We map
        # feature views to BigTable table names and figure out which ones to create
        # and/or delete.
        bt_tables_to_keep: Set[str] = {
            self._get_table_name(config=config, feature_view=feature_view)
            for feature_view in feature_views_to_keep
        }
        bt_tables_to_delete: Set[str] = {
            self._get_table_name(config=config, feature_view=feature_view)
            for feature_view in feature_views_to_delete
        } - bt_tables_to_keep  # we don't delete a table if it's in `bt_tables_to_keep`

        for bt_table_name in bt_tables_to_keep:
            bt_table = bt_instance.table(bt_table_name)
            if not bt_table.exists():
                logger.info(f"Creating table `{bt_table_name}` in BigTable")
                bt_table.create()
            else:
                logger.info(f"Table {bt_table_name} already exists in BigTable")

            if self.feature_column_family not in bt_table.list_column_families():
                bt_table.column_family(
                    self.feature_column_family, gc_rule=max_versions_gc_rule
                ).create()

        for bt_table_name in bt_tables_to_delete:
            bt_table = bt_instance.table(bt_table_name)
            logger.info(f"Deleting table {bt_table_name} in BigTable")
            bt_table.delete()

    @staticmethod
    def _get_table_name(config: RepoConfig, feature_view: FeatureView) -> str:
        return f"{config.project}.{'-'.join(sorted(feature_view.entities))}"

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        # Because of historical reasons, Feast calls them tables. We use this alias for
        # readability.
        feature_views = tables

        bt_tables = {
            self._get_table_name(config=config, feature_view=fv) for fv in feature_views
        }

        online_config = config.online_store
        assert isinstance(online_config, BigTableOnlineStoreConfig)
        client = self._get_client(online_config, admin=True)
        bt_instance = client.instance(instance_id=online_config.instance)
        for table_name in bt_tables:
            try:
                logger.info(f"Deleting BigTable table `{table_name}`")
                bt_instance.table(table_name).delete()
            except google.api_core.exceptions.NotFound:
                logger.warning(
                    f"Table `{table_name}` was not found. Skipping deletion."
                )

    def _get_client(
        self, online_config: BigTableOnlineStoreConfig, admin: bool = False
    ):
        return bigtable.Client(project=online_config.project_id, admin=admin)
