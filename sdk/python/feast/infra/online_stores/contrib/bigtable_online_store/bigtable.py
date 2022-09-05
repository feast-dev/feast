import logging
from concurrent import futures
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import google
from google.cloud import bigtable
from pydantic import StrictStr
from pydantic.typing import Literal

from feast import Entity, FeatureView
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.protos.feast.types.Value_pb2 import ValueType
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import get_user_agent, log_exceptions_and_usage, tracing_span

logger = logging.getLogger(__name__)

# Number of mutations per BigTable write operation we're aiming for. The official max is 100K; we're
# being conservative.
MUTATIONS_PER_OP = 50_000
# The Bigtable client library limits the connection pool size to 10. This imposes a limitation to
# the concurrency we can get using a thread pool in each worker.
BIGTABLE_CLIENT_CONNECTION_POOL_SIZE = 10


class BigTableOnlineStoreConfig(FeastConfigBaseModel):
    type: Literal["bigtable"] = "bigtable"

    project: StrictStr
    instance: StrictStr
    max_versions: int = 2


class BigTableOnlineStore(OnlineStore):
    _client: Optional[bigtable.Client] = None

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
        column_family_id = feature_view.name

        client = bigtable.Client(project=config.online_store.project)
        bt_instance = client.instance(instance_id=config.online_store.instance)
        bt_table = bt_instance.table(bt_table_name)
        row_keys = [compute_entity_id(entity_key) for entity_key in entity_keys]

        batch_result: List[
            Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]
        ] = []

        # TODO: read all the rows in a single call instead of reading them sequentially
        for row_key in row_keys:
            res = {}
            # TODO: use filters to reduce the amount of data transfered and skip unnecessary columns.
            row = bt_table.read_row(row_key)

            if row is None:
                continue

            for feature_name, feature_values in row.cells.get(
                column_family_id, {}
            ).items():
                # We only want to retrieve the latest value for each feature
                feature_value = feature_values[0]
                val = ValueProto()
                val.ParseFromString(feature_value.value)
                res[feature_name.decode()] = val

            batch_result.append((feature_value.timestamp, res))

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        # Pad in case not all entities in a batch have responses
        batch_size_nones = ((None, None),) * (len(row_keys) - len(batch_result))
        batch_result.extend(batch_size_nones)
        result.extend(batch_result)
        return result

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
        column_family_id = feature_view.name

        client = bigtable.Client(project=config.online_store.project)
        bt_instance = client.instance(instance_id=config.online_store.instance)
        bt_table = bt_instance.table(bt_table_name)

        # `columns_per_row` is used to calculate the number of rows we are allowed to mutate in one
        # request. Since `MUTATIONS_PER_OP` is set much lower than the max allowed value, the
        # calculation of `columns_per_row` doesn't need to be precise. Feature views can have 1 or 2
        # timestamp fields: event timestamp and created timestamp. We assume 2 conservatively.
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
                        column_family_id=column_family_id,
                    )
                )
            futures.wait(fs)

    @staticmethod
    def _write_rows_to_bt(rows_to_write, bt_table, column_family_id):
        rows = []
        for row in rows_to_write:
            entity_key, features, timestamp, created_ts = row
            bt_row = bt_table.direct_row(compute_entity_id(entity_key))

            for feature_name, feature_value in features.items():
                bt_row.set_cell(
                    column_family_id, feature_name, feature_value.SerializeToString()
                )
            # TODO: write timestamps during materialization as well
            rows.append(bt_row)
        bt_table.mutate_rows(rows)

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """Creates the appropriate tables and column families in BigTable.

        We use a dedicated table for each entity combination. For example, if a FeatureView uses the entities `shop` and
        `customer`, the resulting table would be called `customer-shop` (entities are sorted lexicographically first).

        FeatureViews are represented by column families in their respective tables.
        """
        online_config = config.online_store
        assert isinstance(online_config, BigTableOnlineStoreConfig)
        client = self._get_client(online_config, admin=True)
        bt_instance = client.instance(instance_id=online_config.instance)
        max_versions_gc_rule = bigtable.column_family.MaxVersionsGCRule(
            online_config.max_versions
        )

        for feature_view in tables_to_keep:
            table_name = self._get_table_name(config=config, feature_view=feature_view)
            table = bt_instance.table(table_name)
            if not table.exists():
                logger.info(
                    f"Creating table `{table_name}` in BigTable for feature view `{feature_view.name}`"
                )
                table.create()
            else:
                logger.info(f"Table {table_name} already exists in BigTable")

            cfs = table.list_column_families()
            if feature_view.name not in cfs:
                table.column_family(
                    feature_view.name, gc_rule=max_versions_gc_rule
                ).create()

        for feature_view in tables_to_delete:
            table_name = self._get_table_name(config=config, feature_view=feature_view)
            table = bt_instance.table(table_name)
            cfs = table.list_column_families()
            cf = cfs.pop(feature_view.name, None)
            if cf is not None:
                cf.delete()
            else:
                logger.warning(
                    f"Skipping deletion of column family `{feature_view.name}` in table `{table_name}` since it "
                    "doesn't exist. Perhaps it was deleted manually."
                )
            if not cfs:
                logger.info(
                    f"We've deleted all column families from the table `{table_name}`, so we're deleting it too."
                )
                table.delete()

    @staticmethod
    def _get_table_name(config: RepoConfig, feature_view: FeatureView) -> str:
        return f"{config.project}.{'-'.join(sorted(feature_view.entities))}"

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        # Because of historical reasons, Feast calls them tables. We use this alias for readability.
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
                pass

    def _get_client(
        self, online_config: BigTableOnlineStoreConfig, admin: bool = False
    ):
        if not self._client:
            self._client = bigtable.Client(project=online_config.project, admin=admin)
        return self._client
