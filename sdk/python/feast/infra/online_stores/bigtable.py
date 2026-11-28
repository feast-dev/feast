import hashlib
import logging
import re
from concurrent import futures
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Set, Tuple

import google
from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from pydantic import NonNegativeInt, PositiveInt, StrictStr

from feast import Entity, FeatureView, utils
from feast.feature_view import DUMMY_ENTITY_NAME
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig

logger = logging.getLogger(__name__)

# Default number of mutations per Bigtable write operation we're aiming for. The official
# max is 100K; we're being conservative. Overridable via
# ``BigtableOnlineStoreConfig.mutations_per_write``.
MUTATIONS_PER_OP = 50_000
# Default thread-pool size used to parallelize writes within a worker. The Bigtable client
# library limits the connection pool size to 10, which bounds the useful concurrency.
# Overridable via ``BigtableOnlineStoreConfig.write_concurrency``.
BIGTABLE_CLIENT_CONNECTION_POOL_SIZE = 10
# Default for ``BigtableOnlineStoreConfig.max_batched_result_rows``: the largest
# request, measured as entity rows x feature views, whose feature views are read with
# one ReadRows call per Bigtable table. Past it each view is read on its own, so that
# the rows of every view are not held at once.
MAX_BATCHED_RESULT_ROWS = 2048
# Bigtable rejects a row filter larger than 20 KB
# (https://cloud.google.com/bigtable/quotas#limits-filters). One batched read carries
# the row key pattern and the requested features of every view of a table in a single
# filter, so a request whose filter would pass this size reads one feature view at a
# time instead.
MAX_BATCHED_FILTER_BYTES = 16_000
# Upper bound on what one feature view adds to the serialized filter of a batched read
# on top of its two regexes: the tags and lengths of its chain and of both conditions.
BATCHED_FILTER_VIEW_OVERHEAD_BYTES = 32


class BigtableOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for GCP Bigtable"""

    type: Literal["bigtable"] = "bigtable"
    """Online store typee selector"""

    project_id: Optional[StrictStr] = None
    """(optional) GCP Project ID"""

    instance: StrictStr
    """The Bigtable instance's ID"""

    max_versions: int = 2
    """The number of historical versions of data that will be kept around."""

    mutations_per_write: PositiveInt = MUTATIONS_PER_OP
    """(optional) Target number of cell mutations per Bigtable write request. Bigtable's
    hard limit is 100K; lower values produce smaller requests, which reduces the write
    burst a materialization places on the instance. Defaults to 50,000."""

    write_concurrency: PositiveInt = BIGTABLE_CLIENT_CONNECTION_POOL_SIZE
    """(optional) Maximum number of concurrent write requests (thread-pool size) issued
    per worker during ``online_write_batch``. Lower values reduce the write load placed
    on the Bigtable instance at the cost of longer materialization. Defaults to 10."""

    max_batched_result_rows: NonNegativeInt = MAX_BATCHED_RESULT_ROWS
    """(optional) Largest request, as entity rows x feature views, whose feature views
    are read with one ReadRows call per Bigtable table. Set to 0 to always read one
    feature view at a time. Defaults to 2048."""


class BigtableOnlineStore(OnlineStore):
    _client: Optional[bigtable.Client] = None

    feature_column_family: str = "features"

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        # Requests spanning several feature views are batched per Bigtable table by
        # `_read_features_per_fv`; see https://github.com/feast-dev/feast/issues/3259
        feature_view = table
        bt_table_name = self._get_table_name(config=config, feature_view=feature_view)
        row_keys = [
            self._compute_row_key(
                entity_key=entity_key,
                feature_view_name=feature_view.name,
                config=config,
            )
            for entity_key in entity_keys
        ]
        bt_rows_dict = self._read_rows(
            config, bt_table_name, row_keys, self._column_filter(requested_features)
        )
        return [self._process_bt_row(bt_rows_dict.get(row_key)) for row_key in row_keys]

    def _read_features_per_fv(
        self,
        config: RepoConfig,
        grouped_refs: List,
        join_key_values: Dict,
        entity_name_to_join_key_map: Dict,
        online_features_response,
        full_feature_names: bool,
        include_feature_view_version_metadata: bool,
    ) -> None:
        """Read every requested feature view with one ReadRows call per Bigtable table.

        Feature views keyed on the same entities share a Bigtable table, and each
        view's rows are keyed ``{entity_id}#{feature_view_name}``, so the rows of all
        those views are fetched together instead of one ReadRows call per view. The
        filter of that call selects, for the rows of each view, the columns requested
        from that view, so it returns the same cells as the per-view reads.
        """

        def read_one_view_at_a_time() -> None:
            super(BigtableOnlineStore, self)._read_features_per_fv(
                config,
                grouped_refs,
                join_key_values,
                entity_name_to_join_key_map,
                online_features_response,
                full_feature_names,
                include_feature_view_version_metadata,
            )

        if not self._should_batch(config, grouped_refs, join_key_values):
            return read_one_view_at_a_time()

        reads = []
        keys_by_table: Dict[str, Dict[bytes, None]] = {}
        # Per Bigtable table and feature view: the features requested from the view,
        # where None stands for every column. One feature view requested under two
        # aliases reads the same rows, so its row keys are deduplicated and its
        # requested features merged.
        views_by_table: Dict[str, Dict[str, Optional[Set[str]]]] = {}
        for table, requested_features in grouped_refs:
            table_entity_values, idxs, output_len = utils._get_unique_entities(
                table, join_key_values, entity_name_to_join_key_map
            )
            row_keys = [
                self._compute_row_key(
                    entity_key=entity_key, feature_view_name=table.name, config=config
                )
                for entity_key in utils._get_entity_key_protos(table_entity_values)
            ]
            reads.append((table, requested_features, row_keys, idxs, output_len))

            bt_table_name = self._get_table_name(config=config, feature_view=table)
            keys_by_table.setdefault(bt_table_name, {}).update(dict.fromkeys(row_keys))
            views = views_by_table.setdefault(bt_table_name, {})
            requested = views.get(table.name, set())
            views[table.name] = (
                requested | set(requested_features)
                if requested is not None and requested_features
                else None
            )

        filters_by_table: Dict[str, Optional[row_filters.RowFilter]] = {}
        for bt_table_name, views in views_by_table.items():
            if not all(name.isascii() and name.isprintable() for name in views):
                # The row key patterns of the filter are only built for names that
                # are plain ASCII.
                return read_one_view_at_a_time()
            if self._batched_filter_size(views) > MAX_BATCHED_FILTER_BYTES:
                return read_one_view_at_a_time()
            filters_by_table[bt_table_name] = self._batched_filter(views)

        # A row key names its feature view, and a feature view lives in one table, so
        # the rows of every table can be indexed by row key alone. Each row is decoded
        # once, however many aliases of its view were requested.
        rows: Dict[
            bytes, Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]
        ] = {}
        for bt_table_name, table_row_keys in keys_by_table.items():
            bt_rows = self._read_rows(
                config,
                bt_table_name,
                list(table_row_keys),
                filters_by_table[bt_table_name],
            )
            for row_key, row in bt_rows.items():
                rows[row_key] = self._process_bt_row(row)

        for table, requested_features, row_keys, idxs, output_len in reads:
            utils._populate_response_from_feature_data(
                requested_features,
                [rows.get(row_key, (None, None)) for row_key in row_keys],
                idxs,
                online_features_response,
                full_feature_names,
                table,
                output_len,
                include_feature_view_version_metadata,
            )

    @staticmethod
    def _should_batch(
        config: RepoConfig, grouped_refs: List, join_key_values: Dict
    ) -> bool:
        """Whether to read this request's feature views together.

        A single view has nothing to share a ReadRows call with. The number of entity
        rows in the request bounds the unique keys any one view reads, so the size is
        estimated from the request shape. See :data:`MAX_BATCHED_RESULT_ROWS`.
        """
        if len(grouped_refs) < 2:
            return False
        entity_rows = max(
            (len(values) for values in join_key_values.values()), default=0
        )
        return (
            entity_rows * len(grouped_refs)
            <= config.online_store.max_batched_result_rows
        )

    @classmethod
    def _batched_filter(
        cls, views: Dict[str, Optional[Set[str]]]
    ) -> Optional[row_filters.RowFilter]:
        """The filter of one read of a table's feature views.

        ``views`` maps each feature view to the features requested from it, or to None
        when it is read without a feature list. A view's rows only pass the columns
        requested from that view: its condition is tied to its row keys, which end in
        the view's name. So a column stored under a feature name that was only
        requested from another view is not read, whether or not the view's current
        schema still lists it.
        """
        if len(views) == 1:
            return cls._column_filter(cls._sorted(next(iter(views.values()))))
        per_view: List[row_filters.RowFilter] = []
        for name, requested in views.items():
            row_key_filter = row_filters.RowKeyRegexFilter(cls._row_key_regex(name))
            column_filter = cls._column_filter(cls._sorted(requested))
            per_view.append(
                row_filters.RowFilterChain(filters=[row_key_filter, column_filter])
                if column_filter is not None
                else row_key_filter
            )
        return row_filters.RowFilterUnion(filters=per_view)

    @classmethod
    def _batched_filter_size(cls, views: Dict[str, Optional[Set[str]]]) -> int:
        """An upper bound on the serialized size of :meth:`_batched_filter`."""
        return sum(
            len(cls._row_key_regex(name))
            + (len(cls._column_filter_regex(sorted(requested))) if requested else 0)
            + BATCHED_FILTER_VIEW_OVERHEAD_BYTES
            for name, requested in views.items()
        )

    @staticmethod
    def _row_key_regex(feature_view_name: str) -> bytes:
        """Matches the row keys of one feature view; see :meth:`_compute_row_key`.

        The entity id is a hex digest, so the first ``#`` of a row key ends it and a
        row key matches the pattern of exactly one feature view.
        """
        return f"^[0-9a-f]+#{re.escape(feature_view_name)}$".encode()

    @staticmethod
    def _sorted(requested: Optional[Set[str]]) -> Optional[List[str]]:
        return sorted(requested) if requested is not None else None

    @classmethod
    def _column_filter(
        cls, requested_features: Optional[List[str]]
    ) -> Optional[row_filters.RowFilter]:
        if not requested_features:
            return None
        return row_filters.ColumnQualifierRegexFilter(
            cls._column_filter_regex(requested_features)
        )

    @staticmethod
    def _column_filter_regex(requested_features: List[str]) -> bytes:
        return f"^({'|'.join(requested_features)}|event_ts)$".encode()

    def _read_rows(
        self,
        config: RepoConfig,
        bt_table_name: str,
        row_keys: List[bytes],
        filter_: Optional[row_filters.RowFilter],
    ) -> Dict[bytes, bigtable.row.PartialRowData]:
        client = self._get_client(online_config=config.online_store)
        bt_instance = client.instance(instance_id=config.online_store.instance)
        bt_table = bt_instance.table(bt_table_name)

        row_set = bigtable.row_set.RowSet()
        for row_key in row_keys:
            row_set.add_row_key(row_key)
        rows = bt_table.read_rows(row_set=row_set, filter_=filter_)

        # The BigTable client library only returns rows for keys that are found. This
        # means that it's our responsibility to match the returned rows to the original
        # `row_keys`, so the rows are returned by key.
        return {row.row_key: row for row in rows}

    def _process_bt_row(
        self, row: Optional[bigtable.row.PartialRowData]
    ) -> Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]:
        res = {}

        if row is None:
            return (None, None)

        row_values = row.cells[self.feature_column_family]
        event_ts = datetime.fromisoformat(row_values.pop(b"event_ts")[0].value.decode())
        for feature_name, feature_values in row_values.items():
            # We only want to retrieve the latest value for each feature
            feature_value = feature_values[0]
            val = ValueProto()
            val.ParseFromString(feature_value.value)
            res[feature_name.decode()] = val

        return (event_ts, res)

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
        # mutate in one request.
        columns_per_row = len(feature_view.features) + 1  # extra for event timestamp
        # At least one row per request even when a feature view is very wide.
        rows_per_write = max(
            1, config.online_store.mutations_per_write // columns_per_row
        )

        with futures.ThreadPoolExecutor(
            max_workers=config.online_store.write_concurrency
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
                    f"Not all batches were written to Bigtable: {not_done_tasks}"
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
            rows.append(bt_row)
        # mutate_rows does not raise when rows fail. It returns one status per row,
        # and None for a row that never got a response (google-cloud-bigtable < 2.45).
        statuses = bt_table.mutate_rows(rows)
        failed = [status for status in statuses if status is None or status.code != 0]
        if failed:
            # Only counts and the status code: the status message can carry the backend
            # address, and the feature server returns exception text to the caller.
            first = "no status" if failed[0] is None else f"code={failed[0].code}"
            raise RuntimeError(
                f"Failed to write {len(failed)} of {len(rows)} rows to Bigtable table "
                f"{bt_table.table_id}; first failed status: {first}"
            )

        if progress:
            progress(len(rows))

    def _compute_row_key(
        self, entity_key: EntityKeyProto, feature_view_name: str, config: RepoConfig
    ) -> bytes:
        entity_id = compute_entity_id(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        )
        # Even though `entity_id` uniquely identifies an entity, we use the same table
        # for multiple feature_views with the same set of entities. To uniquely identify
        # the row for a feature_view, we suffix the name of the feature_view itself.
        # This also ensures that features for entities from various feature_views are
        # colocated.
        return f"{entity_id}#{feature_view_name}".encode()

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """Creates the appropriate tables and column family in Bigtable.

        We use a dedicated table for each entity combination. For example, if a
        FeatureView uses the entities `shop` and `customer`, the resulting table would
        be called `customer-shop` (entities are sorted lexicographically first).
        """
        online_config = config.online_store
        assert isinstance(online_config, BigtableOnlineStoreConfig)
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
        # feature views to Bigtable table names and figure out which ones to create
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
                logger.info(f"Creating table `{bt_table_name}` in Bigtable")
                bt_table.create()
            else:
                logger.info(f"Table {bt_table_name} already exists in Bigtable")

            if self.feature_column_family not in bt_table.list_column_families():
                bt_table.column_family(
                    self.feature_column_family, gc_rule=max_versions_gc_rule
                ).create()

        for bt_table_name in bt_tables_to_delete:
            bt_table = bt_instance.table(bt_table_name)
            logger.info(f"Deleting table {bt_table_name} in Bigtable")
            bt_table.delete()

    @staticmethod
    def _get_table_name(config: RepoConfig, feature_view: FeatureView) -> str:
        entities_part = (
            "-".join(sorted(feature_view.entities))
            if feature_view.entities
            else DUMMY_ENTITY_NAME
        )
        BIGTABLE_TABLE_MAX_LENGTH = 50
        ENTITIES_PART_MAX_LENGTH = 24
        # Bigtable limits table names to 50 characters. We'll limit the max size of of
        # the `entities_part` and if that's not enough, we'll just hash the
        # entities_part. The remaining length is dedicated to the project name. This
        # allows multiple projects to coexist in the same bigtable instance. This also
        # allows multiple integration test executions to run simultaneously without
        # conflicts.
        if len(entities_part) > ENTITIES_PART_MAX_LENGTH:
            entities_part = hashlib.md5(entities_part.encode()).hexdigest()[
                :ENTITIES_PART_MAX_LENGTH
            ]
        remaining_length = BIGTABLE_TABLE_MAX_LENGTH - len(entities_part) - 1
        if len(config.project) > remaining_length:
            HUMAN_READABLE_PART_LENGTH = 10
            HASH_PART_LENGTH = remaining_length - HUMAN_READABLE_PART_LENGTH - 1
            project_part = (
                config.project[:HUMAN_READABLE_PART_LENGTH]
                + "_"
                + hashlib.md5(config.project.encode()).hexdigest()[:HASH_PART_LENGTH]
            )
        else:
            project_part = config.project
        return f"{project_part}.{entities_part}"

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
        assert isinstance(online_config, BigtableOnlineStoreConfig)
        client = self._get_client(online_config, admin=True)
        bt_instance = client.instance(instance_id=online_config.instance)
        for table_name in bt_tables:
            try:
                logger.info(f"Deleting Bigtable table `{table_name}`")
                bt_instance.table(table_name).delete()
            except google.api_core.exceptions.NotFound:
                logger.warning(
                    f"Table `{table_name}` was not found. Skipping deletion."
                )

    def _get_client(
        self, online_config: BigtableOnlineStoreConfig, admin: bool = False
    ):
        if self._client is None:
            self._client = bigtable.Client(
                project=online_config.project_id, admin=admin
            )
        return self._client
