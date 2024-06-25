import calendar
import struct
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from happybase import ConnectionPool
from happybase.connection import DEFAULT_PROTOCOL, DEFAULT_TRANSPORT
from pydantic import StrictStr

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.utils.hbase_utils import HBaseConnector, HbaseConstants
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig


class HbaseOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Hbase store"""

    type: Literal["hbase"] = "hbase"
    """Online store type selector"""

    host: StrictStr
    """Hostname of Hbase Thrift server"""

    port: StrictStr
    """Port in which Hbase Thrift server is running"""

    connection_pool_size: int = 4
    """Number of connections to Hbase Thrift server to keep in the connection pool"""

    protocol: StrictStr = DEFAULT_PROTOCOL
    """Protocol used to communicate with Hbase Thrift server"""

    transport: StrictStr = DEFAULT_TRANSPORT
    """Transport used to communicate with Hbase Thrift server"""


class HbaseOnlineStore(OnlineStore):
    """
    Online feature store for Hbase.

    Attributes:
        _conn: Happybase Connection to connect to hbase thrift server.
    """

    _conn: ConnectionPool = None

    def _get_conn(self, config: RepoConfig):
        """
        Get or Create Hbase Connection from Repoconfig.

        Args:
            config: The RepoConfig for the current FeatureStore.
        """

        store_config = config.online_store
        assert isinstance(store_config, HbaseOnlineStoreConfig)

        if not self._conn:
            self._conn = ConnectionPool(
                host=store_config.host,
                port=int(store_config.port),
                size=int(store_config.connection_pool_size),
                protocol=store_config.protocol,
                transport=store_config.transport,
            )
        return self._conn

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
        Write a batch of feature rows to Hbase online store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView.
            data: a list of quadruplets containing Feature data. Each quadruplet contains an Entity Key,
            a dict containing feature values, an event timestamp for the row, and
            the created timestamp for the row if it exists.
            progress: Optional function to be called once every mini-batch of rows is written to
            the online store. Can be used to display progress.
        """

        hbase = HBaseConnector(self._get_conn(config))
        project = config.project
        table_name = self._table_id(project, table)

        b = hbase.batch(table_name)
        for entity_key, values, timestamp, created_ts in data:
            row_key = self._hbase_row_key(
                entity_key,
                feature_view_name=table.name,
                config=config,
            )
            values_dict = {}
            for feature_name, val in values.items():
                values_dict[HbaseConstants.get_col_from_feature(feature_name)] = (
                    val.SerializeToString()
                )
            if isinstance(timestamp, datetime):
                values_dict[HbaseConstants.DEFAULT_EVENT_TS] = struct.pack(
                    ">L", int(calendar.timegm(timestamp.timetuple()))
                )
            else:
                values_dict[HbaseConstants.DEFAULT_EVENT_TS] = timestamp
            if created_ts is not None:
                if isinstance(created_ts, datetime):
                    values_dict[HbaseConstants.DEFAULT_CREATED_TS] = struct.pack(
                        ">L", int(calendar.timegm(created_ts.timetuple()))
                    )
                else:
                    values_dict[HbaseConstants.DEFAULT_CREATED_TS] = created_ts
            b.put(row_key, values_dict)
        b.send()

        if progress:
            progress(len(data))

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Retrieve feature values from the Hbase online store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            table: Feast FeatureView.
            entity_keys: a list of entity keys that should be read from the FeatureStore.
            requested_features: a list of requested feature names.
        """
        hbase = HBaseConnector(self._get_conn(config))
        project = config.project
        table_name = self._table_id(project, table)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        row_keys = [
            self._hbase_row_key(
                entity_key,
                feature_view_name=table.name,
                config=config,
            )
            for entity_key in entity_keys
        ]
        rows = hbase.rows(table_name, row_keys=row_keys)

        for _, row in rows:
            res = {}
            res_ts = None
            for feature_name, feature_value in row.items():
                f_name = HbaseConstants.get_feature_from_col(feature_name)
                if requested_features is not None and f_name in requested_features:
                    v = ValueProto()
                    v.ParseFromString(feature_value)
                    res[f_name] = v
                if f_name is HbaseConstants.EVENT_TS:
                    ts = struct.unpack(">L", feature_value)[0]
                    res_ts = datetime.fromtimestamp(ts)
            if not res:
                result.append((None, None))
            else:
                result.append((res_ts, res))
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
        Update tables from the Hbase Online Store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            tables_to_delete: Tables to delete from the Hbase Online Store.
            tables_to_keep: Tables to keep in the Hbase Online Store.
        """
        hbase = HBaseConnector(self._get_conn(config))
        project = config.project

        # We don't create any special state for the entites in this implementation.
        for table in tables_to_keep:
            table_name = self._table_id(project, table)
            if not hbase.check_if_table_exist(table_name):
                hbase.create_table_with_default_cf(table_name)

        for table in tables_to_delete:
            table_name = self._table_id(project, table)
            hbase.delete_table(table_name)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Delete tables from the Hbase Online Store.

        Args:
            config: The RepoConfig for the current FeatureStore.
            tables: Tables to delete from the feature repo.
        """
        hbase = HBaseConnector(self._get_conn(config))
        project = config.project

        for table in tables:
            table_name = self._table_id(project, table)
            hbase.delete_table(table_name)

    def _hbase_row_key(
        self,
        entity_key: EntityKeyProto,
        feature_view_name: str,
        config: RepoConfig,
    ) -> bytes:
        """
        Computes the HBase row key for a given entity key and feature view name.

        Args:
            entity_key (EntityKeyProto): The entity key to compute the row key for.
            feature_view_name (str): The name of the feature view to compute the row key for.
            config (RepoConfig): The configuration for the Feast repository.

        Returns:
            bytes: The HBase row key for the given entity key and feature view name.
        """
        entity_id = compute_entity_id(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        )
        # Even though `entity_id` uniquely identifies an entity, we use the same table
        # for multiple feature_views with the same set of entities.
        # To uniquely identify the row for a feature_view, we suffix the name of the feature_view itself.
        # This also ensures that features for entities from various feature_views are
        # colocated.
        return f"{entity_id}#{feature_view_name}".encode()

    def _table_id(self, project: str, table: FeatureView) -> str:
        """
        Returns table name given the project_name and the feature_view.

        Args:
            project: Name of the feast project.
            table: Feast FeatureView.
        """
        return f"{project}:{table.name}"
