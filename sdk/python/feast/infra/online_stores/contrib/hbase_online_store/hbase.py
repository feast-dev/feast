import calendar
import struct
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from happybase import Connection
from pydantic.typing import Literal

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.utils.hbase_utils import HbaseConstants, HbaseUtils
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig


class HbaseOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Hbase store"""

    type: Literal["hbase"] = "hbase"
    """Online store type selector"""

    host: str
    """Hostname of Hbase Thrift server"""

    port: str
    """Port in which Hbase Thrift server is running"""


class HbaseConnection:
    """
    Hbase connecttion to connect to hbase.

    Attributes:
        store_config: Online store config for Hbase store.
    """

    def __init__(self, store_config: HbaseOnlineStoreConfig):
        self._store_config = store_config
        self._real_conn = Connection(
            host=store_config.host, port=int(store_config.port)
        )

    @property
    def real_conn(self) -> Connection:
        """Stores the real happybase Connection to connect to hbase."""
        return self._real_conn

    def close(self) -> None:
        """Close the happybase connection."""
        self.real_conn.close()


class HbaseOnlineStore(OnlineStore):
    """
    Online feature store for Hbase.

    Attributes:
        _conn: Happybase Connection to connect to hbase thrift server.
    """

    _conn: Connection = None

    def _get_conn(self, config: RepoConfig):
        """
        Get or Create Hbase Connection from Repoconfig.

        Args:
            config: The RepoConfig for the current FeatureStore.
        """

        store_config = config.online_store
        assert isinstance(store_config, HbaseOnlineStoreConfig)

        if not self._conn:
            self._conn = Connection(host=store_config.host, port=int(store_config.port))
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

        hbase = HbaseUtils(self._get_conn(config))
        project = config.project
        table_name = _table_id(project, table)

        b = hbase.batch(table_name)
        for entity_key, values, timestamp, created_ts in data:
            row_key = serialize_entity_key(entity_key).hex()
            values_dict = {}
            for feature_name, val in values.items():
                values_dict[
                    HbaseConstants.get_col_from_feature(feature_name)
                ] = val.SerializeToString()
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
        """
        hbase = HbaseUtils(self._get_conn(config))
        project = config.project
        table_name = _table_id(project, table)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        row_keys = [
            serialize_entity_key(entity_key).hex() for entity_key in entity_keys
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
        hbase = HbaseUtils(self._get_conn(config))
        project = config.project

        # We don't create any special state for the entites in this implementation.
        for table in tables_to_keep:
            table_name = _table_id(project, table)
            if not hbase.check_if_table_exist(table_name):
                hbase.create_table_with_default_cf(table_name)

        for table in tables_to_delete:
            table_name = _table_id(project, table)
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
        hbase = HbaseUtils(self._get_conn(config))
        project = config.project

        for table in tables:
            table_name = _table_id(project, table)
            hbase.delete_table(table_name)


def _table_id(project: str, table: FeatureView) -> str:
    """
    Returns table name given the project_name and the feature_view.

    Args:
        project: Name of the feast project.
        table: Feast FeatureView.
    """
    return f"{project}_{table.name}"
