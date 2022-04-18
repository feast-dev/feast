# Created by aurobindo.m on 18/04/22
import struct
import calendar
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.repo_config import RepoConfig
from happybase import Connection

from feast.infra.online_stores.contrib.hbase_online_store.hbase_utils import HbaseUtils, HbaseConstants


class HbaseOnlineStoreConfig(FeastConfigBaseModel):
    type: str
    host: str
    port: int


class HbaseConnection:
    def __init__(self, store_config: HbaseOnlineStoreConfig):
        self._store_config = store_config
        self._real_conn = Connection(host=store_config.host, port=store_config.port)

    @property
    def real_conn(self) -> Connection:
        return self._real_conn

    def close(self) -> None:
        self.real_conn.close()


class HbaseOnlineStore(OnlineStore):
    _conn: Connection = None

    def _get_conn(self, config: RepoConfig):

        store_config = config.online_store
        assert isinstance(store_config, HbaseOnlineStoreConfig)

        if not self._conn:
            self._conn = Connection(host=store_config.host, port=store_config.port)
        return self._conn

    def online_write_batch(self, config: RepoConfig, table: FeatureView, data: List[
        Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]], progress: Optional[Callable[[int], Any]], ) -> None:

        hbase = HbaseUtils(self._get_conn(config))
        project = config.project
        table_name = _table_id(project, table)

        b = hbase.batch(table_name)
        for entity_key, values, timestamp, created_ts in data:
            row_key = serialize_entity_key(entity_key).hex()
            values_dict = {}
            for feature_name, val in values.items():
                values_dict[HbaseConstants.get_col_from_feature(feature_name)] = val.SerializeToString()
            if isinstance(timestamp, datetime):
                timestamp = int(calendar.timegm(timestamp.timetuple()))
                timestamp = struct.pack('>L', timestamp)
            values_dict[HbaseConstants.DEFAULT_EVENT_TS] = timestamp
            if created_ts is not None:
                if isinstance(created_ts, datetime):
                    created_ts = int(calendar.timegm(created_ts.timetuple()))
                    created_ts = struct.pack('>L', created_ts)
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
        hbase = HbaseUtils(self._get_conn(config))
        project = config.project
        table_name = _table_id(project, table)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        for entity_key in entity_keys:
            row_key = serialize_entity_key(entity_key).hex()
            val = hbase.row(table_name, row_key=row_key)
            res = {}
            res_ts = None
            for feature_name, feature_value in val.items():
                f_name = HbaseConstants.get_feature_from_col(feature_name)
                if f_name in requested_features:
                    v = ValueProto()
                    v.ParseFromString(feature_value)
                    res[f_name] = v
                if f_name is HbaseConstants.EVENT_TS:
                    ts = struct.unpack('>L', feature_value)[0]
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
        hbase = HbaseUtils(self._get_conn(config))
        project = config.project

        for table in tables:
            table_name = _table_id(project, table)
            hbase.delete_table(table_name)


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"
