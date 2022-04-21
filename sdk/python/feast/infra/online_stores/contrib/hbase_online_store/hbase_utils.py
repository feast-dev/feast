# Created by aurobindo.m on 18/04/22
from typing import List

from happybase import Connection


class HbaseConstants:
    DEFAULT_COLUMN_FAMILY = "default"
    EVENT_TS = "event_ts"
    CREATED_TS = "created_ts"
    TS_COLUMNS = [b"default:created_ts", b"default:event_ts"]

    DEFAULT_EVENT_TS = DEFAULT_COLUMN_FAMILY + ":" + EVENT_TS
    DEFAULT_CREATED_TS = DEFAULT_COLUMN_FAMILY + ":" + CREATED_TS

    @staticmethod
    def get_feature_from_col(col):
        return col.decode("utf-8").split(":")[1]

    @staticmethod
    def get_col_from_feature(feature):
        if isinstance(feature, bytes):
            feature = feature.decode("utf-8")
        return HbaseConstants.DEFAULT_COLUMN_FAMILY + ":" + feature


class HbaseUtils:
    def __init__(
        self, conn: Connection = None, host: str = None, port: int = None, timeout=None
    ):
        if conn is None:
            self.host = host
            self.port = port
            self.conn = Connection(host=host, port=port, timeout=timeout)
        else:
            self.conn = conn

    def create_table(self, table_name: str, colm_family: List[str]):
        cf_dict: dict = {}
        for cf in colm_family:
            cf_dict[cf] = dict()
        return self.conn.create_table(table_name, cf_dict)

    def create_table_with_default_cf(self, table_name: str):
        return self.conn.create_table(table_name, {"default": dict()})

    def check_if_table_exist(self, table: str):
        return bytes(table, "utf-8") in self.conn.tables()

    def batch(self, table):
        return self.conn.table(table).batch()

    def put(self, table_name: str, row_key, data):
        table = self.conn.table(table_name)
        table.put(row_key, data)

    def row(
        self,
        table_name: str,
        row_key,
        columns=None,
        timestamp=None,
        include_timestamp=False,
    ):
        table = self.conn.table(table_name)
        return table.row(row_key, columns, timestamp, include_timestamp)

    def rows(
        self,
        table_name: str,
        row_keys,
        columns=None,
        timestamp=None,
        include_timestamp=False,
    ):
        table = self.conn.table(table_name)
        return table.rows(row_keys, columns, timestamp, include_timestamp)

    def print_table(self, table_name):
        table = self.conn.table(table_name)
        scan_data = table.scan()
        for row_key, cols in scan_data:
            print(row_key.decode("utf-8"), cols)

    def delete_table(self, table: str):
        if self.check_if_table_exist(table):
            self.conn.delete_table(table, disable=True)

    def close_conn(self):
        self.conn.close()
