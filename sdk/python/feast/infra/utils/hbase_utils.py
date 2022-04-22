from typing import List

from happybase import Connection

from feast.infra.key_encoding_utils import serialize_entity_key
from feast.protos.feast.types.EntityKey_pb2 import EntityKey


class HbaseConstants:
    """Constants to be used by the Hbase Online Store."""

    DEFAULT_COLUMN_FAMILY = "default"
    EVENT_TS = "event_ts"
    CREATED_TS = "created_ts"
    DEFAULT_EVENT_TS = DEFAULT_COLUMN_FAMILY + ":" + EVENT_TS
    DEFAULT_CREATED_TS = DEFAULT_COLUMN_FAMILY + ":" + CREATED_TS

    @staticmethod
    def get_feature_from_col(col):
        """Given the column name, exclude the column family to get the feature name."""
        return col.decode("utf-8").split(":")[1]

    @staticmethod
    def get_col_from_feature(feature):
        """Given the feature name, add the column family to get the column name."""
        if isinstance(feature, bytes):
            feature = feature.decode("utf-8")
        return HbaseConstants.DEFAULT_COLUMN_FAMILY + ":" + feature


class HbaseUtils:
    """
    Utils class to manage different Hbase operations.

    Attributes:
        conn: happybase Connection to connect to hbase.
        host: hostname of the hbase thrift server.
        port: port in which thrift server is running.
        timeout: socket timeout in milliseconds.
    """

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
        """
        Create table in hbase online store.

        Arguments:
            table_name: Name of the Hbase table.
            colm_family: List of names of column families to be created in the hbase table.
        """
        cf_dict: dict = {}
        for cf in colm_family:
            cf_dict[cf] = dict()
        return self.conn.create_table(table_name, cf_dict)

    def create_table_with_default_cf(self, table_name: str):
        """
        Create table in hbase online store with one column family "default".

        Arguments:
            table_name: Name of the Hbase table.
        """
        return self.conn.create_table(table_name, {"default": dict()})

    def check_if_table_exist(self, table_name: str):
        """
        Check if table exists in hbase.

        Arguments:
            table_name: Name of the Hbase table.
        """
        return bytes(table_name, "utf-8") in self.conn.tables()

    def batch(self, table_name: str):
        """
        Returns a 'Batch' instance that can be used for mass data manipulation in the hbase table.

        Arguments:
            table_name: Name of the Hbase table.
        """
        return self.conn.table(table_name).batch()

    def put(self, table_name: str, row_key: str, data: dict):
        """
        Store data in the hbase table.

        Arguments:
            table_name: Name of the Hbase table.
            row_key: Row key of the row to be inserted to hbase table.
            data: Mapping of column family name:column name to column values
        """
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
        """
        Fetch a row of data from the hbase table.

        Arguments:
            table_name: Name of the Hbase table.
            row_key: Row key of the row to be inserted to hbase table.
            columns: the name of columns that needs to be fetched.
            timestamp: timestamp specifies the maximum version the cells can have.
            include_timestamp: specifies if (column, timestamp) to be return instead of only column.
        """
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
        """
        Fetch multiple rows of data from the hbase table.

        Arguments:
            table_name: Name of the Hbase table.
            row_keys: List of row key of the row to be inserted to hbase table.
            columns: the name of columns that needs to be fetched.
            timestamp: timestamp specifies the maximum version the cells can have.
            include_timestamp: specifies if (column, timestamp) to be return instead of only column.
        """
        table = self.conn.table(table_name)
        return table.rows(row_keys, columns, timestamp, include_timestamp)

    def print_table(self, table_name):
        """Prints the table scanning all the rows of the hbase table."""
        table = self.conn.table(table_name)
        scan_data = table.scan()
        for row_key, cols in scan_data:
            print(row_key.decode("utf-8"), cols)

    def delete_table(self, table: str):
        """Deletes the hbase table given the table name."""
        if self.check_if_table_exist(table):
            self.conn.delete_table(table, disable=True)

    def close_conn(self):
        """Closes the happybase connection."""
        self.conn.close()


def main():
    from feast.protos.feast.types.Value_pb2 import Value

    connection = Connection(host="localhost", port=9090)
    table = connection.table("test_hbase_driver_hourly_stats")
    row_keys = [
        serialize_entity_key(
            EntityKey(join_keys=["driver_id"], entity_values=[Value(int64_val=1004)])
        ).hex(),
        serialize_entity_key(
            EntityKey(join_keys=["driver_id"], entity_values=[Value(int64_val=1005)])
        ).hex(),
        serialize_entity_key(
            EntityKey(join_keys=["driver_id"], entity_values=[Value(int64_val=1024)])
        ).hex(),
    ]
    rows = table.rows(row_keys)

    for row_key, row in rows:
        for key, value in row.items():
            col_name = bytes.decode(key, "utf-8").split(":")[1]
            print(col_name, value)
        print()


if __name__ == "__main__":
    main()
