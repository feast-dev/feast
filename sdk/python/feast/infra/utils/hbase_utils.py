from typing import List, Optional

from happybase import ConnectionPool


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


class HBaseConnector:
    """
    Utils class to manage different Hbase operations.

    Attributes:
        conn: happybase Connection to connect to hbase.
        host: hostname of the hbase thrift server.
        port: port in which thrift server is running.
        timeout: socket timeout in milliseconds.
    """

    def __init__(
        self,
        pool: Optional[ConnectionPool] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        connection_pool_size: int = 4,
    ):
        if pool is None:
            self.host = host
            self.port = port
            self.pool = ConnectionPool(
                host=host,
                port=port,
                size=connection_pool_size,
            )
        else:
            self.pool = pool

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

        with self.pool.connection() as conn:
            return conn.create_table(table_name, cf_dict)

    def create_table_with_default_cf(self, table_name: str):
        """
        Create table in hbase online store with one column family "default".

        Arguments:
            table_name: Name of the Hbase table.
        """
        with self.pool.connection() as conn:
            return conn.create_table(table_name, {"default": dict()})

    def check_if_table_exist(self, table_name: str):
        """
        Check if table exists in hbase.

        Arguments:
            table_name: Name of the Hbase table.
        """
        with self.pool.connection() as conn:
            return bytes(table_name, "utf-8") in conn.tables()

    def batch(self, table_name: str):
        """
        Returns a "Batch" instance that can be used for mass data manipulation in the hbase table.

        Arguments:
            table_name: Name of the Hbase table.
        """
        with self.pool.connection() as conn:
            return conn.table(table_name).batch()

    def put(self, table_name: str, row_key: str, data: dict):
        """
        Store data in the hbase table.

        Arguments:
            table_name: Name of the Hbase table.
            row_key: Row key of the row to be inserted to hbase table.
            data: Mapping of column family name:column name to column values
        """
        with self.pool.connection() as conn:
            table = conn.table(table_name)
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
        with self.pool.connection() as conn:
            table = conn.table(table_name)
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
        with self.pool.connection() as conn:
            table = conn.table(table_name)
            return table.rows(row_keys, columns, timestamp, include_timestamp)

    def print_table(self, table_name):
        """Prints the table scanning all the rows of the hbase table."""
        with self.pool.connection() as conn:
            table = conn.table(table_name)
            scan_data = table.scan()
            for row_key, cols in scan_data:
                print(row_key.decode("utf-8"), cols)

    def delete_table(self, table: str):
        """Deletes the hbase table given the table name."""
        if self.check_if_table_exist(table):
            with self.pool.connection() as conn:
                conn.delete_table(table, disable=True)

    def close_conn(self):
        """Closes the happybase connection."""
        with self.pool.connection() as conn:
            conn.close()


def main():
    from feast.infra.key_encoding_utils import serialize_entity_key
    from feast.protos.feast.types.EntityKey_pb2 import EntityKey
    from feast.protos.feast.types.Value_pb2 import Value

    pool = ConnectionPool(
        host="localhost",
        port=9090,
        size=2,
    )
    with pool.connection() as connection:
        table = connection.table("test_hbase_driver_hourly_stats")
        row_keys = [
            serialize_entity_key(
                EntityKey(
                    join_keys=["driver_id"], entity_values=[Value(int64_val=1004)]
                ),
                entity_key_serialization_version=3,
            ).hex(),
            serialize_entity_key(
                EntityKey(
                    join_keys=["driver_id"], entity_values=[Value(int64_val=1005)]
                ),
                entity_key_serialization_version=3,
            ).hex(),
            serialize_entity_key(
                EntityKey(
                    join_keys=["driver_id"], entity_values=[Value(int64_val=1024)]
                ),
                entity_key_serialization_version=3,
            ).hex(),
        ]
        rows = table.rows(row_keys)

        for _, row in rows:
            for key, value in row.items():
                col_name = bytes.decode(key, "utf-8").split(":")[1]
                print(col_name, value)
            print()


if __name__ == "__main__":
    main()
