from typing import Optional

import psycopg2
import psycopg2.errors
from psycopg2 import sql

from feast.infra.registry.registry_store import RegistryStore
from feast.infra.utils.postgres.connection_utils import _get_conn
from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig


class PostgresRegistryConfig(RegistryConfig):
    scheme: Optional[str]
    host: str
    port: int
    database: str
    db_schema: str
    user: str
    password: Optional[str]
    sslmode: Optional[str]
    sslkey_path: Optional[str]
    sslcert_path: Optional[str]
    sslrootcert_path: Optional[str]


class PostgreSQLRegistryStore(RegistryStore):
    def __init__(self, config: PostgresRegistryConfig, registry_path: str):
        self.db_config = PostgreSQLConfig(
            scheme=getattr(config, "scheme", "postgresql"),
            host=config.host,
            port=getattr(config, "port", 5432),
            database=config.database,
            db_schema=getattr(config, "db_schema", "public"),
            user=config.user,
            password=getattr(config, "password", None),
            sslmode=getattr(config, "sslmode", None),
            sslkey_path=getattr(config, "sslkey_path", None),
            sslcert_path=getattr(config, "sslcert_path", None),
            sslrootcert_path=getattr(config, "sslrootcert_path", None),
        )
        self.table_name = config.path
        self.cache_ttl_seconds = config.cache_ttl_seconds

    def get_registry_proto(self) -> RegistryProto:
        registry_proto = RegistryProto()
        try:
            with _get_conn(self.db_config) as conn, conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT registry
                        FROM {}
                        WHERE version = (SELECT max(version) FROM {})
                        """
                    ).format(
                        sql.Identifier(self.table_name),
                        sql.Identifier(self.table_name),
                    )
                )
                row = cur.fetchone()
                if row:
                    registry_proto = registry_proto.FromString(row[0])
        except psycopg2.errors.UndefinedTable:
            pass
        return registry_proto

    def update_registry_proto(self, registry_proto: RegistryProto):
        """
        Overwrites the current registry proto with the proto passed in. This method
        writes to the registry path.

        Args:
            registry_proto: the new RegistryProto
        """
        schema_name = self.db_config.db_schema or self.db_config.user
        with _get_conn(self.db_config) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = %s
                """,
                (schema_name,),
            )
            schema_exists = cur.fetchone()
            if not schema_exists:
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(self.db_config.user),
                    ),
                )

            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        version BIGSERIAL PRIMARY KEY,
                        registry BYTEA NOT NULL
                    );
                    """
                ).format(sql.Identifier(self.table_name)),
            )
            # Do we want to keep track of the history or just keep the latest?
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (registry)
                    VALUES (%s);
                    """
                ).format(sql.Identifier(self.table_name)),
                [registry_proto.SerializeToString()],
            )

    def teardown(self):
        with _get_conn(self.db_config) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    DROP TABLE IF EXISTS {};
                    """
                ).format(sql.Identifier(self.table_name))
            )
