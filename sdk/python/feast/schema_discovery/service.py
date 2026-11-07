"""Orchestration for SQL schema discovery.

Discovery combines two sources of truth:

* **SQLAlchemy** (``introspector``) navigates the database — which schemas and
  tables exist — and supplies nullability and primary-key metadata.
* **Feast's own ``DataSource``** supplies column types. This matters: Feast's
  type mappers key on canonical catalog spellings such as
  ``character varying``, which is what the DBAPI cursor reports, whereas
  SQLAlchemy reports ``VARCHAR(255)`` and would map to ``UNKNOWN``.
"""

import logging
from typing import List, Optional, Union

from feast.errors import ZeroColumnQueryResult
from feast.schema_discovery import introspector
from feast.schema_discovery.dialects import (
    build_data_source,
    build_repo_config,
    default_schema_for,
    parse_connection_url,
    split_table_name,
    validate_identifier,
)
from feast.schema_discovery.errors import (
    DatabaseConnectionError,
    DatabaseConnectionTimeout,
    SchemaDiscoveryError,
    TableNotFoundError,
)
from feast.schema_discovery.models import (
    ColumnSchema,
    DatabaseSchemas,
    SchemaDiscoveryRequest,
    SchemaInfo,
    TableInfo,
    TableSchema,
)

logger = logging.getLogger(__name__)

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10


class SchemaDiscoveryService:
    """Introspects a SQL database from an ad-hoc connection URL.

    Requires neither a ``feature_store.yaml`` nor a registered data source.
    """

    def __init__(self, connect_timeout: int = DEFAULT_CONNECT_TIMEOUT_SECONDS):
        self.connect_timeout = connect_timeout

    def discover(
        self, request: SchemaDiscoveryRequest
    ) -> Union[DatabaseSchemas, TableSchema]:
        """Dispatch on the request: a table describes columns, otherwise schemas."""
        if request.table:
            return self.describe_table(
                request.connection_url, request.table, request.schema_name
            )
        return self.list_schemas(
            request.connection_url,
            include_tables=request.include_tables,
            schema=request.schema_name,
        )

    def list_schemas(
        self,
        connection_url: str,
        include_tables: bool = True,
        schema: Optional[str] = None,
    ) -> DatabaseSchemas:
        """List schemas in the database, optionally with their tables.

        When ``schema`` is given the listing is scoped to that one schema.
        """
        url = parse_connection_url(connection_url)

        if schema:
            validate_identifier(schema)
            # Always listed, so a missing schema is a 404 even when the caller
            # does not want the tables themselves.
            found_tables = introspector.list_tables(url, schema, self.connect_timeout)
            schemas = [
                SchemaInfo(name=schema, tables=found_tables if include_tables else None)
            ]
        else:
            found = introspector.list_schemas_with_tables(
                url, self.connect_timeout, include_tables
            )
            schemas = [
                SchemaInfo(name=name, tables=tables if include_tables else None)
                for name, tables in found.items()
            ]

        return DatabaseSchemas(
            database=url.database or "",
            dialect=url.get_backend_name(),
            schemas=schemas,
        )

    def list_tables(self, connection_url: str, schema: str) -> List[TableInfo]:
        """List the tables and views in one schema."""
        url = parse_connection_url(connection_url)
        validate_identifier(schema)
        return introspector.list_tables(url, schema, self.connect_timeout)

    def describe_table(
        self, connection_url: str, table: str, schema: Optional[str] = None
    ) -> TableSchema:
        """Describe one table's columns, with Feast type mappings."""
        url = parse_connection_url(connection_url)
        parsed_schema, table_name = split_table_name(table, schema)
        resolved_schema = parsed_schema or default_schema_for(url)
        qualified = f"{resolved_schema}.{table_name}"

        # SQLAlchemy first: it validates existence and carries the nullability
        # and primary-key metadata the cursor description omits.
        metadata = introspector.get_column_metadata(
            url, resolved_schema, table_name, self.connect_timeout
        )

        source = build_data_source(url, resolved_schema, table_name)
        columns_and_types = self._read_column_types(
            source, url, resolved_schema, table_name
        )
        to_feast_type = type(source).source_datatype_to_feast_value_type()

        columns = []
        for name, sql_type in columns_and_types:
            column_meta = metadata.get(name, {})
            columns.append(
                ColumnSchema(
                    name=name,
                    sql_type=sql_type,
                    nullable=bool(column_meta.get("nullable", True)),
                    primary_key=bool(column_meta.get("primary_key", False)),
                    feast_type=to_feast_type(sql_type).name,
                )
            )

        return TableSchema(
            database=url.database or "",
            dialect=url.get_backend_name(),
            table=qualified,
            columns=columns,
        )

    def _read_column_types(self, source, url, schema: str, table: str):
        """Read ``(column_name, native_sql_type)`` pairs via Feast's DataSource."""
        qualified = f"{schema}.{table}"
        config = build_repo_config(url, db_schema=schema)

        try:
            return list(source.get_table_column_names_and_types(config))
        except ZeroColumnQueryResult as e:
            raise TableNotFoundError(qualified) from e
        except SchemaDiscoveryError:
            raise
        except Exception as e:
            if type(e).__name__ in ("UndefinedTable", "InvalidSchemaName"):
                raise TableNotFoundError(qualified) from e
            if "timeout" in str(e).lower():
                raise DatabaseConnectionTimeout(self.connect_timeout) from e
            raise DatabaseConnectionError(str(e)) from e
