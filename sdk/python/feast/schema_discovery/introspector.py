"""SQLAlchemy-backed introspection for navigating a database.

``DataSource.get_table_column_names_and_types()`` can only describe a table you
already know the name of, so schema/table *navigation* — and the nullability and
primary-key metadata that the cursor description does not carry — comes from
SQLAlchemy's ``Inspector``.
"""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List

from sqlalchemy import create_engine
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import DBAPIError, NoSuchTableError, SQLAlchemyError
from sqlalchemy.pool import NullPool

from feast.schema_discovery.dialects import normalize_engine_url
from feast.schema_discovery.errors import (
    DatabaseConnectionError,
    DatabaseConnectionTimeout,
    SchemaNotFoundError,
    TableNotFoundError,
)
from feast.schema_discovery.models import TableInfo

logger = logging.getLogger(__name__)

# Namespaces that belong to the database itself, not the user's data.
_SYSTEM_SCHEMAS = frozenset({"information_schema", "pg_catalog", "pg_toast"})


def _is_system_schema(name: str) -> bool:
    return name in _SYSTEM_SCHEMAS or name.startswith("pg_")


@contextmanager
def engine_for(url: URL, timeout: int) -> Iterator[Engine]:
    """Yield a short-lived, unpooled engine for one introspection call."""
    engine = create_engine(
        normalize_engine_url(url),
        poolclass=NullPool,
        connect_args={"connect_timeout": timeout},
    )
    try:
        yield engine
    finally:
        engine.dispose()


@contextmanager
def _translate_errors(timeout: int) -> Iterator[None]:
    """Map SQLAlchemy/DBAPI failures onto schema discovery errors."""
    try:
        yield
    except DBAPIError as e:
        if "timeout" in str(e).lower():
            raise DatabaseConnectionTimeout(timeout) from e
        raise DatabaseConnectionError(str(e.orig or e)) from e
    except SQLAlchemyError as e:
        raise DatabaseConnectionError(str(e)) from e


def list_tables(url: URL, schema: str, timeout: int) -> List[TableInfo]:
    """List tables and views within ``schema``.

    The existence check shares this connection rather than opening its own.

    Raises:
        SchemaNotFoundError: ``schema`` does not exist in the database.
    """
    with _translate_errors(timeout), engine_for(url, timeout) as engine:
        inspector = sa_inspect(engine)
        if schema not in inspector.get_schema_names():
            raise SchemaNotFoundError(schema)
        tables = inspector.get_table_names(schema=schema)
        views = inspector.get_view_names(schema=schema)

    return [TableInfo(name=n, type="table") for n in sorted(tables)] + [
        TableInfo(name=n, type="view") for n in sorted(views)
    ]


def list_schemas_with_tables(
    url: URL, timeout: int, include_tables: bool
) -> Dict[str, List[TableInfo]]:
    """List schemas and, when requested, the tables in each.

    Reuses a single engine across every schema rather than reconnecting per
    schema.
    """
    result: Dict[str, List[TableInfo]] = {}

    with _translate_errors(timeout), engine_for(url, timeout) as engine:
        inspector = sa_inspect(engine)
        schemas = sorted(
            n for n in inspector.get_schema_names() if not _is_system_schema(n)
        )

        for schema in schemas:
            if not include_tables:
                result[schema] = []
                continue
            tables = inspector.get_table_names(schema=schema)
            views = inspector.get_view_names(schema=schema)
            result[schema] = [
                TableInfo(name=n, type="table") for n in sorted(tables)
            ] + [TableInfo(name=n, type="view") for n in sorted(views)]

    return result


def get_column_metadata(
    url: URL, schema: str, table: str, timeout: int
) -> Dict[str, Dict[str, Any]]:
    """Return ``{column_name: {"nullable": bool, "primary_key": bool}}``.

    Raises:
        TableNotFoundError: the table does not exist in ``schema``.
    """
    with _translate_errors(timeout), engine_for(url, timeout) as engine:
        inspector = sa_inspect(engine)
        try:
            columns = inspector.get_columns(table, schema=schema)
        except NoSuchTableError as e:
            raise TableNotFoundError(f"{schema}.{table}") from e

        if not columns:
            raise TableNotFoundError(f"{schema}.{table}")

        try:
            pk_columns = set(
                inspector.get_pk_constraint(table, schema=schema).get(
                    "constrained_columns"
                )
                or []
            )
        except SQLAlchemyError:
            # Views and some permission setups expose no PK constraint.
            logger.debug("No primary key constraint readable for %s.%s", schema, table)
            pk_columns = set()

    return {
        c["name"]: {
            "nullable": bool(c.get("nullable", True)),
            "primary_key": c["name"] in pk_columns,
        }
        for c in columns
    }
