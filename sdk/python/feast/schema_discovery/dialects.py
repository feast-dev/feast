"""Dialect allowlist, connection-URL parsing, and identifier validation.

v1 supports PostgreSQL only. Adding a dialect means registering its backend
name here plus the ``DataSource``/offline-store config pair used to read
column types.
"""

import re
from typing import Optional, Tuple

from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError

from feast.repo_config import RepoConfig
from feast.schema_discovery.errors import (
    InvalidConnectionUrlError,
    InvalidIdentifierError,
    MissingDialectDependencyError,
    UnsupportedDialectError,
)

POSTGRES = "postgresql"

#: Backend names accepted by schema discovery.
SUPPORTED_DIALECTS = frozenset({POSTGRES})

#: Driver used when the URL names a backend without one (``postgresql://``).
_DEFAULT_DRIVERS = {POSTGRES: "psycopg"}

#: Packaging extra that provides each dialect's driver.
_DIALECT_EXTRAS = {POSTGRES: "postgres"}

# Unquoted SQL identifier. Deliberately strict: table names reach the database
# through string interpolation in DataSource.get_table_query_string().
# Anchored with \Z, not $: "$" also matches before a trailing newline, which
# would let "users\n" through.
_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")

# RepoConfig requires a registry, but schema discovery only ever reads
# config.offline_store. Nothing resolves this path.
_UNUSED_REGISTRY = "schema_discovery_unused_registry.db"


def parse_connection_url(connection_url: str) -> URL:
    """Parse and validate a connection URL, returning a SQLAlchemy ``URL``.

    Raises:
        InvalidConnectionUrlError: URL is malformed or missing host/database.
        UnsupportedDialectError: the backend is not in the v1 allowlist.
    """
    if not connection_url or not connection_url.strip():
        raise InvalidConnectionUrlError("connection_url must not be empty")

    try:
        url = make_url(connection_url.strip())
    except ArgumentError as e:
        raise InvalidConnectionUrlError(str(e)) from e

    backend = url.get_backend_name()
    if backend not in SUPPORTED_DIALECTS:
        raise UnsupportedDialectError(backend, SUPPORTED_DIALECTS)

    if not url.host:
        raise InvalidConnectionUrlError("no host specified")
    if not url.database:
        raise InvalidConnectionUrlError("no database specified")
    if not url.username:
        raise InvalidConnectionUrlError("no username specified")

    return url


def normalize_engine_url(url: URL) -> URL:
    """Pin a driver on a bare backend URL so the right DBAPI is loaded."""
    if "+" not in url.drivername:
        driver = _DEFAULT_DRIVERS[url.get_backend_name()]
        return url.set(drivername=f"{url.drivername}+{driver}")
    return url


def validate_identifier(identifier: str) -> str:
    """Validate a single unquoted SQL identifier."""
    if not identifier or not _IDENTIFIER_RE.fullmatch(identifier):
        raise InvalidIdentifierError(identifier)
    return identifier


def split_table_name(
    table: str, schema: Optional[str] = None
) -> Tuple[Optional[str], str]:
    """Split ``table`` into ``(schema, table)``, validating both parts.

    A schema qualifier on ``table`` wins over the ``schema`` argument.

    Raises:
        InvalidIdentifierError: either part is not a valid unquoted identifier,
            or the name has more than two parts.
    """
    if not table:
        raise InvalidIdentifierError(table)

    parts = table.split(".")
    if len(parts) > 2:
        raise InvalidIdentifierError(table)

    if len(parts) == 2:
        return validate_identifier(parts[0]), validate_identifier(parts[1])

    validated = validate_identifier(parts[0])
    return (validate_identifier(schema) if schema else None), validated


def build_repo_config(url: URL, db_schema: str) -> RepoConfig:
    """Build an in-memory ``RepoConfig`` from a connection URL.

    Produces the config that ``DataSource.get_table_column_names_and_types()``
    needs, without a ``feature_store.yaml`` or a registered data source.
    """
    backend = url.get_backend_name()
    if backend != POSTGRES:
        raise UnsupportedDialectError(backend, SUPPORTED_DIALECTS)

    try:
        from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
            PostgreSQLOfflineStoreConfig,
        )
    except ImportError as e:
        raise MissingDialectDependencyError(backend, _DIALECT_EXTRAS[backend]) from e

    # parse_connection_url() guarantees these, but build_repo_config() is also
    # reachable directly.
    if not url.host or not url.database or not url.username:
        raise InvalidConnectionUrlError("host, database and username are required")

    query = {k: v for k, v in url.query.items() if isinstance(v, str)}

    offline_store = PostgreSQLOfflineStoreConfig(
        host=url.host,
        port=url.port or 5432,
        database=url.database,
        db_schema=db_schema,
        user=url.username,
        password=url.password or "",
        sslmode=query.get("sslmode", "require"),
        sslkey_path=query.get("sslkey"),
        sslcert_path=query.get("sslcert"),
        sslrootcert_path=query.get("sslrootcert"),
    )

    return RepoConfig(
        project="schema_discovery",
        provider="local",
        registry=_UNUSED_REGISTRY,
        offline_store=offline_store,
        entity_key_serialization_version=3,
    )


def build_data_source(url: URL, schema: str, table: str):
    """Build an unregistered ``DataSource`` pointing at ``schema.table``.

    The source is never applied to a registry — it exists only so that
    ``get_table_column_names_and_types()`` and
    ``source_datatype_to_feast_value_type()`` can be reused for type discovery.
    """
    backend = url.get_backend_name()
    if backend != POSTGRES:
        raise UnsupportedDialectError(backend, SUPPORTED_DIALECTS)

    try:
        from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
            PostgreSQLSource,
        )
    except ImportError as e:
        raise MissingDialectDependencyError(backend, _DIALECT_EXTRAS[backend]) from e

    return PostgreSQLSource(
        name=f"schema_discovery_{schema}_{table}",
        table=f"{schema}.{table}",
    )


def default_schema_for(url: URL) -> str:
    """The schema searched when a table name carries no qualifier."""
    backend = url.get_backend_name()
    if backend != POSTGRES:
        raise UnsupportedDialectError(backend, SUPPORTED_DIALECTS)
    return "public"
