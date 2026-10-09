"""End-to-end schema discovery against a real PostgreSQL instance."""

import pytest
from testcontainers.postgres import PostgresContainer

from feast.schema_discovery import SchemaDiscoveryService
from feast.schema_discovery.errors import (
    DatabaseConnectionError,
    SchemaNotFoundError,
    TableNotFoundError,
)

DDL = """
CREATE TABLE public.users (
    user_id     BIGINT PRIMARY KEY,
    email       VARCHAR(255),
    age         INTEGER NOT NULL,
    balance     DOUBLE PRECISION,
    is_active   BOOLEAN NOT NULL,
    created_at  TIMESTAMP NOT NULL,
    metadata    JSONB
);

CREATE VIEW public.active_users AS
    SELECT user_id, email FROM public.users WHERE is_active;

CREATE SCHEMA analytics;
CREATE TABLE analytics.daily_metrics (
    metric_date DATE PRIMARY KEY,
    value       NUMERIC
);
"""


@pytest.fixture(scope="module")
def postgres_url():
    with PostgresContainer(
        "postgres:16",
        username="root",
        password="test!@#$%",  # pragma: allowlist secret
        dbname="test",
    ).with_exposed_ports(5432) as container:
        import psycopg

        host = container.get_container_host_ip()
        port = container.get_exposed_port(5432)
        url = (  # pragma: allowlist secret
            f"postgresql://root:test!%40%23%24%25@{host}:{port}/test?sslmode=disable"
        )

        with psycopg.connect(
            host=host,
            port=port,
            user="root",
            password="test!@#$%",  # pragma: allowlist secret
            dbname="test",
            sslmode="disable",
        ) as conn:
            conn.execute(DDL)
            conn.commit()

        yield url


@pytest.fixture
def service():
    return SchemaDiscoveryService(connect_timeout=10)


@pytest.mark.integration
class TestListSchemas:
    def test_lists_user_schemas_only(self, service, postgres_url):
        result = service.list_schemas(postgres_url, include_tables=False)
        names = {s.name for s in result.schemas}
        assert {"public", "analytics"} <= names
        assert not any(n.startswith("pg_") for n in names)
        assert "information_schema" not in names

    def test_reports_database_and_dialect(self, service, postgres_url):
        result = service.list_schemas(postgres_url, include_tables=False)
        assert result.database == "test"
        assert result.dialect == "postgresql"

    def test_includes_tables_and_views(self, service, postgres_url):
        result = service.list_schemas(postgres_url, include_tables=True)
        public = next(s for s in result.schemas if s.name == "public")
        by_name = {t.name: t.type for t in public.tables}
        assert by_name["users"] == "table"
        assert by_name["active_users"] == "view"

    def test_scopes_to_one_schema(self, service, postgres_url):
        result = service.list_schemas(postgres_url, schema="analytics")
        assert [s.name for s in result.schemas] == ["analytics"]
        assert [t.name for t in result.schemas[0].tables] == ["daily_metrics"]

    def test_unknown_schema_raises(self, service, postgres_url):
        with pytest.raises(SchemaNotFoundError):
            service.list_schemas(postgres_url, schema="does_not_exist")


@pytest.mark.integration
class TestListTables:
    def test_lists_tables_in_schema(self, service, postgres_url):
        tables = service.list_tables(postgres_url, "analytics")
        assert [t.name for t in tables] == ["daily_metrics"]

    def test_unknown_schema_raises(self, service, postgres_url):
        with pytest.raises(SchemaNotFoundError):
            service.list_tables(postgres_url, "does_not_exist")


@pytest.mark.integration
class TestDescribeTable:
    def test_maps_every_column_to_a_feast_type(self, service, postgres_url):
        result = service.describe_table(postgres_url, "public.users")
        by_name = {c.name: c for c in result.columns}

        assert by_name["user_id"].feast_type == "INT64"
        assert by_name["email"].feast_type == "STRING"
        assert by_name["age"].feast_type == "INT32"
        assert by_name["balance"].feast_type == "DOUBLE"
        assert by_name["is_active"].feast_type == "BOOL"
        assert by_name["created_at"].feast_type == "UNIX_TIMESTAMP"
        assert by_name["metadata"].feast_type == "MAP"

    def test_no_column_falls_back_to_unknown(self, service, postgres_url):
        """Regression guard: SQLAlchemy type spellings would map to UNKNOWN."""
        result = service.describe_table(postgres_url, "public.users")
        assert all(c.feast_type != "UNKNOWN" for c in result.columns)

    def test_reports_primary_key(self, service, postgres_url):
        result = service.describe_table(postgres_url, "public.users")
        pks = {c.name for c in result.columns if c.primary_key}
        assert pks == {"user_id"}

    def test_reports_nullability(self, service, postgres_url):
        by_name = {
            c.name: c
            for c in service.describe_table(postgres_url, "public.users").columns
        }
        assert by_name["email"].nullable is True
        assert by_name["age"].nullable is False
        assert by_name["user_id"].nullable is False

    def test_unqualified_table_uses_public(self, service, postgres_url):
        result = service.describe_table(postgres_url, "users")
        assert result.table == "public.users"

    def test_describes_table_in_non_default_schema(self, service, postgres_url):
        result = service.describe_table(postgres_url, "analytics.daily_metrics")
        assert {c.name for c in result.columns} == {"metric_date", "value"}

    def test_schema_argument_qualifies_table(self, service, postgres_url):
        result = service.describe_table(
            postgres_url, "daily_metrics", schema="analytics"
        )
        assert result.table == "analytics.daily_metrics"

    def test_describes_a_view(self, service, postgres_url):
        result = service.describe_table(postgres_url, "public.active_users")
        assert [c.name for c in result.columns] == ["user_id", "email"]
        assert all(c.primary_key is False for c in result.columns)

    def test_unknown_table_raises(self, service, postgres_url):
        with pytest.raises(TableNotFoundError):
            service.describe_table(postgres_url, "public.does_not_exist")

    def test_unknown_schema_raises(self, service, postgres_url):
        with pytest.raises(TableNotFoundError):
            service.describe_table(postgres_url, "nosuchschema.users")


@pytest.mark.integration
class TestConnectionFailures:
    def test_bad_credentials_raise_connection_error(self, service, postgres_url):
        broken = postgres_url.replace("root:", "wronguser:")
        with pytest.raises(DatabaseConnectionError):
            service.list_schemas(broken)
