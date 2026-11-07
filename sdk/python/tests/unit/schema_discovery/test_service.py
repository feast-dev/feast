import pytest

from feast.schema_discovery import introspector
from feast.schema_discovery.errors import (
    DatabaseConnectionError,
    DatabaseConnectionTimeout,
    InvalidConnectionUrlError,
    InvalidIdentifierError,
    SchemaDiscoveryError,
    SchemaNotFoundError,
    TableNotFoundError,
    UnsupportedDialectError,
)
from feast.schema_discovery.models import SchemaDiscoveryRequest, TableInfo
from feast.schema_discovery.service import SchemaDiscoveryService

VALID_URL = (
    "postgresql://alice:s3cret@db.internal:5432/mydb"  # pragma: allowlist secret
)


def _raise_schema_not_found(*args, **kwargs):
    raise SchemaNotFoundError("nope")


@pytest.fixture
def service():
    return SchemaDiscoveryService(connect_timeout=3)


@pytest.fixture
def no_network(monkeypatch):
    """Make every introspection call explode, proving it was never reached."""

    def boom(*args, **kwargs):
        raise AssertionError("attempted to connect to the database")

    for name in (
        "list_tables",
        "list_schemas_with_tables",
        "get_column_metadata",
    ):
        monkeypatch.setattr(introspector, name, boom)


class TestValidationHappensBeforeConnecting:
    """Bad input must be rejected without opening a connection."""

    def test_invalid_url_rejected(self, service, no_network):
        with pytest.raises(InvalidConnectionUrlError):
            service.list_schemas("not a url")

    def test_unsupported_dialect_rejected(self, service, no_network):
        with pytest.raises(UnsupportedDialectError):
            service.list_schemas("mysql://u:p@h:3306/db")

    def test_injection_in_table_rejected(self, service, no_network):
        with pytest.raises(InvalidIdentifierError):
            service.describe_table(VALID_URL, "users; DROP TABLE accounts")

    def test_injection_in_schema_rejected(self, service, no_network):
        with pytest.raises(InvalidIdentifierError):
            service.list_tables(VALID_URL, "public; DROP SCHEMA x")

    def test_three_part_table_rejected(self, service, no_network):
        with pytest.raises(InvalidIdentifierError):
            service.describe_table(VALID_URL, "db.public.users")

    def test_unsupported_dialect_rejected_on_describe(self, service, no_network):
        with pytest.raises(UnsupportedDialectError):
            service.describe_table("bigquery://project/dataset", "users")


class TestDescribeTableMerge:
    """Column types come from Feast's DataSource; nullability/PK from SQLAlchemy."""

    @pytest.fixture
    def stubbed(self, service, monkeypatch):
        monkeypatch.setattr(
            introspector,
            "get_column_metadata",
            lambda *a, **k: {
                "user_id": {"nullable": False, "primary_key": True},
                "email": {"nullable": True, "primary_key": False},
                "created_at": {"nullable": False, "primary_key": False},
            },
        )
        monkeypatch.setattr(
            SchemaDiscoveryService,
            "_read_column_types",
            lambda self, source, url, schema, table: [
                ("user_id", "bigint"),
                ("email", "character varying"),
                ("created_at", "timestamp without time zone"),
            ],
        )
        return service

    def test_maps_postgres_types_to_feast_types(self, stubbed):
        result = stubbed.describe_table(VALID_URL, "public.users")
        assert [(c.name, c.feast_type) for c in result.columns] == [
            ("user_id", "INT64"),
            ("email", "STRING"),
            ("created_at", "UNIX_TIMESTAMP"),
        ]

    def test_merges_nullability_and_primary_key(self, stubbed):
        by_name = {
            c.name: c for c in stubbed.describe_table(VALID_URL, "users").columns
        }
        assert by_name["user_id"].primary_key is True
        assert by_name["user_id"].nullable is False
        assert by_name["email"].primary_key is False
        assert by_name["email"].nullable is True

    def test_reports_native_sql_type(self, stubbed):
        by_name = {
            c.name: c for c in stubbed.describe_table(VALID_URL, "users").columns
        }
        assert by_name["email"].sql_type == "character varying"

    def test_preserves_column_order(self, stubbed):
        result = stubbed.describe_table(VALID_URL, "users")
        assert [c.name for c in result.columns] == ["user_id", "email", "created_at"]

    def test_qualifies_unqualified_table_with_default_schema(self, stubbed):
        assert stubbed.describe_table(VALID_URL, "users").table == "public.users"

    def test_honours_explicit_schema(self, stubbed):
        result = stubbed.describe_table(VALID_URL, "users", schema="analytics")
        assert result.table == "analytics.users"

    def test_reports_database_and_dialect(self, stubbed):
        result = stubbed.describe_table(VALID_URL, "users")
        assert result.database == "mydb"
        assert result.dialect == "postgresql"

    def test_unmapped_type_becomes_unknown(self, service, monkeypatch):
        monkeypatch.setattr(
            introspector,
            "get_column_metadata",
            lambda *a, **k: {"weird": {"nullable": True, "primary_key": False}},
        )
        monkeypatch.setattr(
            SchemaDiscoveryService,
            "_read_column_types",
            lambda self, source, url, schema, table: [("weird", "some_custom_type")],
        )
        column = service.describe_table(VALID_URL, "users").columns[0]
        assert column.feast_type == "UNKNOWN"
        assert column.sql_type == "some_custom_type"

    def test_column_missing_from_metadata_defaults_safely(self, service, monkeypatch):
        monkeypatch.setattr(introspector, "get_column_metadata", lambda *a, **k: {})
        monkeypatch.setattr(
            SchemaDiscoveryService,
            "_read_column_types",
            lambda self, source, url, schema, table: [("orphan", "integer")],
        )
        column = service.describe_table(VALID_URL, "users").columns[0]
        assert column.nullable is True
        assert column.primary_key is False


class TestListSchemas:
    def test_lists_schemas_with_tables(self, service, monkeypatch):
        monkeypatch.setattr(
            introspector,
            "list_schemas_with_tables",
            lambda url, timeout, include_tables: {
                "public": [
                    TableInfo(name="users", type="table"),
                    TableInfo(name="active_users", type="view"),
                ],
                "analytics": [TableInfo(name="daily_metrics", type="table")],
            },
        )
        result = service.list_schemas(VALID_URL)
        assert result.database == "mydb"
        assert result.dialect == "postgresql"
        assert [s.name for s in result.schemas] == ["public", "analytics"]
        assert [t.type for t in result.schemas[0].tables] == ["table", "view"]

    def test_omits_tables_when_not_requested(self, service, monkeypatch):
        monkeypatch.setattr(
            introspector,
            "list_schemas_with_tables",
            lambda url, timeout, include_tables: {"public": []},
        )
        result = service.list_schemas(VALID_URL, include_tables=False)
        assert result.schemas[0].tables is None

    def test_scopes_to_single_schema(self, service, monkeypatch):
        monkeypatch.setattr(
            introspector,
            "list_tables",
            lambda *a, **k: [TableInfo(name="daily_metrics", type="table")],
        )
        result = service.list_schemas(VALID_URL, schema="analytics")
        assert [s.name for s in result.schemas] == ["analytics"]

    def test_missing_schema_raises_not_found(self, service, monkeypatch):
        monkeypatch.setattr(introspector, "list_tables", _raise_schema_not_found)
        with pytest.raises(SchemaNotFoundError):
            service.list_schemas(VALID_URL, schema="nope")

    def test_missing_schema_raises_even_without_tables(self, service, monkeypatch):
        """The 404 must survive include_tables=False."""
        monkeypatch.setattr(introspector, "list_tables", _raise_schema_not_found)
        with pytest.raises(SchemaNotFoundError):
            service.list_schemas(VALID_URL, schema="nope", include_tables=False)

    def test_list_tables_missing_schema_raises_not_found(self, service, monkeypatch):
        monkeypatch.setattr(introspector, "list_tables", _raise_schema_not_found)
        with pytest.raises(SchemaNotFoundError):
            service.list_tables(VALID_URL, "nope")


class TestDiscoverDispatch:
    def test_table_present_describes_columns(self, service, monkeypatch):
        monkeypatch.setattr(
            introspector, "get_column_metadata", lambda *a, **k: {"id": {}}
        )
        monkeypatch.setattr(
            SchemaDiscoveryService,
            "_read_column_types",
            lambda self, source, url, schema, table: [("id", "integer")],
        )
        result = service.discover(
            SchemaDiscoveryRequest(connection_url=VALID_URL, table="public.users")
        )
        assert result.table == "public.users"

    def test_table_absent_lists_schemas(self, service, monkeypatch):
        monkeypatch.setattr(
            introspector,
            "list_schemas_with_tables",
            lambda url, timeout, include_tables: {"public": []},
        )
        result = service.discover(SchemaDiscoveryRequest(connection_url=VALID_URL))
        assert [s.name for s in result.schemas] == ["public"]


class TestRequestModel:
    def test_schema_alias_is_accepted(self):
        request = SchemaDiscoveryRequest(
            connection_url=VALID_URL, table="users", schema="analytics"
        )
        assert request.schema_name == "analytics"

    def test_field_name_also_accepted(self):
        request = SchemaDiscoveryRequest(
            connection_url=VALID_URL, schema_name="analytics"
        )
        assert request.schema_name == "analytics"

    def test_include_tables_defaults_true(self):
        assert SchemaDiscoveryRequest(connection_url=VALID_URL).include_tables is True


class TestErrorHttpStatusCodes:
    @pytest.mark.parametrize(
        "error,expected",
        [
            (InvalidConnectionUrlError("bad"), 400),
            (UnsupportedDialectError("mysql", ["postgresql"]), 400),
            (InvalidIdentifierError("x;"), 400),
            (SchemaNotFoundError("nope"), 404),
            (TableNotFoundError("public.nope"), 404),
            (DatabaseConnectionTimeout(10), 408),
            (DatabaseConnectionError("refused"), 502),
        ],
    )
    def test_maps_to_http_status(self, error, expected):
        assert error.http_status_code() == expected

    def test_all_errors_share_a_base(self):
        assert isinstance(SchemaNotFoundError("x"), SchemaDiscoveryError)


class TestRealConnectionFailures:
    """No server needed: a closed port exercises driver load and error translation."""

    CLOSED_PORT_URL = "postgresql://u:p@127.0.0.1:1/db?sslmode=disable"

    def test_engine_loads_psycopg_driver(self):
        from feast.schema_discovery.dialects import parse_connection_url
        from feast.schema_discovery.introspector import engine_for

        with engine_for(parse_connection_url(self.CLOSED_PORT_URL), 2) as engine:
            assert engine.dialect.name == "postgresql"
            assert engine.dialect.driver == "psycopg"

    def test_list_schemas_translates_refused_connection(self):
        service = SchemaDiscoveryService(connect_timeout=2)
        with pytest.raises(DatabaseConnectionError) as excinfo:
            service.list_schemas(self.CLOSED_PORT_URL)
        assert excinfo.value.http_status_code() == 502

    def test_describe_table_translates_refused_connection(self):
        service = SchemaDiscoveryService(connect_timeout=2)
        with pytest.raises(DatabaseConnectionError):
            service.describe_table(self.CLOSED_PORT_URL, "public.users")

    def test_password_is_not_leaked_in_error_message(self):
        service = SchemaDiscoveryService(connect_timeout=2)
        with pytest.raises(DatabaseConnectionError) as excinfo:
            service.list_schemas(
                "postgresql://u:sup3rs3cret@127.0.0.1:1/db?sslmode=disable"  # pragma: allowlist secret
            )
        assert "sup3rs3cret" not in str(excinfo.value)
