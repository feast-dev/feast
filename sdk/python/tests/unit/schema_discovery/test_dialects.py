import pytest

from feast.schema_discovery.dialects import (
    build_data_source,
    build_repo_config,
    default_schema_for,
    normalize_engine_url,
    parse_connection_url,
    split_table_name,
    validate_identifier,
)
from feast.schema_discovery.errors import (
    InvalidConnectionUrlError,
    InvalidIdentifierError,
    UnsupportedDialectError,
)

VALID_URL = (
    "postgresql://alice:s3cret@db.internal:5432/mydb"  # pragma: allowlist secret
)


class TestParseConnectionUrl:
    def test_parses_postgres_url(self):
        url = parse_connection_url(VALID_URL)
        assert url.get_backend_name() == "postgresql"
        assert url.host == "db.internal"
        assert url.port == 5432
        assert url.database == "mydb"
        assert url.username == "alice"
        assert url.password == "s3cret"  # pragma: allowlist secret

    def test_accepts_explicit_driver(self):
        url = parse_connection_url("postgresql+psycopg://alice:p@h:5432/db")
        assert url.get_backend_name() == "postgresql"

    def test_strips_surrounding_whitespace(self):
        assert parse_connection_url(f"  {VALID_URL}  ").database == "mydb"

    @pytest.mark.parametrize("url", ["", "   ", None])
    def test_rejects_empty(self, url):
        with pytest.raises(InvalidConnectionUrlError):
            parse_connection_url(url)

    def test_rejects_malformed(self):
        with pytest.raises(InvalidConnectionUrlError):
            parse_connection_url("not a url at all")

    @pytest.mark.parametrize(
        "url,missing",
        [
            ("postgresql://alice@/mydb", "host"),
            ("postgresql://alice@host:5432/", "database"),
            ("postgresql://host:5432/mydb", "username"),
        ],
    )
    def test_rejects_incomplete_url(self, url, missing):
        with pytest.raises(InvalidConnectionUrlError):
            parse_connection_url(url)

    @pytest.mark.parametrize(
        "url",
        [
            "mysql://u:p@h:3306/db",
            "snowflake://u:p@account/db",
            "bigquery://project/dataset",
            "sqlite:///local.db",
            "trino://u@h:8080/catalog",
        ],
    )
    def test_rejects_unsupported_dialects(self, url):
        """v1 is PostgreSQL only."""
        with pytest.raises(UnsupportedDialectError):
            parse_connection_url(url)

    def test_unsupported_dialect_error_names_supported_set(self):
        with pytest.raises(UnsupportedDialectError, match="postgresql"):
            parse_connection_url("mysql://u:p@h:3306/db")


class TestNormalizeEngineUrl:
    def test_pins_driver_on_bare_backend(self):
        url = normalize_engine_url(parse_connection_url(VALID_URL))
        assert url.drivername == "postgresql+psycopg"

    def test_preserves_explicit_driver(self):
        url = normalize_engine_url(
            parse_connection_url("postgresql+psycopg2://a:p@h:5432/db")
        )
        assert url.drivername == "postgresql+psycopg2"


class TestValidateIdentifier:
    @pytest.mark.parametrize(
        "ident", ["users", "_private", "Table1", "a$b", "user_events_v2", "T"]
    )
    def test_accepts_valid(self, ident):
        assert validate_identifier(ident) == ident

    @pytest.mark.parametrize(
        "ident",
        [
            "",
            "1users",
            "user events",
            "users;",
            "users--",
            '"users"',
            "users'",
            "us*ers",
            "users\n",
            "sch.tbl",
        ],
    )
    def test_rejects_invalid(self, ident):
        with pytest.raises(InvalidIdentifierError):
            validate_identifier(ident)


class TestSplitTableName:
    def test_simple_name_without_schema(self):
        assert split_table_name("users") == (None, "users")

    def test_simple_name_with_schema_arg(self):
        assert split_table_name("users", "analytics") == ("analytics", "users")

    def test_qualified_name(self):
        assert split_table_name("public.users") == ("public", "users")

    def test_qualifier_beats_schema_arg(self):
        assert split_table_name("public.users", "analytics") == ("public", "users")

    def test_rejects_three_part_name(self):
        with pytest.raises(InvalidIdentifierError):
            split_table_name("db.public.users")

    def test_rejects_empty(self):
        with pytest.raises(InvalidIdentifierError):
            split_table_name("")

    @pytest.mark.parametrize(
        "table",
        [
            "users; DROP TABLE accounts",
            "users WHERE 1=1",
            "users) UNION SELECT * FROM secrets --",
            "pg_shadow--",
            "users/*",
        ],
    )
    def test_rejects_sql_injection_attempts(self, table):
        """Table names are interpolated into SQL, so they must be strictly validated."""
        with pytest.raises(InvalidIdentifierError):
            split_table_name(table)

    def test_rejects_injection_in_schema_arg(self):
        with pytest.raises(InvalidIdentifierError):
            split_table_name("users", "public; DROP SCHEMA x")


class TestBuildRepoConfig:
    def test_maps_url_parts_onto_offline_store(self):
        config = build_repo_config(parse_connection_url(VALID_URL), db_schema="public")
        store = config.offline_store
        assert store.host == "db.internal"
        assert store.port == 5432
        assert store.database == "mydb"
        assert store.user == "alice"
        assert store.password == "s3cret"  # pragma: allowlist secret
        assert store.db_schema == "public"

    def test_defaults_port_when_absent(self):
        config = build_repo_config(
            parse_connection_url("postgresql://a:p@h/db"), db_schema="public"
        )
        assert config.offline_store.port == 5432

    def test_honours_non_default_schema(self):
        config = build_repo_config(
            parse_connection_url(VALID_URL), db_schema="analytics"
        )
        assert config.offline_store.db_schema == "analytics"

    def test_sslmode_defaults_to_require(self):
        config = build_repo_config(parse_connection_url(VALID_URL), db_schema="public")
        assert config.offline_store.sslmode == "require"

    def test_sslmode_read_from_query_string(self):
        config = build_repo_config(
            parse_connection_url(f"{VALID_URL}?sslmode=disable"), db_schema="public"
        )
        assert config.offline_store.sslmode == "disable"

    def test_does_not_require_feature_store_yaml(self):
        """The whole point: an ad-hoc config with no repo on disk."""
        config = build_repo_config(parse_connection_url(VALID_URL), db_schema="public")
        assert config.project == "schema_discovery"


class TestBuildDataSource:
    def test_builds_qualified_source(self):
        source = build_data_source(parse_connection_url(VALID_URL), "public", "users")
        assert source.get_table_query_string() == "public.users"

    def test_exposes_feast_type_mapper(self):
        source = build_data_source(parse_connection_url(VALID_URL), "public", "users")
        mapper = type(source).source_datatype_to_feast_value_type()
        assert mapper.__name__ == "pg_type_to_feast_value_type"


class TestDefaultSchema:
    def test_postgres_defaults_to_public(self):
        assert default_schema_for(parse_connection_url(VALID_URL)) == "public"
