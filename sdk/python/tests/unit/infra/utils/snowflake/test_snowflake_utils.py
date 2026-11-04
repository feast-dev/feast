import tempfile
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from feast.infra.utils.snowflake.snowflake_utils import (
    GetSnowflakeConnection,
    execute_snowflake_statement,
    parse_private_key_path,
)

PRIVATE_KEY_PASSPHRASE = "test"


def _pem_private_key(passphrase: Optional[str]):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=(
            serialization.BestAvailableEncryption(passphrase.encode())
            if passphrase
            else serialization.NoEncryption()
        ),
    )


@pytest.fixture
def unencrypted_private_key():
    return _pem_private_key(None)


@pytest.fixture
def encrypted_private_key():
    return _pem_private_key(PRIVATE_KEY_PASSPHRASE)


def test_parse_private_key_path_key_content_unencrypted(unencrypted_private_key):
    parse_private_key_path(
        None,
        None,
        unencrypted_private_key,
    )


def test_parse_private_key_path_key_content_encrypted(encrypted_private_key):
    parse_private_key_path(
        PRIVATE_KEY_PASSPHRASE,
        None,
        encrypted_private_key,
    )


def test_parse_private_key_path_key_path_unencrypted(unencrypted_private_key):
    with tempfile.NamedTemporaryFile(mode="wb") as f:
        f.write(unencrypted_private_key)
        f.flush()
        parse_private_key_path(
            None,
            f.name,
            None,
        )


def test_parse_private_key_path_key_path_encrypted(encrypted_private_key):
    with tempfile.NamedTemporaryFile(mode="wb") as f:
        f.write(encrypted_private_key)
        f.flush()
        parse_private_key_path(
            PRIVATE_KEY_PASSPHRASE,
            f.name,
            None,
        )


class _AttrDict(dict):
    __getattr__ = dict.__getitem__


def _make_config(**overrides):
    defaults = {
        "type": "snowflake.offline",
        "account": "test_account",
        "user": "test_user",
        "password": "test_password",  # pragma: allowlist secret
        "role": "test_role",
        "warehouse": "test_wh",
        "database": "test_db",
        "schema_": "test_schema",
        "config_path": "",
    }
    defaults.update(overrides)
    return _AttrDict(defaults)


@patch("feast.infra.utils.snowflake.snowflake_utils.snowflake.connector")
class TestGetSnowflakeConnectionIdentifierQuoting:
    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        with patch("feast.infra.utils.snowflake.snowflake_utils._cache", {}):
            yield

    @pytest.mark.parametrize(
        "config_key,connect_key,value",
        [
            ("warehouse", "warehouse", "MY_WH"),
            ("role", "role", "ANALYST"),
            ("database", "database", "PROD_DB"),
            ("schema_", "schema", "PUBLIC"),
        ],
    )
    def test_identifier_passed_without_quoting(
        self, mock_connector, config_key, connect_key, value
    ):
        mock_connector.connect.return_value = MagicMock()

        with GetSnowflakeConnection(_make_config(**{config_key: value})):
            pass

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs[connect_key] == value

    def test_schema_key_renamed_from_schema_underscore(self, mock_connector):
        mock_connector.connect.return_value = MagicMock()

        with GetSnowflakeConnection(_make_config(schema_="analytics")):
            pass

        kwargs = mock_connector.connect.call_args[1]
        assert "schema" in kwargs
        assert "schema_" not in kwargs


class TestExecuteSnowflakeStatement:
    def test_empty_query_is_passed_through_to_execute(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_executed_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.return_value = mock_executed_cursor

        result = execute_snowflake_statement(mock_conn, "")

        assert result is mock_executed_cursor
        mock_cursor.execute.assert_called_once_with("")

    def test_valid_query_executes_and_returns_cursor(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_executed_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.return_value = mock_executed_cursor

        result = execute_snowflake_statement(mock_conn, "SELECT 1")

        assert result is mock_executed_cursor
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_valid_query_raises_on_none_cursor(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.return_value = None

        with pytest.raises(Exception, match="Snowflake query failed"):
            execute_snowflake_statement(mock_conn, "SELECT 1")
