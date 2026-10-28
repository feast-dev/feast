from typing import Iterable

from fastapi import status as HttpStatusCode

from feast.errors import FeastError


class SchemaDiscoveryError(FeastError):
    """Base class for all schema discovery failures."""


class InvalidConnectionUrlError(SchemaDiscoveryError):
    def __init__(self, reason: str):
        super().__init__(f"Invalid connection URL: {reason}")

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_400_BAD_REQUEST


class UnsupportedDialectError(SchemaDiscoveryError):
    def __init__(self, dialect: str, supported: Iterable[str]):
        super().__init__(
            f"Unsupported database dialect '{dialect}'. "
            f"Supported dialects: {', '.join(sorted(supported))}."
        )

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_400_BAD_REQUEST


class InvalidIdentifierError(SchemaDiscoveryError):
    def __init__(self, identifier: str):
        super().__init__(
            f"Invalid SQL identifier '{identifier}'. Expected an unquoted name, "
            f"optionally schema-qualified (e.g. 'users' or 'public.users')."
        )

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_400_BAD_REQUEST


class SchemaNotFoundError(SchemaDiscoveryError):
    def __init__(self, schema: str):
        super().__init__(f"Schema '{schema}' was not found in the database.")

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_404_NOT_FOUND


class TableNotFoundError(SchemaDiscoveryError):
    def __init__(self, table: str):
        super().__init__(f"Table '{table}' was not found in the database.")

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_404_NOT_FOUND


class DatabaseConnectionError(SchemaDiscoveryError):
    def __init__(self, reason: str):
        super().__init__(f"Failed to connect to or query the database: {reason}")

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_502_BAD_GATEWAY


class DatabaseConnectionTimeout(SchemaDiscoveryError):
    def __init__(self, timeout_seconds: int):
        super().__init__(
            f"Timed out connecting to the database after {timeout_seconds}s."
        )

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_408_REQUEST_TIMEOUT


class MissingDialectDependencyError(SchemaDiscoveryError):
    def __init__(self, dialect: str, extra: str):
        super().__init__(
            f"Schema discovery for '{dialect}' requires optional dependencies. "
            f"Install them with: pip install 'feast[{extra}]'"
        )

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_400_BAD_REQUEST
