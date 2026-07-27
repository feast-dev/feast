# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class IcebergCatalogError(Exception):
    """Base exception for Iceberg REST Catalog errors."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        self.status_code = status_code
        super().__init__(message)


class TableNotFoundError(IcebergCatalogError):
    """Raised when a table or namespace is not found (HTTP 404)."""

    pass


class CatalogAuthError(IcebergCatalogError):
    """Raised on authentication/authorization failures (HTTP 401/403)."""

    pass


@dataclass
class TableMetadata:
    """Metadata returned by the Iceberg REST Catalog for a table."""

    table_id: str
    location: str
    schema_fields: List[Dict[str, str]]
    properties: Dict[str, str] = field(default_factory=dict)
    format_version: int = 2
    current_snapshot_id: Optional[int] = None


@dataclass
class TempCredential:
    """Temporary, scoped credential vended by the catalog."""

    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    session_token: Optional[str] = None
    expiration_ms: Optional[int] = None
    uri: Optional[str] = None
    credential_type: str = "s3"


class IcebergRestClient:
    """Client for the standard Apache Iceberg REST Catalog protocol.

    Implements table metadata loading and credential vending as defined
    in the Iceberg REST Catalog specification. Works with any compliant
    catalog: Unity Catalog, Apache Polaris, Nessie, Snowflake Open Catalog, etc.

    URL construction follows the spec:
    - With warehouse and url_style="unity": /v1/catalogs/{warehouse}/...
      (Unity Catalog flavor)
    - With warehouse and url_style="prefix": /v1/{warehouse}/...
      (Polaris, Nessie, standard Iceberg REST spec)
    - Without warehouse: /v1/... (vanilla spec)
    """

    def __init__(
        self,
        endpoint: str,
        token: Optional[str] = None,
        warehouse: Optional[str] = None,
        credential_vending: bool = True,
        timeout: int = 30,
        url_style: str = "auto",
    ):
        """
        Args:
            endpoint: Base URL of the catalog server.
            token: Bearer token for authentication.
            warehouse: Warehouse/catalog identifier.
            credential_vending: Whether to request vended credentials.
            timeout: HTTP request timeout in seconds.
            url_style: URL path construction style:
                - "unity": /v1/catalogs/{warehouse} (Databricks Unity Catalog)
                - "prefix": /v1/{warehouse} (Polaris, Nessie, standard spec)
                - "auto" (default): uses "unity" if endpoint contains
                  "unity-catalog", otherwise "prefix"
        """
        self.endpoint = endpoint.rstrip("/")
        self.warehouse = warehouse
        self.credential_vending = credential_vending
        self.timeout = timeout

        if url_style == "auto":
            self.url_style = "unity" if "unity-catalog" in self.endpoint else "prefix"
        else:
            self.url_style = url_style

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"
        if credential_vending:
            self._session.headers["X-Iceberg-Access-Delegation"] = "vended-credentials"
        self.headers = dict(self._session.headers)

    @property
    def _base_url(self) -> str:
        """Build base URL following the Iceberg REST spec.

        Unity Catalog uses /v1/catalogs/{warehouse} while the standard
        spec uses /v1/{prefix} (where prefix = warehouse).
        """
        if self.warehouse:
            if self.url_style == "unity":
                return f"{self.endpoint}/v1/catalogs/{self.warehouse}"
            return f"{self.endpoint}/v1/{self.warehouse}"
        return f"{self.endpoint}/v1"

    def _raise_for_status(self, response: requests.Response, context: str):
        """Raise typed exceptions based on HTTP status codes."""
        if response.ok:
            return
        status = response.status_code
        try:
            body = response.json()
            detail = body.get("message", body.get("error", response.text))
        except Exception:
            detail = response.text

        msg = f"{context}: HTTP {status} — {detail}"
        if status == 404:
            raise TableNotFoundError(msg, status_code=status)
        if status in (401, 403):
            raise CatalogAuthError(msg, status_code=status)
        raise IcebergCatalogError(msg, status_code=status)

    def load_table(self, namespace: str, table: str) -> TableMetadata:
        """Load table metadata from the catalog.

        Raises:
            TableNotFoundError: If the table does not exist.
            CatalogAuthError: On authentication/authorization failures.
            IcebergCatalogError: On other HTTP errors.
        """
        url = f"{self._base_url}/namespaces/{namespace}/tables/{table}"
        try:
            response = self._session.get(url, timeout=self.timeout)
        except requests.exceptions.ConnectionError as e:
            raise IcebergCatalogError(
                f"Connection failed for {namespace}.{table}: {e}"
            ) from e
        except requests.exceptions.Timeout as e:
            raise IcebergCatalogError(
                f"Request timed out for {namespace}.{table}: {e}"
            ) from e

        self._raise_for_status(response, f"Failed to load table {namespace}.{table}")
        data = response.json()
        return self._parse_table_metadata(data)

    def get_table_location(self, namespace: str, table: str) -> Optional[str]:
        """Returns the storage location for a table."""
        metadata = self.load_table(namespace, table)
        return metadata.location

    def vend_credentials(self, namespace: str, table: str) -> Optional[TempCredential]:
        """Request temporary credentials for table access.

        Uses the standard Iceberg REST credential vending protocol via the
        X-Iceberg-Access-Delegation header. Credentials are scoped and short-lived.
        """
        if not self.credential_vending:
            return None

        url = f"{self._base_url}/namespaces/{namespace}/tables/{table}"
        try:
            response = self._session.get(url, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            logger.warning(
                "Credential vending failed for %s.%s: %s", namespace, table, e
            )
            return None

        self._raise_for_status(
            response, f"Credential vending failed for {namespace}.{table}"
        )
        data = response.json()
        return self._parse_credentials(data)

    def list_tables(self, namespace: str) -> List[str]:
        """List all tables in a namespace, handling pagination.

        Raises:
            CatalogAuthError: On authentication/authorization failures.
            IcebergCatalogError: On other HTTP errors.
        """
        tables: List[str] = []
        url = f"{self._base_url}/namespaces/{namespace}/tables"
        params: Dict[str, str] = {}

        while True:
            try:
                response = self._session.get(url, params=params, timeout=self.timeout)
            except requests.exceptions.RequestException as e:
                raise IcebergCatalogError(
                    f"Failed to list tables in namespace {namespace}: {e}"
                ) from e

            self._raise_for_status(
                response, f"Failed to list tables in namespace {namespace}"
            )
            data = response.json()
            identifiers = data.get("identifiers", [])
            tables.extend(ident["name"] for ident in identifiers if "name" in ident)

            next_token = data.get("next-page-token")
            if not next_token:
                break
            params["pageToken"] = next_token

        return tables

    def list_namespaces(self) -> List[str]:
        """List all namespaces in the catalog, handling pagination.

        Raises:
            CatalogAuthError: On authentication/authorization failures.
            IcebergCatalogError: On other HTTP errors.
        """
        result: List[str] = []
        url = f"{self._base_url}/namespaces"
        params: Dict[str, str] = {}

        while True:
            try:
                response = self._session.get(url, params=params, timeout=self.timeout)
            except requests.exceptions.RequestException as e:
                raise IcebergCatalogError(f"Failed to list namespaces: {e}") from e

            self._raise_for_status(response, "Failed to list namespaces")
            data = response.json()
            namespaces = data.get("namespaces", [])
            result.extend(".".join(ns) for ns in namespaces)

            next_token = data.get("next-page-token")
            if not next_token:
                break
            params["pageToken"] = next_token

        return result

    def get_table_schema(self, namespace: str, table: str) -> List[Dict[str, str]]:
        """Returns the schema fields for a table."""
        metadata = self.load_table(namespace, table)
        return metadata.schema_fields

    def _parse_table_metadata(self, data: Dict[str, Any]) -> TableMetadata:
        """Parse the Iceberg REST API response into TableMetadata."""
        metadata = data.get("metadata", data)
        location = metadata.get("location", "")
        properties = metadata.get("properties", {})
        format_version = metadata.get("format-version", 2)

        schema_fields = []
        schemas = metadata.get("schemas", [])
        if schemas:
            current_schema_id = metadata.get("current-schema-id")
            current_schema = None
            if current_schema_id is not None:
                for s in schemas:
                    if s.get("schema-id") == current_schema_id:
                        current_schema = s
                        break
            if current_schema is None:
                current_schema = schemas[-1]

            for col in current_schema.get("fields", []):
                field_type = col.get("type", "")
                if isinstance(field_type, dict):
                    field_type = field_type.get("type", "unknown")
                schema_fields.append(
                    {"name": col.get("name", ""), "type": str(field_type)}
                )

        current_snapshot_id = metadata.get("current-snapshot-id")
        table_uuid = metadata.get("table-uuid", "")

        return TableMetadata(
            table_id=table_uuid,
            location=location,
            schema_fields=schema_fields,
            properties=properties,
            format_version=format_version,
            current_snapshot_id=current_snapshot_id,
        )

    def _parse_credentials(self, data: Dict[str, Any]) -> Optional[TempCredential]:
        """Parse vended credentials from the load-table response config."""
        config = data.get("config", {})
        if not config:
            return None

        if any(k.startswith("s3.") for k in config):
            return TempCredential(
                access_key=config.get("s3.access-key-id"),
                secret_key=config.get("s3.secret-access-key"),
                session_token=config.get("s3.session-token"),
                expiration_ms=config.get("s3.session-token-expiration-ms"),
                uri=config.get("s3.endpoint"),
                credential_type="s3",
            )
        if any(k.startswith("gcs.") for k in config):
            return TempCredential(
                access_key=config.get("gcs.oauth2.token"),
                expiration_ms=config.get("gcs.oauth2.token-expires-at"),
                credential_type="gcs",
            )
        if any(k.startswith("adls.") for k in config):
            return TempCredential(
                access_key=config.get("adls.sas-token"),
                credential_type="adls",
            )
        return None
