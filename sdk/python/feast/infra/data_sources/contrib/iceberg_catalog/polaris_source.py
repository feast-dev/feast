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

import json
import logging
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

from feast.credentials import ConnectionRef
from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)
from feast.infra.data_sources.contrib.iceberg_catalog.polaris_client import (
    DEFAULT_OAUTH_SCOPE,
    PolarisClient,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto

logger = logging.getLogger(__name__)

DEFAULT_TIMESTAMP_CANDIDATES = ("event_timestamp", "event_ts", "timestamp", "ts")


class PolarisSource(IcebergSource):
    """Data source backed by an Apache Polaris catalog.

    Extends :class:`IcebergSource` with Polaris-specific behaviour:

    - ``endpoint`` defaults to ``$POLARIS_HOST/api/catalog``.
    - Authentication via OAuth2 client credentials (read from the environment
      variables named by ``client_id_env_var`` / ``client_secret_env_var``,
      with automatic token refresh), falling back to a static bearer token in
      ``token_env_var``. Only environment variable *names* are stored in the
      registry, never secrets.
    - Credential vending (``X-Iceberg-Access-Delegation``) is requested for
      reads and writes done through PyIceberg when ``credential_vending`` is set.
    - With ``sync_metadata`` set, ``PolarisProvider`` records Feast metadata
      (project, feature view, owner, tags) as Polaris table properties on
      ``feast apply``.

    ``warehouse`` is the Polaris catalog name.
    """

    def __init__(
        self,
        *,
        warehouse: str,
        namespace: str,
        table: str,
        name: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        endpoint: Optional[str] = None,
        token_env_var: Optional[str] = None,
        client_id_env_var: Optional[str] = None,
        client_secret_env_var: Optional[str] = None,
        oauth_scope: str = DEFAULT_OAUTH_SCOPE,
        oauth_server_uri: Optional[str] = None,
        credential_vending: bool = True,
        catalog_name: str = "feast_polaris",
        sync_metadata: bool = True,
        connection_ref: Optional[ConnectionRef] = None,
    ):
        if endpoint is None:
            host = os.environ.get("POLARIS_HOST", "").rstrip("/")
            if not host:
                raise ValueError(
                    "PolarisSource needs an endpoint: pass `endpoint=` or set "
                    "the POLARIS_HOST environment variable."
                )
            endpoint = f"{host}/api/catalog"

        super().__init__(
            warehouse=warehouse,
            namespace=namespace,
            table=table,
            catalog_type="rest",
            catalog_name=catalog_name,
            endpoint=endpoint,
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
            token_env_var=token_env_var or "POLARIS_TOKEN",
            credential_vending=credential_vending,
            connection_ref=connection_ref,
        )
        self.client_id_env_var = client_id_env_var or "POLARIS_CLIENT_ID"
        self.client_secret_env_var = client_secret_env_var or "POLARIS_CLIENT_SECRET"
        self.oauth_scope = oauth_scope
        self.oauth_server_uri = oauth_server_uri
        self.sync_metadata = sync_metadata

    def _resolve_client_credentials(self) -> Tuple[Optional[str], Optional[str]]:
        client_id = os.environ.get(self.client_id_env_var)
        client_secret = os.environ.get(self.client_secret_env_var)
        if client_id and client_secret:
            return client_id, client_secret
        return None, None

    def get_catalog_client(self) -> PolarisClient:  # type: ignore[override]
        client_id, client_secret = self._resolve_client_credentials()
        token = os.environ.get(self.token_env_var) if self.token_env_var else None
        if not (client_id and client_secret) and not token:
            logger.warning(
                "No Polaris credentials found in the environment for source "
                "%s (expected OAuth client credentials or a bearer token); "
                "requests will be unauthenticated.",
                self.name,
            )
        return PolarisClient(
            endpoint=self.endpoint,
            warehouse=self.warehouse,
            token=token,
            client_id=client_id,
            client_secret=client_secret,
            scope=self.oauth_scope,
            oauth_server_uri=self.oauth_server_uri,
            credential_vending=self.credential_vending,
        )

    def get_pyiceberg_catalog_config(self) -> Dict[str, str]:
        config: Dict[str, str] = {
            "type": "rest",
            "uri": self.endpoint,
            "warehouse": self.warehouse,
        }
        client_id, client_secret = self._resolve_client_credentials()
        if client_id and client_secret:
            config["credential"] = f"{client_id}:{client_secret}"
            config["scope"] = self.oauth_scope
            if self.oauth_server_uri:
                config["oauth2-server-uri"] = self.oauth_server_uri
        elif self.token_env_var:
            token = os.environ.get(self.token_env_var)
            if token:
                config["token"] = token
        if self.credential_vending:
            config["header.X-Iceberg-Access-Delegation"] = "vended-credentials"
        return config

    @staticmethod
    def discover(
        *,
        warehouse: str,
        endpoint: Optional[str] = None,
        namespaces: Optional[Sequence[str]] = None,
        timestamp_fields: Sequence[str] = DEFAULT_TIMESTAMP_CANDIDATES,
        **source_kwargs: Any,
    ) -> List["PolarisSource"]:
        """Build a ``PolarisSource`` for every table in the catalog.

        Tables are skipped unless they have a column named in
        ``timestamp_fields`` (the first match is used as ``timestamp_field``).
        Extra keyword arguments (``token_env_var``, ``client_id_env_var``,
        ``tags``, ...) are passed to every created source. Each source is named
        ``<namespace>__<table>``.
        """
        probe = PolarisSource(
            warehouse=warehouse,
            namespace="_",
            table="_",
            endpoint=endpoint,
            **source_kwargs,
        )
        client = probe.get_catalog_client()
        sources: List[PolarisSource] = []
        for namespace in namespaces or client.list_namespaces():
            for table in client.list_tables(namespace):
                columns = {f["name"] for f in client.get_table_schema(namespace, table)}
                timestamp_field = next(
                    (c for c in timestamp_fields if c in columns), None
                )
                if timestamp_field is None:
                    logger.info(
                        "Skipping %s.%s: no timestamp column among %s",
                        namespace,
                        table,
                        list(timestamp_fields),
                    )
                    continue
                sources.append(
                    PolarisSource(
                        name=f"{namespace}__{table}",
                        warehouse=warehouse,
                        namespace=namespace,
                        table=table,
                        endpoint=probe.endpoint,
                        timestamp_field=timestamp_field,
                        **source_kwargs,
                    )
                )
        return sources

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        options = json.loads(
            str(data_source.custom_options.configuration, encoding="utf8")
        )
        return PolarisSource(
            name=data_source.name,
            endpoint=options["endpoint"],
            warehouse=options["warehouse"],
            namespace=options["namespace"],
            table=options["table"],
            token_env_var=options.get("token_env_var"),
            client_id_env_var=options.get("client_id_env_var"),
            client_secret_env_var=options.get("client_secret_env_var"),
            oauth_scope=options.get("oauth_scope", DEFAULT_OAUTH_SCOPE),
            oauth_server_uri=options.get("oauth_server_uri"),
            credential_vending=options.get("credential_vending", True),
            catalog_name=options.get("catalog_name", "feast_polaris"),
            sync_metadata=options.get("sync_metadata", True),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
            connection_ref=ConnectionRef.from_tags(dict(data_source.tags)),
        )

    def _to_proto_impl(self) -> DataSourceProto:
        config_json = json.dumps(
            {
                "endpoint": self.endpoint,
                "warehouse": self.warehouse,
                "namespace": self.namespace,
                "table": self.iceberg_table,
                "token_env_var": self.token_env_var,
                "client_id_env_var": self.client_id_env_var,
                "client_secret_env_var": self.client_secret_env_var,
                "oauth_scope": self.oauth_scope,
                "oauth_server_uri": self.oauth_server_uri,
                "credential_vending": self.credential_vending,
                "catalog_name": self.catalog_name,
                "sync_metadata": self.sync_metadata,
            }
        )
        return DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_ICEBERG,
            data_source_class_type=(
                "feast.infra.data_sources.contrib.iceberg_catalog"
                ".polaris_source.PolarisSource"
            ),
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=bytes(config_json, encoding="utf8")
            ),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
            field_mapping=self.field_mapping,
        )

    def __eq__(self, other):
        if not isinstance(other, PolarisSource):
            return False
        if not super().__eq__(other):
            return False
        return (
            self.client_id_env_var == other.client_id_env_var
            and self.client_secret_env_var == other.client_secret_env_var
            and self.oauth_scope == other.oauth_scope
            and self.oauth_server_uri == other.oauth_server_uri
            and self.sync_metadata == other.sync_metadata
        )

    def __hash__(self):
        return hash(
            (
                self.name,
                self.endpoint,
                self.warehouse,
                self.namespace,
                self.iceberg_table,
            )
        )
