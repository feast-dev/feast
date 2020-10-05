# Copyright 2019 The Feast Authors
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
import datetime
import logging
import multiprocessing
import os
import shutil
import tempfile
import time
from math import ceil
from typing import Dict, List, Optional, Tuple, Union

import grpc
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

from feast.config import Config
from feast.constants import (
    CONFIG_CORE_ENABLE_SSL_KEY,
    CONFIG_CORE_SERVER_SSL_CERT_KEY,
    CONFIG_CORE_URL_KEY,
    CONFIG_ENABLE_AUTH_KEY,
    CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY,
    CONFIG_PROJECT_KEY,
    CONFIG_SERVING_ENABLE_SSL_KEY,
    CONFIG_SERVING_SERVER_SSL_CERT_KEY,
    CONFIG_SERVING_URL_KEY,
    FEAST_DEFAULT_OPTIONS,
)
from feast.core.CoreService_pb2 import (
    ApplyEntityRequest,
    ApplyEntityResponse,
    ApplyFeatureTableRequest,
    ApplyFeatureTableResponse,
    ArchiveProjectRequest,
    ArchiveProjectResponse,
    CreateProjectRequest,
    CreateProjectResponse,
    GetEntityRequest,
    GetEntityResponse,
    GetFeastCoreVersionRequest,
    GetFeatureTableRequest,
    GetFeatureTableResponse,
    ListEntitiesRequest,
    ListEntitiesResponse,
    ListFeatureTablesRequest,
    ListFeatureTablesResponse,
    ListProjectsRequest,
    ListProjectsResponse,
)
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.data_source import SourceType
from feast.entity import Entity
from feast.feature_table import FeatureTable
from feast.grpc import auth as feast_auth
from feast.grpc.grpc import create_grpc_channel
from feast.loaders.ingest import (
    BATCH_INGESTION_PRODUCTION_TIMEOUT,
    _check_field_mappings,
)
from feast.serving.ServingService_pb2 import GetFeastServingInfoRequest
from feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.staging.storage_client import get_staging_client

_logger = logging.getLogger(__name__)

CPU_COUNT: int = multiprocessing.cpu_count()


class Client:
    """
    Feast Client: Used for creating, managing, and retrieving features.
    """

    def __init__(self, options: Optional[Dict[str, str]] = None, **kwargs):
        """
        The Feast Client should be initialized with at least one service url
        Please see constants.py for configuration options. Commonly used options
        or arguments include:
            core_url: Feast Core URL. Used to manage features
            serving_url: Feast Serving URL. Used to retrieve features
            project: Sets the active project. This field is optional.
            core_secure: Use client-side SSL/TLS for Core gRPC API
            serving_secure: Use client-side SSL/TLS for Serving gRPC API
            enable_auth: Enable authentication and authorization
            auth_provider: Authentication provider – "google" or "oauth"
            if auth_provider is "oauth", the following fields are mandatory –
            oauth_grant_type, oauth_client_id, oauth_client_secret, oauth_audience, oauth_token_request_url

        Args:
            options: Configuration options to initialize client with
            **kwargs: Additional keyword arguments that will be used as
                configuration options along with "options"
        """

        if options is None:
            options = dict()
        self._config = Config(options={**options, **kwargs})

        self._core_service_stub: Optional[CoreServiceStub] = None
        self._serving_service_stub: Optional[ServingServiceStub] = None
        self._auth_metadata: Optional[grpc.AuthMetadataPlugin] = None

        # Configure Auth Metadata Plugin if auth is enabled
        if self._config.getboolean(CONFIG_ENABLE_AUTH_KEY):
            self._auth_metadata = feast_auth.get_auth_metadata_plugin(self._config)

    @property
    def _core_service(self):
        """
        Creates or returns the gRPC Feast Core Service Stub

        Returns: CoreServiceStub
        """
        if not self._core_service_stub:
            channel = create_grpc_channel(
                url=self._config.get(CONFIG_CORE_URL_KEY),
                enable_ssl=self._config.getboolean(CONFIG_CORE_ENABLE_SSL_KEY),
                enable_auth=self._config.getboolean(CONFIG_ENABLE_AUTH_KEY),
                ssl_server_cert_path=self._config.get(CONFIG_CORE_SERVER_SSL_CERT_KEY),
                auth_metadata_plugin=self._auth_metadata,
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
            )
            self._core_service_stub = CoreServiceStub(channel)
        return self._core_service_stub

    @property
    def _serving_service(self):
        """
        Creates or returns the gRPC Feast Serving Service Stub

        Returns: ServingServiceStub
        """
        if not self._serving_service_stub:
            channel = create_grpc_channel(
                url=self._config.get(CONFIG_SERVING_URL_KEY),
                enable_ssl=self._config.getboolean(CONFIG_SERVING_ENABLE_SSL_KEY),
                enable_auth=self._config.getboolean(CONFIG_ENABLE_AUTH_KEY),
                ssl_server_cert_path=self._config.get(
                    CONFIG_SERVING_SERVER_SSL_CERT_KEY
                ),
                auth_metadata_plugin=self._auth_metadata,
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
            )
            self._serving_service_stub = ServingServiceStub(channel)
        return self._serving_service_stub

    @property
    def core_url(self) -> str:
        """
        Retrieve Feast Core URL

        Returns:
            Feast Core URL string
        """
        return self._config.get(CONFIG_CORE_URL_KEY)

    @core_url.setter
    def core_url(self, value: str):
        """
        Set the Feast Core URL

        Args:
            value: Feast Core URL
        """
        self._config.set(CONFIG_CORE_URL_KEY, value)

    @property
    def serving_url(self) -> str:
        """
        Retrieve Serving Core URL

        Returns:
            Feast Serving URL string
        """
        return self._config.get(CONFIG_SERVING_URL_KEY)

    @serving_url.setter
    def serving_url(self, value: str):
        """
        Set the Feast Serving URL

        Args:
            value: Feast Serving URL
        """
        self._config.set(CONFIG_SERVING_URL_KEY, value)

    @property
    def core_secure(self) -> bool:
        """
        Retrieve Feast Core client-side SSL/TLS setting

        Returns:
            Whether client-side SSL/TLS is enabled
        """
        return self._config.getboolean(CONFIG_CORE_ENABLE_SSL_KEY)

    @core_secure.setter
    def core_secure(self, value: bool):
        """
        Set the Feast Core client-side SSL/TLS setting

        Args:
            value: True to enable client-side SSL/TLS
        """
        self._config.set(CONFIG_CORE_ENABLE_SSL_KEY, value)

    @property
    def serving_secure(self) -> bool:
        """
        Retrieve Feast Serving client-side SSL/TLS setting

        Returns:
            Whether client-side SSL/TLS is enabled
        """
        return self._config.getboolean(CONFIG_SERVING_ENABLE_SSL_KEY)

    @serving_secure.setter
    def serving_secure(self, value: bool):
        """
        Set the Feast Serving client-side SSL/TLS setting

        Args:
            value: True to enable client-side SSL/TLS
        """
        self._config.set(CONFIG_SERVING_ENABLE_SSL_KEY, value)

    def version(self):
        """
        Returns version information from Feast Core and Feast Serving
        """
        import pkg_resources

        result = {
            "sdk": {"version": pkg_resources.get_distribution("feast").version},
            "serving": "not configured",
            "core": "not configured",
        }

        if self.serving_url:
            serving_version = self._serving_service.GetFeastServingInfo(
                GetFeastServingInfoRequest(),
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
                metadata=self._get_grpc_metadata(),
            ).version
            result["serving"] = {"url": self.serving_url, "version": serving_version}

        if self.core_url:
            core_version = self._core_service.GetFeastCoreVersion(
                GetFeastCoreVersionRequest(),
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
                metadata=self._get_grpc_metadata(),
            ).version
            result["core"] = {"url": self.core_url, "version": core_version}

        return result

    @property
    def project(self) -> Union[str, None]:
        """
        Retrieve currently active project

        Returns:
            Project name
        """
        if not self._config.get(CONFIG_PROJECT_KEY):
            raise ValueError("No project has been configured.")
        return self._config.get(CONFIG_PROJECT_KEY)

    def set_project(self, project: Optional[str] = None):
        """
        Set currently active Feast project

        Args:
            project: Project to set as active. If unset, will reset to the default project.
        """
        if project is None:
            project = FEAST_DEFAULT_OPTIONS[CONFIG_PROJECT_KEY]
        self._config.set(CONFIG_PROJECT_KEY, project)

    def list_projects(self) -> List[str]:
        """
        List all active Feast projects

        Returns:
            List of project names

        """

        response = self._core_service.ListProjects(
            ListProjectsRequest(),
            timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
            metadata=self._get_grpc_metadata(),
        )  # type: ListProjectsResponse
        return list(response.projects)

    def create_project(self, project: str):
        """
        Creates a Feast project

        Args:
            project: Name of project
        """

        self._core_service.CreateProject(
            CreateProjectRequest(name=project),
            timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
            metadata=self._get_grpc_metadata(),
        )  # type: CreateProjectResponse

    def archive_project(self, project):
        """
        Archives a project. Project will still continue to function for
        ingestion and retrieval, but will be in a read-only state. It will
        also not be visible from the Core API for management purposes.

        Args:
            project: Name of project to archive
        """

        try:
            self._core_service_stub.ArchiveProject(
                ArchiveProjectRequest(name=project),
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
                metadata=self._get_grpc_metadata(),
            )  # type: ArchiveProjectResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

        # revert to the default project
        if self._project == project:
            self._project = FEAST_DEFAULT_OPTIONS[CONFIG_PROJECT_KEY]

    def apply_entity(self, entities: Union[List[Entity], Entity], project: str = None):
        """
        Idempotently registers entities with Feast Core. Either a single
        entity or a list can be provided.

        Args:
            entities: List of entities that will be registered

        Examples:
            >>> from feast import Client
            >>> from feast.entity import Entity
            >>> from feast.value_type import ValueType
            >>>
            >>> feast_client = Client(core_url="localhost:6565")
            >>> entity = Entity(
            >>>     name="driver_entity",
            >>>     description="Driver entity for car rides",
            >>>     value_type=ValueType.STRING,
            >>>     labels={
            >>>         "key": "val"
            >>>     }
            >>> )
            >>> feast_client.apply_entity(entity)
        """

        if project is None:
            project = self.project

        if not isinstance(entities, list):
            entities = [entities]
        for entity in entities:
            if isinstance(entity, Entity):
                self._apply_entity(project, entity)  # type: ignore
                continue
            raise ValueError(f"Could not determine entity type to apply {entity}")

    def _apply_entity(self, project: str, entity: Entity):
        """
        Registers a single entity with Feast

        Args:
            entity: Entity that will be registered
        """

        entity.is_valid()
        entity_proto = entity.to_spec_proto()

        # Convert the entity to a request and send to Feast Core
        try:
            apply_entity_response = self._core_service.ApplyEntity(
                ApplyEntityRequest(project=project, spec=entity_proto),  # type: ignore
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
                metadata=self._get_grpc_metadata(),
            )  # type: ApplyEntityResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

        # Extract the returned entity
        applied_entity = Entity.from_proto(apply_entity_response.entity)

        # Deep copy from the returned entity to the local entity
        entity._update_from_entity(applied_entity)

    def list_entities(
        self, project: str = None, labels: Dict[str, str] = dict()
    ) -> List[Entity]:
        """
        Retrieve a list of entities from Feast Core

        Args:
            project: Filter entities based on project name
            labels: User-defined labels that these entities are associated with

        Returns:
            List of entities
        """

        if project is None:
            project = self.project

        filter = ListEntitiesRequest.Filter(project=project, labels=labels)

        # Get latest entities from Feast Core
        entity_protos = self._core_service.ListEntities(
            ListEntitiesRequest(filter=filter), metadata=self._get_grpc_metadata(),
        )  # type: ListEntitiesResponse

        # Extract entities and return
        entities = []
        for entity_proto in entity_protos.entities:
            entity = Entity.from_proto(entity_proto)
            entity._client = self
            entities.append(entity)
        return entities

    def get_entity(self, name: str, project: str = None) -> Union[Entity, None]:
        """
        Retrieves an entity.

        Args:
            project: Feast project that this entity belongs to
            name: Name of entity

        Returns:
            Returns either the specified entity, or raises an exception if
            none is found
        """

        if project is None:
            project = self.project

        try:
            get_entity_response = self._core_service.GetEntity(
                GetEntityRequest(project=project, name=name.strip()),
                metadata=self._get_grpc_metadata(),
            )  # type: GetEntityResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())
        entity = Entity.from_proto(get_entity_response.entity)

        return entity

    def apply_feature_table(
        self,
        feature_tables: Union[List[FeatureTable], FeatureTable],
        project: str = None,
    ):
        """
        Idempotently registers feature tables with Feast Core. Either a single
        feature table or a list can be provided.

        Args:
            feature_tables: List of feature tables that will be registered
        """

        if project is None:
            project = self.project

        if not isinstance(feature_tables, list):
            feature_tables = [feature_tables]
        for feature_table in feature_tables:
            if isinstance(feature_table, FeatureTable):
                self._apply_feature_table(project, feature_table)  # type: ignore
                continue
            raise ValueError(
                f"Could not determine feature table type to apply {feature_table}"
            )

    def _apply_feature_table(self, project: str, feature_table: FeatureTable):
        """
        Registers a single feature table with Feast

        Args:
            feature_table: Feature table that will be registered
        """

        feature_table.is_valid()
        feature_table_proto = feature_table.to_spec_proto()

        # Convert the feature table to a request and send to Feast Core
        try:
            apply_feature_table_response = self._core_service.ApplyFeatureTable(
                ApplyFeatureTableRequest(project=project, table_spec=feature_table_proto),  # type: ignore
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
                metadata=self._get_grpc_metadata(),
            )  # type: ApplyFeatureTableResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

        # Extract the returned feature table
        applied_feature_table = FeatureTable.from_proto(
            apply_feature_table_response.table
        )

        # Deep copy from the returned feature table to the local entity
        feature_table._update_from_feature_table(applied_feature_table)

    def list_feature_tables(
        self, project: str = None, labels: Dict[str, str] = dict()
    ) -> List[FeatureTable]:
        """
        Retrieve a list of feature tables from Feast Core

        Args:
            project: Filter feature tables based on project name

        Returns:
            List of feature tables
        """

        if project is None:
            project = self.project

        filter = ListFeatureTablesRequest.Filter(project=project, labels=labels)

        # Get latest feature tables from Feast Core
        feature_table_protos = self._core_service.ListFeatureTables(
            ListFeatureTablesRequest(filter=filter), metadata=self._get_grpc_metadata(),
        )  # type: ListFeatureTablesResponse

        # Extract feature tables and return
        feature_tables = []
        for feature_table_proto in feature_table_protos.tables:
            feature_table = FeatureTable.from_proto(feature_table_proto)
            feature_table._client = self
            feature_tables.append(feature_table)
        return feature_tables

    def get_feature_table(
        self, name: str, project: str = None
    ) -> Union[FeatureTable, None]:
        """
        Retrieves a feature table.

        Args:
            project: Feast project that this feature table belongs to
            name: Name of feature table

        Returns:
            Returns either the specified feature table, or raises an exception if
            none is found
        """

        if project is None:
            project = self.project

        try:
            get_feature_table_response = self._core_service.GetFeatureTable(
                GetFeatureTableRequest(project=project, name=name.strip()),
                metadata=self._get_grpc_metadata(),
            )  # type: GetFeatureTableResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())
        return FeatureTable.from_proto(get_feature_table_response.table)

    def ingest(
        self,
        feature_table: Union[str, FeatureTable],
        source: Union[pd.DataFrame, str],
        project: str = None,
        chunk_size: int = 10000,
        max_workers: int = max(CPU_COUNT - 1, 1),
        timeout: int = BATCH_INGESTION_PRODUCTION_TIMEOUT,
    ) -> None:
        """
        Batch load feature data into a FeatureTable.

        Args:
            feature_table (typing.Union[str, feast.feature_table.FeatureTable]):
                FeatureTable object or the string name of the feature table

            source (typing.Union[pd.DataFrame, str]):
                Either a file path or Pandas Dataframe to ingest into Feast
                Files that are currently supported:
                    * parquet
                    * csv
                    * json

            project: Feast project to locate FeatureTable

            chunk_size (int):
                Amount of rows to load and ingest at a time.

            max_workers (int):
                Number of worker processes to use to encode values.

            timeout (int):
                Timeout in seconds to wait for completion.

        Examples:
            >>> from feast import Client
            >>>
            >>> client = Client(core_url="localhost:6565")
            >>> ft_df = pd.DataFrame(
            >>>         {
            >>>            "datetime": [pd.datetime.now()],
            >>>            "driver": [1001],
            >>>            "rating": [4.3],
            >>>         }
            >>>     )
            >>> client.set_project("project1")
            >>>
            >>> driver_ft = client.get_feature_table("driver")
            >>> client.ingest(driver_ft, ft_df)
        """

        if project is None:
            project = self.project
        if isinstance(feature_table, FeatureTable):
            name = feature_table.name

        dir_path, dest_path, column_names = _read_table_from_source(
            source, chunk_size, max_workers
        )

        fetched_feature_table: Optional[FeatureTable] = self.get_feature_table(
            name, project
        )
        if fetched_feature_table is not None:
            feature_table = fetched_feature_table
        else:
            raise Exception(f"FeatureTable, {name} cannot be found.")

        # Check 1) Only parquet file format for FeatureTable batch source is supported
        if (
            feature_table.batch_source
            and SourceType(feature_table.batch_source.type).name == "BATCH_FILE"
            and "".join(
                feature_table.batch_source.file_options.file_format.split()
            ).lower()
            != "parquet"
        ):
            raise Exception(
                f"No suitable batch source found for FeatureTable, {name}."
                f"Only BATCH_FILE source with parquet format is supported for batch ingestion."
            )

        # Check 2) Check if FeatureTable batch source field mappings can be found in provided source table
        _check_field_mappings(
            column_names, name, feature_table.batch_source.field_mapping
        )

        batch_source_type = SourceType(feature_table.batch_source.type).name

        try:
            if batch_source_type == "BATCH_FILE":
                from urllib.parse import urlparse

                file_url = feature_table.batch_source.file_options.file_url[:-1]
                uri = urlparse(file_url)
                staging_client = get_staging_client(uri.scheme)

                file_name = dest_path.split("/")[-1]
                date_today = datetime.datetime.today().strftime("%Y-%m-%d")

                staging_client.upload_file(
                    dest_path,
                    uri.hostname,
                    str(uri.path).strip("/") + "/" + f"date={date_today}/" + file_name,
                )
            if batch_source_type == "BATCH_BIGQUERY":
                from google.cloud import bigquery

                bq_table_ref = feature_table.batch_source.bigquery_options.table_ref
                gcp_project, dataset_table = bq_table_ref.split(":")
                dataset, table = dataset_table.split(".")

                client = bigquery.Client(project=gcp_project)

                table_ref = client.dataset(dataset).table(table)
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = bigquery.SourceFormat.PARQUET

                # Check for date partitioning column in FeatureTable spec
                if feature_table.batch_source.date_partition_column:
                    time_partitioning_obj = bigquery.table.TimePartitioning(
                        field=feature_table.batch_source.date_partition_column
                    )
                    job_config.time_partitioning = time_partitioning_obj
                with open(dest_path, "rb") as source_file:
                    client.load_table_from_file(
                        source_file, table_ref, job_config=job_config
                    )
        finally:
            # Remove parquet file(s) that were created earlier
            print("Removing temporary file(s)...")
            shutil.rmtree(dir_path)

        print(
            f"Data has been successfully ingested into FeatureTable {batch_source_type} batch source."
        )

    def _get_grpc_metadata(self):
        """
        Returns a metadata tuple to attach to gRPC requests. This is primarily
        used when authentication is enabled but SSL/TLS is disabled.

        Returns: Tuple of metadata to attach to each gRPC call
        """
        if self._config.getboolean(CONFIG_ENABLE_AUTH_KEY) and self._auth_metadata:
            return self._auth_metadata.get_signed_meta()
        return ()


def _read_table_from_source(
    source: Union[pd.DataFrame, str], chunk_size: int, max_workers: int
) -> Tuple[str, str, List[str]]:
    """
    Infers a data source type (path or Pandas DataFrame) and reads it in as
    a PyArrow Table.

    The PyArrow Table that is read will be written to a parquet file with row
    group size determined by the minimum of:
        * (table.num_rows / max_workers)
        * chunk_size

    The parquet file that is created will be passed as file path to the
    multiprocessing pool workers.

    Args:
        source (Union[pd.DataFrame, str]):
            Either a string path or Pandas DataFrame.

        chunk_size (int):
            Number of worker processes to use to encode values.

        max_workers (int):
            Amount of rows to load and ingest at a time.

    Returns:
        Tuple[str, str, List[str]]:
            Tuple containing parent directory path, destination path to
            parquet file and column names of pyarrow table.
    """

    # Pandas DataFrame detected
    if isinstance(source, pd.DataFrame):
        table = pa.Table.from_pandas(df=source)

    # Inferring a string path
    elif isinstance(source, str):
        file_path = source
        filename, file_ext = os.path.splitext(file_path)

        if ".csv" in file_ext:
            from pyarrow import csv

            table = csv.read_csv(filename)
        elif ".json" in file_ext:
            from pyarrow import json

            table = json.read_json(filename)
        else:
            table = pq.read_table(file_path)
    else:
        raise ValueError(f"Unknown data source provided for ingestion: {source}")

    # Ensure that PyArrow table is initialised
    assert isinstance(table, pa.lib.Table)

    # Write table as parquet file with a specified row_group_size
    dir_path = tempfile.mkdtemp()
    tmp_table_name = f"{int(time.time())}.parquet"
    dest_path = f"{dir_path}/{tmp_table_name}"
    row_group_size = min(ceil(table.num_rows / max_workers), chunk_size)
    pq.write_table(table=table, where=dest_path, row_group_size=row_group_size)

    column_names = table.column_names

    # Remove table from memory
    del table

    return dir_path, dest_path, column_names
