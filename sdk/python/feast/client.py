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
import uuid
import warnings
from collections import OrderedDict
from math import ceil
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import grpc
import pandas as pd
import pyarrow as pa
from google.protobuf.timestamp_pb2 import Timestamp
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
    ApplyFeatureSetRequest,
    ApplyFeatureSetResponse,
    ArchiveProjectRequest,
    ArchiveProjectResponse,
    CreateProjectRequest,
    CreateProjectResponse,
    GetFeastCoreVersionRequest,
    GetFeatureSetRequest,
    GetFeatureSetResponse,
    GetFeatureStatisticsRequest,
    ListFeatureSetsRequest,
    ListFeatureSetsResponse,
    ListFeaturesRequest,
    ListFeaturesResponse,
    ListProjectsRequest,
    ListProjectsResponse,
)
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.FeatureSet_pb2 import FeatureSetStatus
from feast.feature import Feature, FeatureRef
from feast.feature_set import Entity, FeatureSet
from feast.grpc import auth as feast_auth
from feast.grpc.grpc import create_grpc_channel
from feast.job import RetrievalJob
from feast.loaders.abstract_producer import get_producer
from feast.loaders.file import export_source_to_staging_location
from feast.loaders.ingest import KAFKA_CHUNK_PRODUCTION_TIMEOUT, get_feature_row_chunks
from feast.online_response import OnlineResponse
from feast.serving.ServingService_pb2 import (
    DataFormat,
    DatasetSource,
    FeastServingType,
    FeatureReference,
    GetBatchFeaturesRequest,
    GetFeastServingInfoRequest,
    GetFeastServingInfoResponse,
    GetOnlineFeaturesRequest,
)
from feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.type_map import _python_value_to_proto_value, python_type_to_feast_value_type
from feast.types.Value_pb2 import Value as Value
from tensorflow_metadata.proto.v0 import statistics_pb2

_logger = logging.getLogger(__name__)

CPU_COUNT: int = multiprocessing.cpu_count()

warnings.simplefilter("once", DeprecationWarning)


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

    def apply(self, feature_sets: Union[List[FeatureSet], FeatureSet]):
        """
        Idempotently registers feature set(s) with Feast Core. Either a single
        feature set or a list can be provided.

        Args:
            feature_sets: List of feature sets that will be registered
        """
        if not isinstance(feature_sets, list):
            feature_sets = [feature_sets]
        for feature_set in feature_sets:
            if isinstance(feature_set, FeatureSet):
                self._apply_feature_set(feature_set)
                continue
            raise ValueError(
                f"Could not determine feature set type to apply {feature_set}"
            )

    def _apply_feature_set(self, feature_set: FeatureSet):
        """
        Registers a single feature set with Feast

        Args:
            feature_set: Feature set that will be registered
        """

        feature_set.is_valid()
        feature_set_proto = feature_set.to_proto()
        if len(feature_set_proto.spec.project) == 0:
            if self.project is not None:
                feature_set_proto.spec.project = self.project

        # Convert the feature set to a request and send to Feast Core
        try:
            apply_fs_response = self._core_service.ApplyFeatureSet(
                ApplyFeatureSetRequest(feature_set=feature_set_proto),
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
                metadata=self._get_grpc_metadata(),
            )  # type: ApplyFeatureSetResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

        # Extract the returned feature set
        applied_fs = FeatureSet.from_proto(apply_fs_response.feature_set)

        # If the feature set has changed, update the local copy
        if apply_fs_response.status == ApplyFeatureSetResponse.Status.CREATED:
            print(f'Feature set created: "{applied_fs.name}"')

        if apply_fs_response.status == ApplyFeatureSetResponse.Status.UPDATED:
            print(f'Feature set updated: "{applied_fs.name}"')

        # If no change has been applied, do nothing
        if apply_fs_response.status == ApplyFeatureSetResponse.Status.NO_CHANGE:
            print(f"No change detected or applied: {feature_set.name}")

        # Deep copy from the returned feature set to the local feature set
        feature_set._update_from_feature_set(applied_fs)

    def list_feature_sets(
        self, project: str = None, name: str = None, labels: Dict[str, str] = dict()
    ) -> List[FeatureSet]:
        """
        Retrieve a list of feature sets from Feast Core

        Args:
            project: Filter feature sets based on project name
            name: Filter feature sets based on feature set name

        Returns:
            List of feature sets
        """

        if project is None:
            if self.project is not None:
                project = self.project
            else:
                project = "*"

        if name is None:
            name = "*"

        filter = ListFeatureSetsRequest.Filter(
            project=project, feature_set_name=name, labels=labels
        )

        # Get latest feature sets from Feast Core
        feature_set_protos = self._core_service.ListFeatureSets(
            ListFeatureSetsRequest(filter=filter), metadata=self._get_grpc_metadata(),
        )  # type: ListFeatureSetsResponse

        # Extract feature sets and return
        feature_sets = []
        for feature_set_proto in feature_set_protos.feature_sets:
            feature_set = FeatureSet.from_proto(feature_set_proto)
            feature_set._client = self
            feature_sets.append(feature_set)
        return feature_sets

    def get_feature_set(
        self, name: str, project: str = None
    ) -> Union[FeatureSet, None]:
        """
        Retrieves a feature set.

        Args:
            project: Feast project that this feature set belongs to
            name: Name of feature set

        Returns:
            Returns either the specified feature set, or raises an exception if
            none is found
        """

        if project is None:
            if self.project is not None:
                project = self.project
            else:
                raise ValueError("No project has been configured.")

        try:
            get_feature_set_response = self._core_service.GetFeatureSet(
                GetFeatureSetRequest(project=project, name=name.strip()),
                metadata=self._get_grpc_metadata(),
            )  # type: GetFeatureSetResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())
        return FeatureSet.from_proto(get_feature_set_response.feature_set)

    def list_features_by_ref(
        self,
        project: str = None,
        entities: List[str] = list(),
        labels: Dict[str, str] = dict(),
    ) -> Dict[FeatureRef, Feature]:
        """
        Returns a list of features based on filters provided.

        Args:
            project: Feast project that these features belongs to
            entities: Feast entity that these features are associated with
            labels: Feast labels that these features are associated with

        Returns:
            Dictionary of <feature references: features>

        Examples:
            >>> from feast import Client
            >>>
            >>> feast_client = Client(core_url="localhost:6565")
            >>> features = list_features_by_ref(project="test_project", entities=["driver_id"], labels={"key1":"val1","key2":"val2"})
            >>> print(features)
        """
        if project is None:
            if self.project is not None:
                project = self.project
            else:
                project = "default"

        filter = ListFeaturesRequest.Filter(
            project=project, entities=entities, labels=labels
        )

        feature_protos = self._core_service.ListFeatures(
            ListFeaturesRequest(filter=filter), metadata=self._get_grpc_metadata(),
        )  # type: ListFeaturesResponse

        features_dict = {}
        for ref_str, feature_proto in feature_protos.features.items():
            feature_ref = FeatureRef.from_str(ref_str, ignore_project=True)
            feature = Feature.from_proto(feature_proto)
            features_dict[feature_ref] = feature

        return features_dict

    def list_entities(self) -> Dict[str, Entity]:
        """
        Returns a dictionary of entities across all feature sets
        Returns:
            Dictionary of entities, indexed by name
        """
        entities_dict = OrderedDict()
        for fs in self.list_feature_sets():
            for entity in fs.entities:
                entities_dict[entity.name] = entity
        return entities_dict

    def get_batch_features(
        self,
        feature_refs: List[str],
        entity_rows: Union[pd.DataFrame, str],
        compute_statistics: bool = False,
        project: str = None,
    ) -> RetrievalJob:
        """
        Deprecated. Please see get_historical_features.
        """
        warnings.warn(
            "The method get_batch_features() is being deprecated. Please use the identical get_historical_features(). "
            "Feast 0.7 and onwards will not support get_batch_features().",
            DeprecationWarning,
        )
        return self.get_historical_features(
            feature_refs, entity_rows, compute_statistics, project
        )

    def get_historical_features(
        self,
        feature_refs: List[str],
        entity_rows: Union[pd.DataFrame, str],
        compute_statistics: bool = False,
        project: str = None,
    ) -> RetrievalJob:
        """
        Retrieves historical features from a Feast Serving deployment.

        Args:
            feature_refs: List of feature references that will be returned for each entity.
                Each feature reference should have the following format:
                "feature_set:feature" where "feature_set" & "feature" refer to
                the feature and feature set names respectively.
                Only the feature name is required.
            entity_rows (Union[pd.DataFrame, str]):
                Pandas dataframe containing entities and a 'datetime' column.
                Each entity in a feature set must be present as a column in this
                dataframe. The datetime column must contain timestamps in
                datetime64 format.
            compute_statistics (bool):
                Indicates whether Feast should compute statistics over the retrieved dataset.
            project: Specifies the project which contain the FeatureSets
                which the requested features belong to.

        Returns:
            feast.job.RetrievalJob:
                Returns a retrival job object that can be used to monitor retrieval
                progress asynchronously, and can be used to materialize the
                results.

        Examples:
            >>> from feast import Client
            >>> from datetime import datetime
            >>>
            >>> feast_client = Client(core_url="localhost:6565", serving_url="localhost:6566")
            >>> feature_refs = ["my_project/bookings_7d", "booking_14d"]
            >>> entity_rows = pd.DataFrame(
            >>>         {
            >>>            "datetime": [pd.datetime.now() for _ in range(3)],
            >>>            "customer": [1001, 1002, 1003],
            >>>         }
            >>>     )
            >>> feature_retrieval_job = feast_client.get_historical_features(
            >>>     feature_refs, entity_rows, project="my_project")
            >>> df = feature_retrieval_job.to_dataframe()
            >>> print(df)
        """

        # Retrieve serving information to determine store type and
        # staging location
        serving_info = self._serving_service.GetFeastServingInfo(
            GetFeastServingInfoRequest(),
            timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
            metadata=self._get_grpc_metadata(),
        )  # type: GetFeastServingInfoResponse

        if serving_info.type != FeastServingType.FEAST_SERVING_TYPE_BATCH:
            raise Exception(
                f'You are connected to a store "{self.serving_url}" which '
                f"does not support batch retrieval "
            )

        if isinstance(entity_rows, pd.DataFrame):
            # Pandas DataFrame detected

            # Remove timezone from datetime column
            if isinstance(
                entity_rows["datetime"].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype
            ):
                entity_rows["datetime"] = pd.DatetimeIndex(
                    entity_rows["datetime"]
                ).tz_localize(None)
        elif isinstance(entity_rows, str):
            # String based source
            if not entity_rows.endswith((".avro", "*")):
                raise Exception(
                    "Only .avro and wildcard paths are accepted as entity_rows"
                )
        else:
            raise Exception(
                f"Only pandas.DataFrame and str types are allowed"
                f" as entity_rows, but got {type(entity_rows)}."
            )

        # Export and upload entity row DataFrame to staging location
        # provided by Feast
        staged_files = export_source_to_staging_location(
            entity_rows, serving_info.job_staging_location
        )  # type: List[str]
        request = GetBatchFeaturesRequest(
            features=_build_feature_references(
                feature_ref_strs=feature_refs,
                project=project if project is not None else self.project,
            ),
            dataset_source=DatasetSource(
                file_source=DatasetSource.FileSource(
                    file_uris=staged_files, data_format=DataFormat.DATA_FORMAT_AVRO
                )
            ),
            compute_statistics=compute_statistics,
        )

        # Retrieve Feast Job object to manage life cycle of retrieval
        try:
            response = self._serving_service.GetBatchFeatures(
                request, metadata=self._get_grpc_metadata()
            )
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

        return RetrievalJob(
            response.job,
            self._serving_service,
            auth_metadata_plugin=self._auth_metadata,
        )

    def get_online_features(
        self,
        feature_refs: List[str],
        entity_rows: List[Union[GetOnlineFeaturesRequest.EntityRow, Dict[str, Any]]],
        project: Optional[str] = None,
        omit_entities: bool = False,
    ) -> OnlineResponse:
        """
        Retrieves the latest online feature data from Feast Serving

        Args:
            feature_refs: List of feature references that will be returned for each entity.
                Each feature reference should have the following format:
                "feature_set:feature" where "feature_set" & "feature" refer to
                the feature and feature set names respectively.
                Only the feature name is required.
            entity_rows: A list of dictionaries where each key is an entity and each value is
                feast.types.Value or Python native form.
            project: Optionally specify the the project override. If specified, uses given project for retrieval.
                Overrides the projects specified in Feature References if also are specified.
            omit_entities: If true will omit entity values in the returned feature data.
        Returns:
            GetOnlineFeaturesResponse containing the feature data in records.
            Each EntityRow provided will yield one record, which contains
            data fields with data value and field status metadata (if included).

        Examples:
            >>> from feast import Client
            >>>
            >>> feast_client = Client(core_url="localhost:6565", serving_url="localhost:6566")
            >>> feature_refs = ["daily_transactions"]
            >>> entity_rows = [{"customer_id": 0},{"customer_id": 1}]
            >>>
            >>> online_response = feast_client.get_online_features(
            >>>     feature_refs, entity_rows, project="my_project")
            >>> online_response_dict = online_response.to_dict()
            >>> print(online_response_dict)
            {'daily_transactions': [1.1,1.2], 'customer_id': [0,1]}
        """

        try:
            response = self._serving_service.GetOnlineFeatures(
                GetOnlineFeaturesRequest(
                    omit_entities_in_response=omit_entities,
                    features=_build_feature_references(feature_ref_strs=feature_refs),
                    entity_rows=_infer_online_entity_rows(entity_rows),
                    project=project if project is not None else self.project,
                ),
                metadata=self._get_grpc_metadata(),
            )
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

        response = OnlineResponse(response)
        return response

    def ingest(
        self,
        feature_set: Union[str, FeatureSet],
        source: Union[pd.DataFrame, str],
        chunk_size: int = 10000,
        max_workers: int = max(CPU_COUNT - 1, 1),
        disable_progress_bar: bool = False,
        timeout: int = KAFKA_CHUNK_PRODUCTION_TIMEOUT,
    ) -> str:
        """
        Loads feature data into Feast for a specific feature set.

        Args:
            feature_set (typing.Union[str, feast.feature_set.FeatureSet]):
                Feature set object or the string name of the feature set

            source (typing.Union[pd.DataFrame, str]):
                Either a file path or Pandas Dataframe to ingest into Feast
                Files that are currently supported:
                    * parquet
                    * csv
                    * json

            chunk_size (int):
                Amount of rows to load and ingest at a time.

            max_workers (int):
                Number of worker processes to use to encode values.

            disable_progress_bar (bool):
                Disable printing of progress statistics.

            timeout (int):
                Timeout in seconds to wait for completion.

        Returns:
            str:
                ingestion id for this dataset

        Examples:
            >>> from feast import Client
            >>>
            >>> client = Client(core_url="localhost:6565")
            >>> fs_df = pd.DataFrame(
            >>>         {
            >>>            "datetime": [pd.datetime.now()],
            >>>            "driver": [1001],
            >>>            "rating": [4.3],
            >>>         }
            >>>     )
            >>> client.set_project("project1")
            >>> client.ingest("driver", fs_df)
            >>>
            >>> driver_fs = client.get_feature_set(name="driver", project="project1")
            >>> client.ingest(driver_fs, fs_df)
        """

        if isinstance(feature_set, FeatureSet):
            name = feature_set.name
            project = feature_set.project
        elif isinstance(feature_set, str):
            if self.project is not None:
                project = self.project
            else:
                project = "default"
            name = feature_set
        else:
            raise Exception("Feature set name must be provided")

        # Read table and get row count
        dir_path, dest_path = _read_table_from_source(source, chunk_size, max_workers)

        pq_file = pq.ParquetFile(dest_path)

        row_count = pq_file.metadata.num_rows

        current_time = time.time()

        print("Waiting for feature set to be ready for ingestion...")
        while True:
            if timeout is not None and time.time() - current_time >= timeout:
                raise TimeoutError("Timed out waiting for feature set to be ready")
            fetched_feature_set: Optional[FeatureSet] = self.get_feature_set(
                name, project
            )
            if (
                fetched_feature_set is not None
                and fetched_feature_set.status == FeatureSetStatus.STATUS_READY
            ):
                feature_set = fetched_feature_set
                break
            time.sleep(3)

        if timeout is not None:
            timeout = timeout - int(time.time() - current_time)

        try:
            # Kafka configs
            brokers = feature_set.get_kafka_source_brokers()
            topic = feature_set.get_kafka_source_topic()
            producer = get_producer(brokers, row_count, disable_progress_bar)

            # Loop optimization declarations
            produce = producer.produce
            flush = producer.flush
            ingestion_id = _generate_ingestion_id(feature_set)

            # Transform and push data to Kafka
            if feature_set.source.source_type == "Kafka":
                for chunk in get_feature_row_chunks(
                    file=dest_path,
                    row_groups=list(range(pq_file.num_row_groups)),
                    fs=feature_set,
                    ingestion_id=ingestion_id,
                    max_workers=max_workers,
                ):

                    # Push FeatureRow one chunk at a time to kafka
                    for serialized_row in chunk:
                        produce(topic=topic, value=serialized_row)

                    # Force a flush after each chunk
                    flush(timeout=timeout)

                    # Remove chunk from memory
                    del chunk

            else:
                raise Exception(
                    f"Could not determine source type for feature set "
                    f'"{feature_set.name}" with source type '
                    f'"{feature_set.source.source_type}"'
                )

            # Print ingestion statistics
            producer.print_results()
        finally:
            # Remove parquet file(s) that were created earlier
            print("Removing temporary file(s)...")
            shutil.rmtree(dir_path)

        return ingestion_id

    def get_statistics(
        self,
        feature_set_id: str,
        store: str,
        features: List[str] = [],
        ingestion_ids: Optional[List[str]] = None,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        force_refresh: bool = False,
        project: Optional[str] = None,
    ) -> statistics_pb2.DatasetFeatureStatisticsList:
        """
        Retrieves the feature featureStatistics computed over the data in the batch
        stores.

        Args:
            feature_set_id: Feature set id to retrieve batch featureStatistics for. If project
                is not provided, the default ("default") will be used.
            store: Name of the store to retrieve feature featureStatistics over. This
                store must be a historical store.
            features: Optional list of feature names to filter from the results.
            ingestion_ids: Optional list of dataset Ids by which to filter data
                before retrieving featureStatistics. Cannot be used with start_date
                and end_date.
                If multiple dataset ids are provided, unaggregatable featureStatistics
                will be dropped.
            start_date: Optional start date over which to filter statistical data.
                Data from this date will be included.
                Cannot be used with dataset_ids. If the provided period spans
                multiple days, unaggregatable featureStatistics will be dropped.
            end_date: Optional end date over which to filter statistical data.
                Data from this data will not be included.
                Cannot be used with dataset_ids. If the provided period spans
                multiple days, unaggregatable featureStatistics will be dropped.
            force_refresh: Setting this flag to true will force a recalculation
                of featureStatistics and overwrite results currently in the cache, if any.
            project: Manual override for default project.

        Returns:
           Returns a tensorflow DatasetFeatureStatisticsList containing TFDV featureStatistics.
        """

        if ingestion_ids is not None and (
            start_date is not None or end_date is not None
        ):
            raise ValueError(
                "Only one of dataset_id or [start_date, end_date] can be provided."
            )

        if project != "" and "/" not in feature_set_id:
            feature_set_id = f"{project}/{feature_set_id}"

        request = GetFeatureStatisticsRequest(
            feature_set_id=feature_set_id,
            features=features,
            store=store,
            force_refresh=force_refresh,
        )
        if ingestion_ids is not None:
            request.ingestion_ids.extend(ingestion_ids)
        else:
            if start_date is not None:
                request.start_date.CopyFrom(
                    Timestamp(seconds=int(start_date.timestamp()))
                )
            if end_date is not None:
                request.end_date.CopyFrom(Timestamp(seconds=int(end_date.timestamp())))

        return self._core_service.GetFeatureStatistics(
            request
        ).dataset_feature_statistics_list

    def _get_grpc_metadata(self):
        """
        Returns a metadata tuple to attach to gRPC requests. This is primarily
        used when authentication is enabled but SSL/TLS is disabled.

        Returns: Tuple of metadata to attach to each gRPC call
        """
        if self._config.getboolean(CONFIG_ENABLE_AUTH_KEY) and self._auth_metadata:
            return self._auth_metadata.get_signed_meta()
        return ()


def _infer_online_entity_rows(
    entity_rows: List[Union[GetOnlineFeaturesRequest.EntityRow, Dict[str, Any]]],
) -> List[GetOnlineFeaturesRequest.EntityRow]:
    """
    Builds a list of EntityRow protos from Python native type format passed by user.

    Args:
        entity_rows: A list of dictionaries where each key is an entity and each value is
            feast.types.Value or Python native form.

    Returns:
        A list of EntityRow protos parsed from args.
    """

    # Maintain backward compatibility with users providing EntityRow Proto
    if entity_rows and isinstance(entity_rows[0], GetOnlineFeaturesRequest.EntityRow):
        warnings.warn(
            "entity_rows parameter will only be accepting Dict format from Feast v0.7 onwards",
            DeprecationWarning,
        )
        entity_rows_proto = cast(
            List[Union[GetOnlineFeaturesRequest.EntityRow]], entity_rows
        )
        return entity_rows_proto

    entity_rows_dicts = cast(List[Dict[str, Any]], entity_rows)
    entity_row_list = []
    entity_type_map = dict()

    for entity in entity_rows_dicts:
        fields = {}
        for key, value in entity.items():
            # Allow for feast.types.Value
            if isinstance(value, Value):
                proto_value = value
            else:
                # Infer the specific type for this row
                current_dtype = python_type_to_feast_value_type(name=key, value=value)

                if key not in entity_type_map:
                    entity_type_map[key] = current_dtype
                else:
                    if current_dtype != entity_type_map[key]:
                        raise TypeError(
                            f"Input entity {key} has mixed types, {current_dtype} and {entity_type_map[key]}. That is not allowed. "
                        )
                proto_value = _python_value_to_proto_value(current_dtype, value)
            fields[key] = proto_value
        entity_row_list.append(GetOnlineFeaturesRequest.EntityRow(fields=fields))
    return entity_row_list


def _build_feature_references(
    feature_ref_strs: List[str], project: Optional[str] = None
) -> List[FeatureReference]:
    """
    Builds a list of FeatureReference protos from string feature set references

    Args:
        feature_ref_strs: List of string feature references
        project: Optionally specifies the project in the parsed feature references.

    Returns:
        A list of FeatureReference protos parsed from args.
    """
    feature_refs = [FeatureRef.from_str(ref_str) for ref_str in feature_ref_strs]
    feature_ref_protos = [ref.to_proto() for ref in feature_refs]
    # apply project if specified
    if project is not None:
        for feature_ref_proto in feature_ref_protos:
            feature_ref_proto.project = project
    return feature_ref_protos


def _generate_ingestion_id(feature_set: FeatureSet) -> str:
    """
    Generates a UUID from the feature set name, version, and the current time.

    Args:
        feature_set: Feature set of the dataset to be ingested.

    Returns:
        UUID unique to current time and the feature set provided.
    """
    uuid_str = f"{feature_set.name}_{int(time.time())}"
    return str(uuid.uuid3(uuid.NAMESPACE_DNS, uuid_str))


def _read_table_from_source(
    source: Union[pd.DataFrame, str], chunk_size: int, max_workers: int
) -> Tuple[str, str]:
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
        Tuple[str, str]:
            Tuple containing parent directory path and destination path to
            parquet file.
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

    # Remove table from memory
    del table

    return dir_path, dest_path
