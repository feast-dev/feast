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


import logging
import os
import shutil
import tempfile
import time
import uuid
from collections import OrderedDict
from math import ceil
from typing import Dict, List, Optional, Tuple, Union

import grpc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from feast.config import Config
from feast.constants import (
    CONFIG_CORE_SECURE_KEY,
    CONFIG_CORE_URL_KEY,
    CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY,
    CONFIG_PROJECT_KEY,
    CONFIG_SERVING_SECURE_KEY,
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
    ListFeatureSetsRequest,
    ListFeatureSetsResponse,
    ListIngestionJobsRequest,
    ListProjectsRequest,
    ListProjectsResponse,
    RestartIngestionJobRequest,
    StopIngestionJobRequest,
)
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.FeatureSet_pb2 import FeatureSetStatus
from feast.feature import FeatureRef
from feast.feature_set import Entity, FeatureSet, FeatureSetRef
from feast.job import IngestJob, RetrievalJob
from feast.loaders.abstract_producer import get_producer
from feast.loaders.file import export_source_to_staging_location
from feast.loaders.ingest import KAFKA_CHUNK_PRODUCTION_TIMEOUT, get_feature_row_chunks
from feast.serving.ServingService_pb2 import (
    DataFormat,
    DatasetSource,
    FeastServingType,
    FeatureReference,
    GetBatchFeaturesRequest,
    GetFeastServingInfoRequest,
    GetFeastServingInfoResponse,
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)
from feast.serving.ServingService_pb2_grpc import ServingServiceStub

_logger = logging.getLogger(__name__)

CPU_COUNT = os.cpu_count()  # type: int


class Client:
    """
    Feast Client: Used for creating, managing, and retrieving features.
    """

    def __init__(self, options: Optional[Dict[str, str]] = None, **kwargs):
        """
        The Feast Client should be initialized with at least one service url

        Args:
            core_url: Feast Core URL. Used to manage features
            serving_url: Feast Serving URL. Used to retrieve features
            project: Sets the active project. This field is optional.
            core_secure: Use client-side SSL/TLS for Core gRPC API
            serving_secure: Use client-side SSL/TLS for Serving gRPC API
            options: Configuration options to initialize client with
            **kwargs: Additional keyword arguments that will be used as
                configuration options along with "options"
        """

        if options is None:
            options = dict()
        self._config = Config(options={**options, **kwargs})

        self.__core_channel: grpc.Channel = None
        self.__serving_channel: grpc.Channel = None
        self._core_service_stub: CoreServiceStub = None
        self._serving_service_stub: ServingServiceStub = None

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
        return self._config.getboolean(CONFIG_CORE_SECURE_KEY)

    @core_secure.setter
    def core_secure(self, value: bool):
        """
        Set the Feast Core client-side SSL/TLS setting

        Args:
            value: True to enable client-side SSL/TLS
        """
        self._config.set(CONFIG_CORE_SECURE_KEY, value)

    @property
    def serving_secure(self) -> bool:
        """
        Retrieve Feast Serving client-side SSL/TLS setting

        Returns:
            Whether client-side SSL/TLS is enabled
        """
        return self._config.getboolean(CONFIG_SERVING_SECURE_KEY)

    @serving_secure.setter
    def serving_secure(self, value: bool):
        """
        Set the Feast Serving client-side SSL/TLS setting

        Args:
            value: True to enable client-side SSL/TLS
        """
        self._config.set(CONFIG_SERVING_SECURE_KEY, value)

    def version(self):
        """
        Returns version information from Feast Core and Feast Serving
        """
        result = {}

        if self.serving_url:
            self._connect_serving()
            serving_version = self._serving_service_stub.GetFeastServingInfo(
                GetFeastServingInfoRequest(),
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
            ).version
            result["serving"] = {"url": self.serving_url, "version": serving_version}

        if self.core_url:
            self._connect_core()
            core_version = self._core_service_stub.GetFeastCoreVersion(
                GetFeastCoreVersionRequest(),
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
            ).version
            result["core"] = {"url": self.core_url, "version": core_version}

        return result

    def _connect_core(self, skip_if_connected: bool = True):
        """
        Connect to Core API

        Args:
            skip_if_connected: Do not attempt to connect if already connected
        """
        if skip_if_connected and self._core_service_stub:
            return

        if not self.core_url:
            raise ValueError("Please set Feast Core URL.")

        if self.__core_channel is None:
            if self.core_secure or self.core_url.endswith(":443"):
                self.__core_channel = grpc.secure_channel(
                    self.core_url, grpc.ssl_channel_credentials()
                )
            else:
                self.__core_channel = grpc.insecure_channel(self.core_url)

        try:
            grpc.channel_ready_future(self.__core_channel).result(
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY)
            )
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                f"Connection timed out while attempting to connect to Feast "
                f"Core gRPC server {self.core_url} "
            )
        else:
            self._core_service_stub = CoreServiceStub(self.__core_channel)

    def _connect_serving(self, skip_if_connected=True):
        """
        Connect to Serving API

        Args:
            skip_if_connected: Do not attempt to connect if already connected
        """

        if skip_if_connected and self._serving_service_stub:
            return

        if not self.serving_url:
            raise ValueError("Please set Feast Serving URL.")

        if self.__serving_channel is None:
            if self.serving_secure or self.serving_url.endswith(":443"):
                self.__serving_channel = grpc.secure_channel(
                    self.serving_url, grpc.ssl_channel_credentials()
                )
            else:
                self.__serving_channel = grpc.insecure_channel(self.serving_url)

        try:
            grpc.channel_ready_future(self.__serving_channel).result(
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY)
            )
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                f"Connection timed out while attempting to connect to Feast "
                f"Serving gRPC server {self.serving_url} "
            )
        else:
            self._serving_service_stub = ServingServiceStub(self.__serving_channel)

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
        self._connect_core()
        response = self._core_service_stub.ListProjects(
            ListProjectsRequest(),
            timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
        )  # type: ListProjectsResponse
        return list(response.projects)

    def create_project(self, project: str):
        """
        Creates a Feast project

        Args:
            project: Name of project
        """

        self._connect_core()
        self._core_service_stub.CreateProject(
            CreateProjectRequest(name=project),
            timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
        )  # type: CreateProjectResponse

    def archive_project(self, project):
        """
        Archives a project. Project will still continue to function for
        ingestion and retrieval, but will be in a read-only state. It will
        also not be visible from the Core API for management purposes.

        Args:
            project: Name of project to archive
        """

        self._connect_core()
        try:
            self._core_service_stub.ArchiveProject(
                ArchiveProjectRequest(name=project),
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
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
        self._connect_core()

        feature_set.is_valid()
        feature_set_proto = feature_set.to_proto()
        if len(feature_set_proto.spec.project) == 0:
            if self.project is not None:
                feature_set_proto.spec.project = self.project

        # Convert the feature set to a request and send to Feast Core
        try:
            apply_fs_response = self._core_service_stub.ApplyFeatureSet(
                ApplyFeatureSetRequest(feature_set=feature_set_proto),
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
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
        self, project: str = None, name: str = None,
    ) -> List[FeatureSet]:
        """
        Retrieve a list of feature sets from Feast Core

        Args:
            project: Filter feature sets based on project name
            name: Filter feature sets based on feature set name

        Returns:
            List of feature sets
        """
        self._connect_core()

        if project is None:
            if self.project is not None:
                project = self.project
            else:
                project = "*"

        if name is None:
            name = "*"

        filter = ListFeatureSetsRequest.Filter(project=project, feature_set_name=name)

        # Get latest feature sets from Feast Core
        feature_set_protos = self._core_service_stub.ListFeatureSets(
            ListFeatureSetsRequest(filter=filter)
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
        self._connect_core()

        if project is None:
            if self.project is not None:
                project = self.project
            else:
                raise ValueError("No project has been configured.")

        try:
            get_feature_set_response = self._core_service_stub.GetFeatureSet(
                GetFeatureSetRequest(project=project, name=name.strip())
            )  # type: GetFeatureSetResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())
        return FeatureSet.from_proto(get_feature_set_response.feature_set)

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
            >>> feature_retrieval_job = feast_client.get_batch_features(
            >>>     feature_refs, entity_rows, default_project="my_project")
            >>> df = feature_retrieval_job.to_dataframe()
            >>> print(df)
        """

        self._connect_serving()

        # Retrieve serving information to determine store type and
        # staging location
        serving_info = self._serving_service_stub.GetFeastServingInfo(
            GetFeastServingInfoRequest(),
            timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
        )  # type: GetFeastServingInfoResponse

        if serving_info.type != FeastServingType.FEAST_SERVING_TYPE_BATCH:
            raise Exception(
                f'You are connected to a store "{self.serving_url}" which '
                f"does not support batch retrieval"
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
        )

        # Retrieve Feast Job object to manage life cycle of retrieval
        try:
            response = self._serving_service_stub.GetBatchFeatures(request)
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

        return RetrievalJob(response.job, self._serving_service_stub)

    def get_online_features(
        self,
        feature_refs: List[str],
        entity_rows: List[GetOnlineFeaturesRequest.EntityRow],
        project: Optional[str] = None,
    ) -> GetOnlineFeaturesResponse:
        """
        Retrieves the latest online feature data from Feast Serving

        Args:
            feature_refs: List of feature references that will be returned for each entity.
                Each feature reference should have the following format:
                "feature_set:feature" where "feature_set" & "feature" refer to
                the feature and feature set names respectively.
                Only the feature name is required.
            entity_rows: List of GetFeaturesRequest.EntityRow where each row
                contains entities. Timestamp should not be set for online
                retrieval. All entity types within a feature
            project: Specifies the project which contain the FeatureSets
                which the requested features belong to.

        Returns:
            Returns a list of maps where each item in the list contains the
            latest feature values for the provided entities
        """
        self._connect_serving()

        try:
            response = self._serving_service_stub.GetOnlineFeatures(
                GetOnlineFeaturesRequest(
                    features=_build_feature_references(
                        feature_ref_strs=feature_refs,
                        project=project if project is not None else self.project,
                    ),
                    entity_rows=entity_rows,
                )
            )
            # collect entity row refs
            entity_refs = set()
            for entity_row in entity_rows:
                entity_refs.update(entity_row.fields.keys())

            strip_field_values = []
            for field_value in response.field_values:
                # strip the project part the string feature references returned from serving
                strip_fields = {}
                for ref_str, value in field_value.fields.items():
                    # find and ignore entities
                    if ref_str in entity_refs:
                        strip_fields[ref_str] = value
                    else:
                        strip_ref_str = repr(
                            FeatureRef.from_str(ref_str, ignore_project=True)
                        )
                        strip_fields[strip_ref_str] = value
                strip_field_values.append(
                    GetOnlineFeaturesResponse.FieldValues(fields=strip_fields)
                )

            del response.field_values[:]
            response.field_values.extend(strip_field_values)

        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

        return response

    def list_ingest_jobs(
        self,
        job_id: str = None,
        feature_set_ref: FeatureSetRef = None,
        store_name: str = None,
    ):
        """
        List the ingestion jobs currently registered in Feast, with optional filters.
        Provides detailed metadata about each ingestion job.

        Args:
            job_id: Select specific ingestion job with the given job_id
            feature_set_ref: Filter ingestion jobs by target feature set (via reference)
            store_name: Filter ingestion jobs by target feast store's name

        Returns:
            List of IngestJobs matching the given filters
        """
        self._connect_core()
        # construct list request
        feature_set_ref = None
        list_filter = ListIngestionJobsRequest.Filter(
            id=job_id, feature_set_reference=feature_set_ref, store_name=store_name,
        )
        request = ListIngestionJobsRequest(filter=list_filter)
        # make list request & unpack response
        response = self._core_service_stub.ListIngestionJobs(request)
        ingest_jobs = [
            IngestJob(proto, self._core_service_stub) for proto in response.jobs
        ]
        return ingest_jobs

    def restart_ingest_job(self, job: IngestJob):
        """
        Restart ingestion job currently registered in Feast.
        NOTE: Data might be lost during the restart for some job runners.
        Does not support stopping a job in a transitional (ie pending, suspending, aborting),
        terminal state (ie suspended or aborted) or unknown status

        Args:
            job: IngestJob to restart
        """
        self._connect_core()
        request = RestartIngestionJobRequest(id=job.id)
        try:
            self._core_service_stub.RestartIngestionJob(request)
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

    def stop_ingest_job(self, job: IngestJob):
        """
        Stop ingestion job currently resgistered in Feast
        Does nothing if the target job if already in a terminal state (ie suspended or aborted).
        Does not support stopping a job in a transitional (ie pending, suspending, aborting)
        or in a unknown status

        Args:
            job: IngestJob to restart
        """
        self._connect_core()
        request = StopIngestionJobRequest(id=job.id)
        try:
            self._core_service_stub.StopIngestionJob(request)
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

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
        """

        if isinstance(feature_set, FeatureSet):
            name = feature_set.name
        elif isinstance(feature_set, str):
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
            feature_set = self.get_feature_set(name)
            if (
                feature_set is not None
                and feature_set.status == FeatureSetStatus.STATUS_READY
            ):
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
