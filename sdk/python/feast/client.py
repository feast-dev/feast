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
import sys
from collections import OrderedDict
from typing import Dict, Union
from typing import List
import grpc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from feast.core.CoreService_pb2 import (
    GetFeastCoreVersionRequest,
    ListFeatureSetsResponse,
    ApplyFeatureSetRequest,
    ListFeatureSetsRequest,
    ApplyFeatureSetResponse,
    GetFeatureSetRequest,
    GetFeatureSetResponse,
)
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.exceptions import format_grpc_exception
from feast.feature_set import FeatureSet, Entity
from feast.job import Job
from feast.loaders.file import export_dataframe_to_staging_location
from feast.loaders.ingest import ingest_table_to_kafka
from feast.serving.ServingService_pb2 import GetFeastServingInfoResponse
from feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequest,
    GetBatchFeaturesRequest,
    GetFeastServingInfoRequest,
    GetOnlineFeaturesResponse,
    DatasetSource,
    DataFormat,
    FeatureSetRequest,
    FeastServingType,
)
from feast.serving.ServingService_pb2_grpc import ServingServiceStub

_logger = logging.getLogger(__name__)

GRPC_CONNECTION_TIMEOUT_DEFAULT = 3  # type: int
GRPC_CONNECTION_TIMEOUT_APPLY = 600  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str
BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS = 300
CPU_COUNT = os.cpu_count()  # type: int


class Client:
    """
    Feast Client: Used for creating, managing, and retrieving features.
    """

    def __init__(
        self, core_url: str = None, serving_url: str = None, verbose: bool = False
    ):
        """
        The Feast Client should be initialized with at least one service url

        Args:
            core_url: Feast Core URL. Used to manage features
            serving_url: Feast Serving URL. Used to retrieve features
            verbose: Enable verbose logging
        """
        self._core_url = core_url
        self._serving_url = serving_url
        self._verbose = verbose
        self.__core_channel: grpc.Channel = None
        self.__serving_channel: grpc.Channel = None
        self._core_service_stub: CoreServiceStub = None
        self._serving_service_stub: ServingServiceStub = None

    @property
    def core_url(self) -> str:
        """
        Retrieve Feast Core URL
        """

        if self._core_url is not None:
            return self._core_url
        if os.getenv(FEAST_CORE_URL_ENV_KEY) is not None:
            return os.getenv(FEAST_CORE_URL_ENV_KEY)
        return ""

    @core_url.setter
    def core_url(self, value: str):
        """
        Set the Feast Core URL

        Returns:
            Feast Core URL string
        """
        self._core_url = value

    @property
    def serving_url(self) -> str:
        """
        Retrieve Serving Core URL
        """
        if self._serving_url is not None:
            return self._serving_url
        if os.getenv(FEAST_SERVING_URL_ENV_KEY) is not None:
            return os.getenv(FEAST_SERVING_URL_ENV_KEY)
        return ""

    @serving_url.setter
    def serving_url(self, value: str):
        """
        Set the Feast Serving URL

        Returns:
            Feast Serving URL string
        """
        self._serving_url = value

    def version(self):
        """
        Returns version information from Feast Core and Feast Serving
        """
        result = {}

        if self.serving_url:
            self._connect_serving()
            serving_version = self._serving_service_stub.GetFeastServingInfo(
                GetFeastServingInfoRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            ).version
            result["serving"] = {"url": self.serving_url, "version": serving_version}

        if self.core_url:
            self._connect_core()
            core_version = self._core_service_stub.GetFeastCoreVersion(
                GetFeastCoreVersionRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
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
            self.__core_channel = grpc.insecure_channel(self.core_url)

        try:
            grpc.channel_ready_future(self.__core_channel).result(
                timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
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
            self.__serving_channel = grpc.insecure_channel(self.serving_url)

        try:
            grpc.channel_ready_future(self.__serving_channel).result(
                timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            )
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                f"Connection timed out while attempting to connect to Feast "
                f"Serving gRPC server {self.serving_url} "
            )
        else:
            self._serving_service_stub = ServingServiceStub(self.__serving_channel)

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
        feature_set._client = self

        feature_set.is_valid()

        # Convert the feature set to a request and send to Feast Core
        apply_fs_response = self._core_service_stub.ApplyFeatureSet(
            ApplyFeatureSetRequest(feature_set=feature_set.to_proto()),
            timeout=GRPC_CONNECTION_TIMEOUT_APPLY,
        )  # type: ApplyFeatureSetResponse

        # Extract the returned feature set
        applied_fs = FeatureSet.from_proto(apply_fs_response.feature_set)

        # If the feature set has changed, update the local copy
        if apply_fs_response.status == ApplyFeatureSetResponse.Status.CREATED:
            print(
                f'Feature set updated/created: "{applied_fs.name}:{applied_fs.version}"'
            )

        # If no change has been applied, do nothing
        if apply_fs_response.status == ApplyFeatureSetResponse.Status.NO_CHANGE:
            print(f"No change detected or applied: {feature_set.name}")

        # Deep copy from the returned feature set to the local feature set
        feature_set.update_from_feature_set(applied_fs)

    def list_feature_sets(self) -> List[FeatureSet]:
        """
        Retrieve a list of feature sets from Feast Core

        Returns:
            List of feature sets
        """
        self._connect_core()

        # Get latest feature sets from Feast Core
        feature_set_protos = self._core_service_stub.ListFeatureSets(
            ListFeatureSetsRequest()
        )  # type: ListFeatureSetsResponse

        # Extract feature sets and return
        feature_sets = []
        for feature_set_proto in feature_set_protos.feature_sets:
            feature_set = FeatureSet.from_proto(feature_set_proto)
            feature_set._client = self
            feature_sets.append(feature_set)
        return feature_sets

    def get_feature_set(
        self, name: str, version: int = None
    ) -> Union[FeatureSet, None]:
        """
        Retrieves a feature set. If no version is specified then the latest
        version will be returned.

        Args:
            name: Name of feature set
            version: Version of feature set

        Returns:
            Returns either the specified feature set, or raises an exception if
            none is found
        """
        self._connect_core()

        if version is None:
            version = 0
        get_feature_set_response = self._core_service_stub.GetFeatureSet(
            GetFeatureSetRequest(name=name.strip(), version=int(version))
        )  # type: GetFeatureSetResponse
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
        self, feature_ids: List[str], entity_rows: pd.DataFrame
    ) -> Job:
        """
        Retrieves historical features from a Feast Serving deployment.

        Args:
            feature_ids: List of feature ids that will be returned for each
                entity. Each feature id should have the following format
                "feature_set_name:version:feature_name".
            entity_rows: Pandas dataframe containing entities and a 'datetime'
                column. Each entity in a feature set must be present as a column
                in this dataframe. The datetime column must

        Returns:
            Returns a job object that can be used to monitor retrieval progress
            asynchronously, and can be used to materialize the results

        Examples:
            >>> from feast import Client
            >>> from datetime import datetime
            >>>
            >>> feast_client = Client(core_url="localhost:6565", serving_url="localhost:6566")
            >>> feature_ids = ["customer:1:bookings_7d"]
            >>> entity_rows = pd.DataFrame(
            >>>         {
            >>>            "datetime": [pd.datetime.now() for _ in range(3)],
            >>>            "customer": [1001, 1002, 1003],
            >>>         }
            >>>     )
            >>> feature_retrieval_job = feast_client.get_batch_features(feature_ids, entity_rows)
            >>> df = feature_retrieval_job.to_dataframe()
            >>> print(df)
        """

        self._connect_serving()

        fs_request = _build_feature_set_request(feature_ids)

        # Validate entity rows based on entities in Feast Core
        self._validate_entity_rows_for_batch_retrieval(entity_rows, fs_request)

        # Remove timezone from datetime column
        if isinstance(
            entity_rows["datetime"].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype
        ):
            entity_rows["datetime"] = pd.DatetimeIndex(
                entity_rows["datetime"]
            ).tz_localize(None)

        # Retrieve serving information to determine store type and
        # staging location
        serving_info = self._serving_service_stub.GetFeastServingInfo(
            GetFeastServingInfoRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
        )  # type: GetFeastServingInfoResponse

        if serving_info.type != FeastServingType.FEAST_SERVING_TYPE_BATCH:
            raise Exception(
                f'You are connected to a store "{self._serving_url}" which '
                f"does not support batch retrieval "
            )

        # Export and upload entity row dataframe to staging location
        # provided by Feast
        staged_file = export_dataframe_to_staging_location(
            entity_rows, serving_info.job_staging_location
        )  # type: str

        request = GetBatchFeaturesRequest(
            feature_sets=fs_request,
            dataset_source=DatasetSource(
                file_source=DatasetSource.FileSource(
                    file_uris=[staged_file], data_format=DataFormat.DATA_FORMAT_AVRO
                )
            ),
        )

        # Retrieve Feast Job object to manage life cycle of retrieval
        response = self._serving_service_stub.GetBatchFeatures(request)
        return Job(response.job, self._serving_service_stub)

    def _validate_entity_rows_for_batch_retrieval(
        self, entity_rows, feature_sets_request
    ):
        """
        Validate whether an entity_row dataframe contains the correct
        information for batch retrieval

        Args:
            entity_rows: Pandas dataframe containing entities and datetime
                column. Each entity in a feature set must be present as a
                column in this dataframe.
            feature_sets_request: Feature sets that will be requested
        """

        # Ensure datetime column exists
        if "datetime" not in entity_rows.columns:
            raise ValueError(
                f'Entity rows does not contain "datetime" column in columns '
                f"{entity_rows.columns}"
            )

        # Validate dataframe columns based on feature set entities
        for feature_set in feature_sets_request:
            fs = self.get_feature_set(
                name=feature_set.name, version=feature_set.version
            )
            if fs is None:
                raise ValueError(
                    f'Feature set "{feature_set.name}:{feature_set.version}" '
                    f"could not be found"
                )
            for entity_type in fs.entities:
                if entity_type.name not in entity_rows.columns:
                    raise ValueError(
                        f'Dataframe does not contain entity "{entity_type.name}"'
                        f' column in columns "{entity_rows.columns}"'
                    )

    def get_online_features(
        self,
        feature_ids: List[str],
        entity_rows: List[GetOnlineFeaturesRequest.EntityRow],
    ) -> GetOnlineFeaturesResponse:
        """
        Retrieves the latest online feature data from Feast Serving

        Args:
            feature_ids: List of feature Ids in the following format
                [feature_set_name]:[version]:[feature_name]
                example:
                    ["feature_set_1:6:my_feature_1",
                    "feature_set_1:6:my_feature_2",]
            entity_rows: List of GetFeaturesRequest.EntityRow where each row
                contains entities. Timestamp should not be set for online
                retrieval. All entity types within a feature

        Returns:
            Returns a list of maps where each item in the list contains the
            latest feature values for the provided entities
        """

        self._connect_serving()

        return self._serving_service_stub.GetOnlineFeatures(
            GetOnlineFeaturesRequest(
                feature_sets=_build_feature_set_request(feature_ids),
                entity_rows=entity_rows,
            )
        )  # type: GetOnlineFeaturesResponse

    def ingest(
        self,
        feature_set: Union[str, FeatureSet],
        source: Union[pd.DataFrame, str],
        version: int = None,
        force_update: bool = False,
        max_workers: int = CPU_COUNT,
        disable_progress_bar: bool = False,
        chunk_size: int = 5000,
        timeout: int = None,
    ):
        """
        Loads feature data into Feast for a specific feature set.

        Args:
            feature_set: Name of feature set or a feature set object
            source: Either a file path or Pandas Dataframe to ingest into Feast
                Files that are currently supported:
                * parquet
                * csv
                * json
            version: Feature set version
            force_update: Automatically update feature set based on source data
                prior to ingesting. This will also register changes to Feast
            max_workers: Number of worker processes to use to encode values
            disable_progress_bar: Disable printing of progress statistics
            chunk_size: Maximum amount of rows to load into memory and ingest at
                a time
            timeout: Seconds to wait before ingestion times out
        """
        if isinstance(feature_set, FeatureSet):
            name = feature_set.name
            if version is None:
                version = feature_set.version
        elif isinstance(feature_set, str):
            name = feature_set
        else:
            raise Exception(f"Feature set name must be provided")

        table = _read_table_from_source(source)

        # Update the feature set based on DataFrame schema
        if force_update:
            # Use a small as reference DataFrame to infer fields
            ref_df = table.to_batches(max_chunksize=20)[0].to_pandas()

            feature_set.infer_fields_from_df(
                ref_df, discard_unused_fields=True, replace_existing_features=True
            )
            self.apply(feature_set)

        feature_set = self.get_feature_set(name, version)

        if feature_set.source.source_type == "Kafka":
            ingest_table_to_kafka(
                feature_set=feature_set,
                table=table,
                max_workers=max_workers,
                disable_pbar=disable_progress_bar,
                chunk_size=chunk_size,
                timeout=timeout,
            )
        else:
            raise Exception(
                f"Could not determine source type for feature set "
                f'"{feature_set.name}" with source type '
                f'"{feature_set.source.source_type}"'
            )


def _build_feature_set_request(feature_ids: List[str]) -> List[FeatureSetRequest]:
    """
    Builds a list of FeatureSet objects from feature set ids in order to
    retrieve feature data from Feast Serving

    Args:
        feature_ids: List of feature ids
            ("feature_set_name:version:feature_name")
    """
    feature_set_request = dict()  # type: Dict[str, FeatureSetRequest]
    for feature_id in feature_ids:
        fid_parts = feature_id.split(":")
        if len(fid_parts) == 3:
            feature_set, version, feature = fid_parts
        else:
            raise ValueError(
                f"Could not parse feature id ${feature_id}, needs 2 colons"
            )

        if feature_set not in feature_set_request:
            feature_set_request[feature_set] = FeatureSetRequest(
                name=feature_set, version=int(version)
            )
        feature_set_request[feature_set].feature_names.append(feature)
    return list(feature_set_request.values())


def _read_table_from_source(source: Union[pd.DataFrame, str]) -> pa.lib.Table:
    """
    Infers a data source type (path or Pandas Dataframe) and reads it in as
    a PyArrow Table.

    Args:
        source: Either a string path or Pandas Dataframe

    Returns:
        PyArrow table
    """
    # Pandas dataframe detected
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
    return table
