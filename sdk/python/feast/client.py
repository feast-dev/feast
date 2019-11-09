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


import tempfile
import shutil
from datetime import datetime
import os
from collections import OrderedDict
from typing import Dict, Union
from typing import List
import grpc
import pandas as pd
from google.cloud import storage
from pandavro import to_avro
from feast.exceptions import format_grpc_exception
from feast.core.CoreService_pb2 import (
    GetFeastCoreVersionRequest,
    GetFeatureSetsResponse,
    ApplyFeatureSetRequest,
    GetFeatureSetsRequest,
    ApplyFeatureSetResponse,
)
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.feature_set import FeatureSet, Entity
from feast.job import Job
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
from feast.serving.ServingService_pb2 import GetFeastServingInfoResponse
from urllib.parse import urlparse
import uuid
import numpy as np
import sys

from feast.utils.loaders import export_dataframe_to_staging_location

GRPC_CONNECTION_TIMEOUT_DEFAULT = 3  # type: int
GRPC_CONNECTION_TIMEOUT_APPLY = 300  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str
BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS = 300


class Client:
    def __init__(
        self, core_url: str = None, serving_url: str = None, verbose: bool = False
    ):
        self._core_url = core_url
        self._serving_url = serving_url
        self._verbose = verbose
        self.__core_channel: grpc.Channel = None
        self.__serving_channel: grpc.Channel = None
        self._core_service_stub: CoreServiceStub = None
        self._serving_service_stub: ServingServiceStub = None

    @property
    def core_url(self) -> str:
        if self._core_url is not None:
            return self._core_url
        if os.getenv(FEAST_CORE_URL_ENV_KEY) is not None:
            return os.getenv(FEAST_CORE_URL_ENV_KEY)
        return ""

    @core_url.setter
    def core_url(self, value: str):
        self._core_url = value

    @property
    def serving_url(self) -> str:
        if self._serving_url is not None:
            return self._serving_url
        if os.getenv(FEAST_SERVING_URL_ENV_KEY) is not None:
            return os.getenv(FEAST_SERVING_URL_ENV_KEY)
        return ""

    @serving_url.setter
    def serving_url(self, value: str):
        self._serving_url = value

    def version(self):
        """
        Returns version information from Feast Core and Feast Serving
        :return: Dictionary containing Core and Serving versions and status
        """

        self._connect_core()
        self._connect_serving()

        core_version = ""
        serving_version = ""
        core_status = "not connected"
        serving_status = "not connected"

        try:
            core_version = self._core_service_stub.GetFeastCoreVersion(
                GetFeastCoreVersionRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            ).version
            core_status = "connected"
        except grpc.RpcError as e:
            print(format_grpc_exception("GetFeastCoreVersion", e.code(), e.details()))

        try:
            serving_version = self._serving_service_stub.GetFeastServingInfo(
                GetFeastServingInfoRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            ).version
            serving_status = "connected"
        except grpc.RpcError as e:
            print(format_grpc_exception("GetFeastServingInfo", e.code(), e.details()))

        return {
            "core": {
                "url": self.core_url,
                "version": core_version,
                "status": core_status,
            },
            "serving": {
                "url": self.serving_url,
                "version": serving_version,
                "status": serving_status,
            },
        }

    def _connect_core(self, skip_if_connected=True):
        """
        Connect to Core API
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
            print(
                f"Connection timed out while attempting to connect to Feast Core gRPC server {self.core_url}"
            )
            sys.exit(1)
        else:
            self._core_service_stub = CoreServiceStub(self.__core_channel)

    def _connect_serving(self, skip_if_connected=True):
        """
        Connect to Serving API
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
            print(
                f"Connection timed out while attempting to connect to Feast Serving gRPC server {self.serving_url} "
            )
            sys.exit(1)
        else:
            self._serving_service_stub = ServingServiceStub(self.__serving_channel)

    def apply(self, feature_sets: Union[List[FeatureSet], FeatureSet]):
        """
        Idempotently registers feature set(s) with Feast Core. Either a single feature set or a list can be provided.
        :param feature_sets: Union[List[FeatureSet], FeatureSet]
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
        self._connect_core()
        feature_set._client = self

        try:
            apply_fs_response = self._core_service_stub.ApplyFeatureSet(
                ApplyFeatureSetRequest(feature_set=feature_set.to_proto()),
                timeout=GRPC_CONNECTION_TIMEOUT_APPLY,
            )  # type: ApplyFeatureSetResponse
            applied_fs = FeatureSet.from_proto(apply_fs_response.feature_set)

            if apply_fs_response.status == ApplyFeatureSetResponse.Status.CREATED:
                print(f'Feature set updated/created: "{applied_fs.name}:{applied_fs.version}".')
                feature_set._update_from_feature_set(applied_fs, is_dirty=False)
                return
            if apply_fs_response.status == ApplyFeatureSetResponse.Status.NO_CHANGE:
                print(f'No change detected in feature set {feature_set.name}')
                return
        except grpc.RpcError as e:
            print(format_grpc_exception("ApplyFeatureSet", e.code(), e.details()))

    def list_feature_sets(self) -> List[FeatureSet]:
        """
        Retrieve a list of feature sets from Feast Core
        :return: Returns a list of feature sets
        """
        self._connect_core()

        try:
            # Get latest feature sets from Feast Core
            feature_set_protos = self._core_service_stub.GetFeatureSets(
                GetFeatureSetsRequest()
            )  # type: GetFeatureSetsResponse
        except grpc.RpcError as e:
            print(format_grpc_exception("GetFeatureSets", e.code(), e.details()))

        # Store list of feature sets
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
        Retrieve a single feature set from Feast Core
        :param name: Name of feature set
        :param version: Version of feature set (int)
        :return: Returns a single feature set
        """
        self._connect_core()

        # TODO: Version should only be integer or unset. Core should handle 'latest'
        if version is None:
            version = "latest"
        elif isinstance(version, int):
            version = str(version)
        else:
            raise ValueError(f"Version {version} is not of type integer.")

        try:
            get_feature_set_response = self._core_service_stub.GetFeatureSets(
                GetFeatureSetsRequest(
                    filter=GetFeatureSetsRequest.Filter(
                        feature_set_name=name.strip(), feature_set_version=version
                    )
                )
            )  # type: GetFeatureSetsResponse
        except grpc.RpcError as e:
            print(format_grpc_exception("GetFeatureSets", e.code(), e.details()))
        else:
            num_feature_sets_found = len(list(get_feature_set_response.feature_sets))
            if num_feature_sets_found == 0:
                return None
            if num_feature_sets_found > 1:
                raise Exception(
                    f'Found {num_feature_sets_found} feature sets with name "{name}"'
                    f' and version "{version}": {list(get_feature_set_response.feature_sets)}'
                )
            return FeatureSet.from_proto(get_feature_set_response.feature_sets[0])

    def list_entities(self) -> Dict[str, Entity]:
        """
        Returns a dictionary of entities across all feature sets
        :return: Dictionary of entity name to Entity
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
            feature_ids: List of feature ids that will be returned for each entity.
            Each feature id should have the following format "feature_set_name:version:feature_name".

            entity_rows: Pandas dataframe containing entities and a 'datetime' column. Each entity in
            a feature set must be present as a column in this dataframe. The datetime column must
            contain timestamps in epoch form and must have the type np.int64

        Returns:
            Feast batch retrieval job: feast.job.Job
            
        Example usage:
        ============================================================
        >>> from feast import Client
        >>> from datetime import datetime
        >>>
        >>> feast_client = Client(core_url="localhost:6565", serving_url="localhost:6566")
        >>> feature_ids = ["customer:1:bookings_7d"]
        >>> entity_rows = pd.DataFrame(
        >>>         {
        >>>            "datetime": [np.int64(time.time_ns()) for _ in range(3)],
        >>>            "customer": [1001, 1002, 1003],
        >>>         }
        >>>     )
        >>> feature_retrieval_job = feast_client.get_batch_features(feature_ids, entity_rows)
        >>> df = feature_retrieval_job.to_dataframe()
        >>> print(df)
        """

        self._connect_serving()

        try:
            fs_request = _build_feature_set_request(feature_ids)

            # Validate entity rows based on entities in Feast Core
            self._validate_entity_rows_for_batch_retrieval(entity_rows, fs_request)

            # we want the timestamp column naming to be consistent with the rest of feast
            entity_rows.columns = ['event_timestamp' if col=='datetime' else col for col in entity_rows.columns]

            # Retrieve serving information to determine store type and staging location
            serving_info = (
                self._serving_service_stub.GetFeastServingInfo()
            )  # type: GetFeastServingInfoResponse

            if serving_info.type != FeastServingType.FEAST_SERVING_TYPE_BATCH:
                raise Exception(
                    f'You are connected to a store "{self._serving_url}" which does not support batch retrieval'
                )

            # Export and upload entity row dataframe to staging location provided by Feast
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

        except grpc.RpcError as e:
            print(format_grpc_exception("GetBatchFeatures", e.code(), e.details()))

    def _validate_entity_rows_for_batch_retrieval(
        self, entity_rows, feature_sets_request
    ):
        """
        Validate whether an entity_row dataframe contains the correct information for batch retrieval
        :param entity_rows: Pandas dataframe containing entities and datetime column. Each entity in a feature set
        must be present as a column in this dataframe. The datetime column must contain Unix Epoch values down to
        nanoseconds
        :param feature_sets_request: Feature sets that will
        """
        # Ensure datetime column exists
        if "datetime" not in entity_rows.columns:
            raise ValueError(
                f'Entity rows does not contain "datetime" column in columns {entity_rows.columns}'
            )

        # Validate dataframe columns based on feature set entities
        for feature_set in feature_sets_request:
            fs = self.get_feature_set(
                name=feature_set.name, version=feature_set.version
            )
            if fs is None:
                raise ValueError(
                    f'Feature set "{feature_set.name}:{feature_set.version}" could not be found'
                )
            for entity_type in fs.entities:
                if entity_type.name not in entity_rows.columns:
                    raise ValueError(
                        f'Dataframe does not contain entity "{entity_type.name}" column in columns "{entity_rows.columns}"'
                    )

    def get_online_features(
        self,
        feature_ids: List[str],
        entity_rows: List[GetOnlineFeaturesRequest.EntityRow],
    ) -> GetOnlineFeaturesResponse:
        """
        Retrieves the latest online feature data from Feast Serving
        :param feature_ids: List of feature Ids in the following format
                            [feature_set_name]:[version]:[feature_name]
                            example: ["feature_set_1:6:my_feature_1",
                                     "feature_set_1:6:my_feature_2",]

        :param entity_rows: List of GetFeaturesRequest.EntityRow where each row
                            contains entities. Timestamp should not be set for
                            online retrieval. All entity types within a feature
                            set must be provided for each entity key.
        :return: Returns a list of maps where each item in the list contains
                 the latest feature values for the provided entities
        """
        self._connect_serving()

        try:
            response = self._serving_service_stub.GetOnlineFeatures(
                GetOnlineFeaturesRequest(
                    feature_sets=_build_feature_set_request(feature_ids),
                    entity_rows=entity_rows,
                )
            )  # type: GetOnlineFeaturesResponse
        except grpc.RpcError as e:
            print(format_grpc_exception("GetOnlineFeatures", e.code(), e.details()))
        else:
            return response


def _build_feature_set_request(feature_ids: List[str]) -> List[FeatureSetRequest]:
    """
    Builds a list of FeatureSet objects from feature set ids in order to retrieve feature data from Feast Serving
    """
    feature_set_request = dict()  # type: Dict[str, FeatureSetRequest]
    for feature_id in feature_ids:
        fid_parts = feature_id.split(":")
        if len(fid_parts) == 3:
            feature_set, version, feature = fid_parts
        else:
            raise ValueError(
                f"Could not parse feature id ${feature_id}, needs 3 colons"
            )

        if feature_set not in feature_set_request:
            feature_set_request[feature_set] = FeatureSetRequest(
                name=feature_set, version=int(version)
            )
        feature_set_request[feature_set].feature_names.append(feature)
    return list(feature_set_request.values())
