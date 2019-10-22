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


import os
from collections import OrderedDict
from typing import Dict, Union
from typing import List
import grpc
import pandas as pd
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
)
from feast.serving.ServingService_pb2_grpc import ServingServiceStub


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
            feature_set._update_from_feature_set(applied_fs, is_dirty=False)
        except grpc.RpcError as e:
            print(format_grpc_exception("ApplyFeatureSet", e.code(), e.details()))

    def list_feature_sets(self) -> List[FeatureSet]:
        """
        Retrieve a list of feature sets from Feast Core
        :return: Returns a list of feature sets
        """
        self._connect_core()

        # Get latest feature sets from Feast Core
        feature_set_protos = self._core_service_stub.GetFeatureSets(
            GetFeatureSetsRequest()
        )  # type: GetFeatureSetsResponse

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
        self, feature_ids: List[str], entity_rows: List[GetBatchFeaturesRequest]
    ) -> Job:
        """

        Args:
            feature_ids: List of feature ids in the following format
                         "feature_set_name:version:feature_name".

            entity_rows: List of GetFeaturesRequest.EntityRow where each row
                         contains entities and a timestamp.

                         Feature values will be looked up based on feature_id
                         and entities, where entities are the keys. The latest
                         feature value will be retrieved based on the timestamp

        Returns:
            Feast batch retrieval job: feast.job.Job
            
        Example usage:
        ============================================================
        >>> from feast.client import Client
        >>> from datetime import datetime
        >>>
        >>> feast_client = Client(core_url="localhost:6565", serving_url="localhost:6566")
        >>> feature_ids = ["driver:1:city"]
        >>> entity_rows = [GetFeaturesRequest.EntityRow(
        >>>                fields={'driver_id': ProtoValue(int64_val='1234')})
        >>>                for user_id in user_emb_df['user_id']]
        >>> feature_retrieval_job = feast_client.get_batch_features(feature_ids, entity_rows)
        >>> df = feature_retrieval_job.to_dataframe()
        >>> print(df)
        """

        self._connect_serving(skip_if_connected=True)

        request = GetFeaturesRequest(
            feature_sets=_build_online_feature_request(feature_ids),
            entity_rows=entity_rows,
        )

        response = self._serving_service_stub.GetBatchFeatures(request)

        return Job(response.job, self._serving_service_stub)

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
        self._connect_serving(skip_if_connected=True)

        try:
            response = self._serving_service_stub.GetOnlineFeatures(
                GetOnlineFeaturesRequest(
                    feature_sets=_build_online_feature_request(feature_ids),
                    entity_rows=entity_rows,
                )
            )  # type: GetOnlineFeaturesResponse
        except grpc.RpcError as e:
            print(format_grpc_exception("GetOnlineFeatures", e.code(), e.details()))
        else:
            return response


def _build_online_feature_request(feature_ids: List[str]) -> List[FeatureSet]:
    """
    Builds a list of FeatureSet objects from feature set ids in order to retrieve feature data from Feast Serving
    """
    feature_set_request = dict()  # type: Dict[str, GetOnlineFeaturesRequest.FeatureSet]
    for feature_id in feature_ids:
        fid_parts = feature_id.split(":")
        if len(fid_parts) == 3:
            feature_set, version, feature = fid_parts
        else:
            raise ValueError(
                f"Could not parse feature id ${feature_id}, needs 3 colons"
            )

        if feature_set not in feature_set_request:
            feature_set_request[feature_set] = GetOnlineFeaturesRequest.FeatureSet(
                name=feature_set, version=int(version)
            )
        feature_set_request[feature_set].feature_names.append(feature)
    return list(feature_set_request.values())
