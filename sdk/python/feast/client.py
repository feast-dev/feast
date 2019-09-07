# Copyright 2018 The Feast Authors
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


import grpc

from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.CoreService_pb2 import (
    GetFeatureSetsResponse,
    ApplyFeatureSetRequest,
    GetFeatureSetsRequest,
    ApplyFeatureSetResponse,
)
from feast.core.FeatureSet_pb2 import FeatureSetSpec, FeatureSpec, EntitySpec
from feast.core.Source_pb2 import Source
from feast.feature_set import FeatureSet, Entity
from feast.serving.ServingService_pb2_grpc import ServingServiceStub
from google.protobuf import empty_pb2 as empty
from typing import List
from collections import OrderedDict
from typing import Dict
from feast.type_map import dtype_to_value_type
import os

GRPC_CONNECTION_TIMEOUT = 5  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str


class Client:
    def __init__(
        self, core_url: str = None, serving_url: str = None, verbose: bool = False
    ):
        self._feature_sets = []  # type: List[FeatureSet]
        self._core_url = core_url
        self._serving_url = serving_url
        self._verbose = verbose
        self.__core_channel: grpc.Channel = None
        self.__serving_channel: grpc.Channel = None
        self._core_service_stub: CoreServiceStub = None
        self._serving_service_stub: ServingServiceStub = None
        self._is_serving_connected = False
        self._is_core_connected = False

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
        self._connect_core()
        self._connect_serving()

        try:
            core_version = self._core_service_stub.GetFeastCoreVersion(
                empty, timeout=GRPC_CONNECTION_TIMEOUT
            ).version
        except grpc.FutureCancelledError:
            core_version = "not connected"

        try:
            serving_version = self._serving_service_stub.GetFeastServingVersion(
                empty, timeout=GRPC_CONNECTION_TIMEOUT
            ).version
        except grpc.FutureCancelledError:
            serving_version = "not connected"

        return {
            "core": {"url": self.core_url, "version": core_version},
            "serving": {"url": self.serving_url, "version": serving_version},
        }

    def _connect_core(self):
        """
        Connect to Core API
        """
        if not self.core_url:
            raise ValueError("Please set Feast Core URL.")

        if self.__core_channel is None:
            self.__core_channel = grpc.insecure_channel(self.core_url)

        try:
            grpc.channel_ready_future(self.__core_channel).result(
                timeout=GRPC_CONNECTION_TIMEOUT
            )
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                "connection timed out while attempting to connect to Feast Core gRPC server "
                + self.core_url
            )
        else:
            self._core_service_stub = CoreServiceStub(self.__core_channel)
            self._is_core_connected = True

    def _connect_serving(self):
        """
        Connect to Serving API
        """

        if not self.serving_url:
            raise ValueError("Please set Feast Serving URL.")

        if self.__serving_channel is None:
            self.__serving_channel = grpc.insecure_channel(self.serving_url)

        try:
            grpc.channel_ready_future(self.__serving_channel).result(
                timeout=GRPC_CONNECTION_TIMEOUT
            )
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                "connection timed out while attempting to connect to Feast Serving gRPC server "
                + self.serving_url
            )
        else:
            self._serving_service_stub = ServingServiceStub(self.__serving_channel)
            self._is_serving_connected = True

    def apply(self, resource):
        if isinstance(resource, FeatureSet):
            self._apply_feature_set(resource)

    def refresh(self):
        """
        Refresh list of Feature Sets from Feast Core
        """
        self._connect_core()

        # Get latest Feature Sets from Feast Core
        feature_set_protos = self._core_service_stub.GetFeatureSets(
            GetFeatureSetsRequest(filter=GetFeatureSetsRequest.Filter())
        )  # type: GetFeatureSetsResponse

        # Store list of Feature Sets
        self._feature_sets = [
            FeatureSet.from_proto(feature_set)
            for feature_set in feature_set_protos.featureSets
        ]

    @property
    def feature_sets(self) -> List[FeatureSet]:
        self.refresh()
        return self._feature_sets

    @property
    def entities(self) -> Dict[str, Entity]:
        entities_dict = OrderedDict()
        for fs in self._feature_sets:
            for entityName, entity in fs.entities:
                entities_dict[entityName] = entity
        return entities_dict

    def _apply_feature_set(self, feature_set: FeatureSet):
        self._connect_core()
        apply_feature_set_response = self._core_service_stub.ApplyFeatureSet(
            ApplyFeatureSetRequest(featureSet=feature_set.to_proto()),
            timeout=GRPC_CONNECTION_TIMEOUT,
        )  # type: ApplyFeatureSetResponse

        feature_set._version = apply_feature_set_response.featureSet.version
        feature_set._is_dirty = False
        feature_set._client = self
