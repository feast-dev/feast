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
from feast.core.CoreService_pb2 import GetFeatureSetsResponse
from feast.feature_set import FeatureSet, Entity
from feast.serving.ServingService_pb2_grpc import ServingServiceStub
from google.protobuf import empty_pb2 as Empty
from typing import List
from collections import OrderedDict
from typing import Dict
import os

GRPC_CONNECTION_TIMEOUT = 5  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str


class Client:
    def __init__(self, core_url: str, serving_url: str, verbose: bool = False):
        self._feature_sets = []  # type: List[FeatureSet]
        self._core_url = core_url
        self._serving_url = serving_url
        self._verbose = verbose
        self.__core_channel: grpc.Channel = None
        self.__serving_channel: grpc.Channel = None
        self._core_service_stub: CoreServiceStub = None
        self._serving_service_stub: ServingServiceStub = None
        self._is_connected = False

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

    def _connect_all(self):
        if not self.core_url and not self.serving_url:
            raise ValueError("Please set Core and Serving URL.")

        if not self._is_connected:
            self._connect_core()
            self._connect_serving()
            self._is_connected = True

    def version(self):
        self._connect_all()

        try:
            core_version = self._core_service_stub.GetFeastCoreVersion(
                Empty, timeout=GRPC_CONNECTION_TIMEOUT
            ).version
        except grpc.FutureCancelledError:
            core_version = "not connected"

        try:
            serving_version = self._serving_service_stub.GetFeastServingVersion(
                Empty, timeout=GRPC_CONNECTION_TIMEOUT
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

    def _connect_serving(self):
        """
        Connect to Serving API
        """

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

    def apply(self, resource):
        if isinstance(resource, FeatureSet):
            self._apply_feature_set(resource)

    def refresh(self):
        self._connect_all()
        fs = self._core_service_stub.GetFeatureSets()  # type: GetFeatureSetsResponse
        self._feature_sets = fs.featureSets

    @property
    def feature_sets(self) -> List[FeatureSet]:
        return self._feature_sets

    @property
    def entities(self) -> Dict[str, Entity]:
        entities_dict = OrderedDict()
        for fs in self._feature_sets:
            for entityName, entity in fs.entities:
                entities_dict[entityName] = entity
        return entities_dict

    def _apply_feature_set(self, resource):
        resource._client = self
        # TODO: Apply Feature Set to Feast Core
