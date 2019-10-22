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
from typing import Dict
from typing import List

import grpc
import numpy as np
import pandas as pd

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
from feast.type_map import FEAST_VALUE_ATTR_TO_DTYPE

GRPC_CONNECTION_TIMEOUT_DEFAULT = 5  # type: int
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
        self._connect_core()
        self._connect_serving()

        try:
            core_version = self._core_service_stub.GetFeastCoreVersion(
                GetFeastCoreVersionRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            ).version
        except grpc.FutureCancelledError:
            core_version = "not connected"

        try:
            serving_version = self._serving_service_stub.GetFeastServingInfo(
                GetFeastServingInfoRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            ).version
        except grpc.FutureCancelledError:
            serving_version = "not connected"

        return {
            "core": {"url": self.core_url, "version": core_version},
            "serving": {"url": self.serving_url, "version": serving_version},
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
            raise ConnectionError(
                "connection timed out while attempting to connect to Feast Core gRPC server "
                + self.core_url
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
            raise ConnectionError(
                "connection timed out while attempting to connect to Feast Serving gRPC server "
                + self.serving_url
            )
        else:
            self._serving_service_stub = ServingServiceStub(self.__serving_channel)

    def apply(self, resources):
        if not isinstance(resources, list):
            resources = [resources]
        for resource in resources:
            if isinstance(resource, FeatureSet):
                self._apply_feature_set(resource)
                continue
            raise Exception("Could not determine resource type to apply")

    def list_feature_sets(self) -> List[FeatureSet]:
        """
        Retrieve a list of Feature Sets from Feast Core
        """
        self._connect_core(skip_if_connected=True)

        # Get latest Feature Sets from Feast Core
        feature_set_protos = self._core_service_stub.GetFeatureSets(
            GetFeatureSetsRequest()
        )  # type: GetFeatureSetsResponse

        # Store list of Feature Sets
        feature_sets = []
        for feature_set_proto in feature_set_protos.feature_sets:
            feature_set = FeatureSet.from_proto(feature_set_proto)
            feature_set._client = self
            feature_sets.append(feature_set)
        return feature_sets

    def get_feature_set(self, name: str, version: int = None) -> FeatureSet:
        """
        Retrieve a single Feature Set from Feast Core
        """
        self._connect_core(skip_if_connected=True)

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
            print(
                f"GetFeatureSets failed with\nCODE: {e.code()}\nDETAILS: {e.details()}"
            )
        else:
            num_feature_sets_found = len(list(get_feature_set_response.feature_sets))

            if num_feature_sets_found == 0:
                return

            if num_feature_sets_found > 1:
                raise Exception(
                    f'Found {num_feature_sets_found} feature sets with name "{name}"'
                    f' and version "{version}".'
                )

            return FeatureSet.from_proto(get_feature_set_response.feature_sets[0])

    def list_entities(self) -> Dict[str, Entity]:
        entities_dict = OrderedDict()
        for fs in self.list_feature_sets():
            for entity in fs.entities:
                entities_dict[entity.name] = entity
        return entities_dict

    def _apply_feature_set(self, feature_set: FeatureSet):
        self._connect_core(skip_if_connected=True)
        feature_set._client = self

        try:
            apply_fs_response = self._core_service_stub.ApplyFeatureSet(
                ApplyFeatureSetRequest(feature_set=feature_set.to_proto()),
                timeout=GRPC_CONNECTION_TIMEOUT_APPLY,
            )  # type: ApplyFeatureSetResponse

            if apply_fs_response.status == ApplyFeatureSetResponse.Status.ERROR:
                raise Exception(
                    "Error while trying to apply feature set " + feature_set.name
                )

            applied_fs = FeatureSet.from_proto(apply_fs_response.feature_set)
            feature_set._update_from_feature_set(applied_fs, is_dirty=False)

        except Exception as e:
            print(e)
        return

    def get_batch_features(
        self, feature_ids: List[str], entity_rows: List[GetBatchFeaturesRequest]
    ) -> Job:
        """

        Args:
            feature_ids: List of feature ids in the following format
                         "feature_set_name:version:feature_name".

            entity_rwaleows: List of GetFeaturesRequest.EntityRow where each row
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
            feature_sets=build_online_feature_request(feature_ids),
            entity_rows=entity_rows,
        )

        response = self._serving_service_stub.GetBatchFeatures(request)

        return Job(response.job, self._serving_service_stub)

    def get_online_features(
        self,
        feature_ids: List[str],
        entity_rows: List[GetOnlineFeaturesRequest.EntityRow],
    ):
        self._connect_serving(skip_if_connected=True)

        try:
            response = self._serving_service_stub.GetOnlineFeatures(
                GetOnlineFeaturesRequest(
                    feature_sets=build_online_feature_request(feature_ids),
                    entity_rows=entity_rows,
                )
            )  # type: GetOnlineFeaturesResponse
        except grpc.RpcError as e:
            print(
                f"GetOnlineFeatures failed with\nCODE: {e.code()}\nDETAILS: {e.details()}"
            )
        else:
            return response


def build_online_feature_request(
    feature_ids: List[str]
) -> List[GetOnlineFeaturesRequest.FeatureSet]:
    feature_set_request = dict()  # type: Dict[str,GetOnlineFeaturesRequest.FeatureSet]
    for feature_id in feature_ids:
        fid_parts = feature_id.split(":")
        if len(fid_parts) == 3:
            feature_set, version, feature = fid_parts
        else:
            raise ValueError(f"Could not parse feature id: ${feature_id}")

        if feature_set not in feature_set_request:
            feature_set_request[feature_set] = GetOnlineFeaturesRequest.FeatureSet(
                name=feature_set, version=version
            )
        feature_set_request[feature_set].feature_names.append(feature)
    return list(feature_set_request.values())


def field_values_to_df(
    field_values: List[GetOnlineFeaturesResponse.FieldValues]
) -> pd.DataFrame():
    dtypes = {}
    value_attr = {}
    columns = []
    data = {}
    first_row_done = False

    for row in field_values:
        for feature_id, value in dict(row.fields).items():

            # For each column, detect the first value type that has been set and keep it consistent
            if feature_id not in value_attr or value_attr[feature_id] is None:
                field_type = value.WhichOneof("val")
                value_attr[feature_id] = field_type

            if not first_row_done:
                columns.append(feature_id)
                data[feature_id] = []
                value_attr[feature_id] = value.WhichOneof("val")
                if value_attr[feature_id] is None:
                    dtypes[feature_id] = None
                else:
                    dtypes[feature_id] = FEAST_VALUE_ATTR_TO_DTYPE[
                        value_attr[feature_id]
                    ]

            if feature_id in value_attr and value_attr[feature_id] is None:
                data[feature_id].append(None)
            else:
                data[feature_id].append(getattr(value, value_attr[feature_id]))

        first_row_done = True

    return (
        pd.DataFrame(columns=columns, data=data).reset_index(drop=True).astype(dtypes)
    )
