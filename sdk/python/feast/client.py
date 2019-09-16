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
    GetFeastCoreVersionRequest,
    GetFeatureSetsResponse,
    ApplyFeatureSetRequest,
    GetFeatureSetsRequest,
    ApplyFeatureSetResponse,
)
from feast.serving.ServingService_pb2 import (
    GetFeaturesRequest,
    GetFeastServingVersionRequest,
    GetOnlineFeaturesResponse,
)
from feast.feature_set import FeatureSet, Entity
from feast.serving.ServingService_pb2_grpc import ServingServiceStub

from typing import List
from collections import OrderedDict
from typing import Dict
import os
import pandas as pd
from feast.type_map import (
    pandas_value_to_proto_value,
    dtype_to_feast_value_attr,
    FEAST_VALUETYPE_TO_DTYPE,
)

GRPC_CONNECTION_TIMEOUT = 600  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str


class Client:
    def __init__(
        self, core_url: str = None, serving_url: str = None, verbose: bool = False
    ):
        self._feature_sets = OrderedDict()  # type: Dict[FeatureSet]
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
                GetFeastCoreVersionRequest(), timeout=GRPC_CONNECTION_TIMEOUT
            ).version
        except grpc.FutureCancelledError:
            core_version = "not connected"

        try:
            serving_version = self._serving_service_stub.GetFeastServingVersion(
                GetFeastServingVersionRequest(), timeout=GRPC_CONNECTION_TIMEOUT
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
                timeout=GRPC_CONNECTION_TIMEOUT
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
            return
        raise Exception("Could not determine resource type to apply")

    def refresh(self):
        """
        Refresh list of Feature Sets from Feast Core
        """
        self._connect_core(skip_if_connected=True)

        # Get latest Feature Sets from Feast Core
        feature_set_protos = self._core_service_stub.GetFeatureSets(
            GetFeatureSetsRequest(filter=GetFeatureSetsRequest.Filter())
        )  # type: GetFeatureSetsResponse

        # Store list of Feature Sets
        for feature_set_proto in feature_set_protos.feature_sets:
            feature_set = FeatureSet.from_proto(feature_set_proto)
            feature_set._is_dirty = False
            feature_set._client = self
            self._feature_sets[feature_set.name.strip()] = feature_set

    @property
    def feature_sets(self) -> List[FeatureSet]:
        self.refresh()
        return list(self._feature_sets.values())

    @property
    def entities(self) -> Dict[str, Entity]:
        entities_dict = OrderedDict()
        for fs in list(self._feature_sets.values()):
            for entity in fs.entities:
                entities_dict[entity.name] = entity
        return entities_dict

    def _apply_feature_set(self, feature_set: FeatureSet):
        self._connect_core(skip_if_connected=True)
        feature_set._client = self

        apply_fs_response = self._core_service_stub.ApplyFeatureSet(
            ApplyFeatureSetRequest(feature_set=feature_set.to_proto()),
            timeout=GRPC_CONNECTION_TIMEOUT,
        )  # type: ApplyFeatureSetResponse

        if apply_fs_response.status == ApplyFeatureSetResponse.Status.ERROR:
            raise Exception(
                "Error while trying to apply feature set " + feature_set.name
            )

        # Refresh state from Feast Core to local client
        self.refresh()

        # Replace applied feature set with refreshed feature set from Feast Core
        deep_update_feature_set(
            source=self._feature_sets[feature_set.name], target=feature_set
        )

    def get(
        self,
        entity_data: pd.DataFrame,
        feature_ids: List[str],
        join_on: Dict[str, str] = None,
    ) -> pd.DataFrame:
        self._connect_serving(skip_if_connected=True)

        if "datetime" != entity_data.columns[0]:
            raise ValueError("The first column in entity_data should be 'datetime'")

        entity_data_field_names = []
        for column in entity_data.columns[1:]:
            if column not in self.entities.keys():
                raise Exception("Entity " + column + " could not be found")
            entity_data_field_names.append(column)

        entity_dataset_rows = []
        for _, row in entity_data.iterrows():
            entity_dataset_row = GetFeaturesRequest.EntityDataSetRow()
            for i in range(len(entity_data.columns[1:])):
                proto_value = pandas_value_to_proto_value(
                    entity_data[entity_data.columns[i]].dtype, row[i+1]
                )
                entity_dataset_row.value.append(proto_value)
            entity_dataset_rows.append(entity_dataset_row)

        feature_set_request = create_feature_set_request_from_feature_strings(
            feature_ids
        )

        get_online_features_response_proto = self._serving_service_stub.GetOnlineFeatures(
            GetFeaturesRequest(
                entityDataSet=GetFeaturesRequest.EntityDataSet(
                    entity_data_set_rows=entity_dataset_rows,
                    fieldNames=entity_data_field_names,
                ),
                featureSets=feature_set_request,
            )
        )  # type: GetOnlineFeaturesResponse

        feature_dataframe = feature_data_sets_to_pandas_dataframe(
            entity_data_set=entity_data.copy(),
            feature_data_sets=list(
                get_online_features_response_proto.feature_data_sets
            ),
        )
        return feature_dataframe


def feature_data_sets_to_pandas_dataframe(
    entity_data_set: pd.DataFrame,
    feature_data_sets: List[GetOnlineFeaturesResponse.FeatureDataSet],
):
    feature_data_set_dataframes = []
    for feature_data_set in feature_data_sets:
        # Validate feature data set length
        if len(feature_data_set.feature_rows) != len(entity_data_set.index):
            raise Exception(
                "Feature data set response is of different size "
                + str(len(feature_data_set.feature_rows))
                + " than the entity data set request "
                + str(len(entity_data_set.index))
            )

        # Convert to Pandas DataFrame
        feature_data_set_dataframes.append(
            feature_data_set_to_pandas_dataframe(feature_data_set)
        )

    # Join dataframes into a single feature dataframe
    dataframe = join_feature_set_dataframes(feature_data_set_dataframes)
    return dataframe


def join_feature_set_dataframes(
    feature_data_set_dataframes: List[pd.DataFrame]
) -> pd.DataFrame:
    return (
        feature_data_set_dataframes[0]
        if len(feature_data_set_dataframes) > 0
        else pd.DataFrame
    )


def feature_data_set_to_pandas_dataframe(
    feature_data_set: GetOnlineFeaturesResponse.FeatureDataSet
) -> pd.DataFrame:
    feature_set_name = feature_data_set.name
    dtypes = {}
    columns = []
    for field in list(feature_data_set.feature_rows[0].fields):
        feature_id = feature_set_name + "." + field.name
        columns.append(feature_id)
        dtypes[feature_id] = FEAST_VALUETYPE_TO_DTYPE[field.value.WhichOneof("val")]

    dataframe = pd.DataFrame(columns=columns).reset_index(drop=True).astype(dtypes)

    for featureRow in list(feature_data_set.feature_rows):
        pandas_row = {}
        for field in list(featureRow.fields):
            pandas_row[feature_set_name + "." + field.name] = getattr(
                field.value, field.value.WhichOneof("val")
            )
        dataframe = dataframe.append(pandas_row, ignore_index=True)

    return dataframe


def create_feature_set_request_from_feature_strings(
    feature_ids: List[str]
) -> List[GetFeaturesRequest.FeatureSet]:
    feature_set_request = dict()  # type: Dict[str, GetFeaturesRequest.FeatureSet]
    for feature_id in feature_ids:
        feature_set, feature = feature_id.split(".")
        if feature_set not in feature_set_request:
            feature_set_name, feature_set_version = feature_set.split(":")
            feature_set_request[feature_set] = GetFeaturesRequest.FeatureSet(
                name=feature_set_name,
                version=feature_set_version
            )
        feature_set_request[feature_set].feature_names.append(feature)
    return list(feature_set_request.values())


def deep_update_feature_set(source: FeatureSet, target: FeatureSet):
    target._name = source.name
    target._version = source.version
    target._source = source.source
    target._max_age = source.max_age
    target._features = source.features
    target._entities = source.entities
    target._is_dirty = source._is_dirty
