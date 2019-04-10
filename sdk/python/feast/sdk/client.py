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
"""
Main interface for users to interact with the Core API. 
"""

import os
from datetime import datetime

import grpc
import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp

from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.JobService_pb2 import JobServiceTypes
from feast.core.JobService_pb2_grpc import JobServiceStub
from feast.core.DatasetService_pb2 import DatasetServiceTypes
from feast.core.DatasetService_pb2_grpc import DatasetServiceStub
from feast.sdk.env import FEAST_CORE_URL_ENV_KEY, FEAST_SERVING_URL_ENV_KEY
from feast.sdk.resources.entity import Entity
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.feature_group import FeatureGroup
from feast.sdk.resources.feature_set import DatasetInfo, FileType
from feast.sdk.resources.storage import Storage
from feast.sdk.utils.bq_util import TableDownloader
from feast.sdk.utils.print_utils import spec_to_yaml
from feast.sdk.utils import types
from feast.serving.Serving_pb2 import QueryFeaturesRequest
from feast.serving.Serving_pb2_grpc import ServingAPIStub


class Client:
    def __init__(self, core_url=None, serving_url=None, verbose=False):
        """Create an instance of Feast client which is connected to feast
        endpoint specified in the parameter. If no url is provided, the
        client will default to the url specified in the environment variable
        FEAST_CORE_URL.

        Args:
            core_url (str, optional): feast's grpc endpoint URL
                                  (e.g.: "my.feast.com:8433")
            serving_url (str, optional): feast serving's grpc endpoint URL
                                  (e.g.: "my.feast.com:8433")
        """

        if core_url is None:
            core_url = os.getenv(FEAST_CORE_URL_ENV_KEY)
        self._core_url = core_url

        if serving_url is None:
            serving_url = os.getenv(FEAST_SERVING_URL_ENV_KEY)
        self._serving_url = serving_url

        self.__core_channel = None
        self.__serving_channel = None
        self._core_service_stub = None
        self._job_service_stub = None
        self._dataset_service_stub = None
        self._serving_service_stub = None

        self._verbose = verbose
        self._table_downloader = TableDownloader()

    @property
    def core_url(self):
        if self._core_url is None:
            self._core_url = os.getenv(FEAST_CORE_URL_ENV_KEY)
            if self._core_url is None:
                raise ValueError(
                    "Core API URL not set. Either set the "
                    + "environment variable {} or set it explicitly.".format(
                        FEAST_CORE_URL_ENV_KEY
                    )
                )
        return self._core_url

    @core_url.setter
    def core_url(self, value):
        self._core_url = value

    @property
    def serving_url(self):
        if self._serving_url is None:
            self._serving_url = os.getenv(FEAST_SERVING_URL_ENV_KEY)
            if self._serving_url is None:
                raise ValueError(
                    "Serving API URL not set. Either set the "
                    + "environment variable {} or set it explicitly.".format(
                        FEAST_SERVING_URL_ENV_KEY
                    )
                )
        return self._serving_url

    @serving_url.setter
    def serving_url(self, value):
        self._serving_url = value

    @property
    def verbose(self):
        return self._verbose

    @verbose.setter
    def verbose(self, val):
        if not isinstance(val, bool):
            raise TypeError("verbose should be a boolean value")
        self._verbose = val

    def apply(self, obj):
        """Create or update one or many feast's resource
        (feature, entity, importer, storage).

        Args:
            obj (object): one or many feast's resource
            // create_entity (bool, optional):  (default: {None})
            // create_features (bool, optional): [description] (default: {None})
        """
        if isinstance(obj, list):
            ids = []
            for resource in obj:
                ids.append(self._apply(resource))
            return ids
        else:
            return self._apply(obj)

    def run(
        self, importer, name_override=None, apply_entity=False, apply_features=False
    ):
        """
        Run an import job
        Args:
            importer (feast.sdk.importer.Importer): importer instance
            name_override (str, optional): Job name override
            apply_entity (bool, optional): (default: False) create/update
                entity inside importer
            apply_features (bool, optional): (default: False) create/update
                features inside importer

        Returns:
            (str) job ID of the import job
        """
        request = JobServiceTypes.SubmitImportJobRequest(importSpec=importer.spec)
        if name_override is not None:
            request.name = name_override

        if apply_entity:
            self._apply_entity(importer.entity)
        if apply_features:
            for feature in importer.features:
                self._apply_feature(importer.features[feature])

        if importer.require_staging:
            print("Staging file to remote path {}".format(importer.remote_path))
            importer.stage()
        print("Submitting job with spec:\n {}".format(spec_to_yaml(importer.spec)))
        self._connect_core()
        response = self._job_service_stub.SubmitJob(request)
        print("Submitted job with id: {}".format(response.jobId))
        return response.jobId

    def create_dataset(
        self, feature_set, start_date, end_date, limit=None, name_prefix=None
    ):
        """
        Create training dataset for a feature set. The training dataset
        will be bounded by event timestamp between start_date and end_date.
        Specify limit to limit number of row returned. The training dataset
        will reside in a bigquery table specified by destination.

        Args:
            feature_set (feast.sdk.resources.feature_set.FeatureSet):
                feature set representing the data wanted
            start_date (str): starting date of the training data in ISO 8601
                format (e.g.: "2018-12-31")
            end_date (str): end date of training data in ISO 8601 format (e.g.:
                "2018-12-31")
            limit (int, optional): (default: None) maximum number of row
                returned
            name_prefix (str, optional): (default: None) name prefix.
        :return:
            feast.resources.feature_set.DatasetInfo: DatasetInfo containing
            the information of training dataset
        """
        self._check_create_dataset_args(feature_set, start_date, end_date, limit)

        req = DatasetServiceTypes.CreateDatasetRequest(
            featureSet=feature_set.proto,
            startDate=_timestamp_from_datetime(_parse_date(start_date)),
            endDate=_timestamp_from_datetime(_parse_date(end_date)),
            limit=limit,
            namePrefix=name_prefix,
        )
        if self.verbose:
            print(
                "creating training dataset for features: " + str(feature_set.features)
            )
        self._connect_core()
        resp = self._dataset_service_stub.CreateDataset(req)

        if self.verbose:
            print(
                "created dataset {}: {}".format(
                    resp.datasetInfo.name, resp.datasetInfo.tableUrl
                )
            )
        return DatasetInfo(resp.datasetInfo.name, resp.datasetInfo.tableUrl)

    def get_serving_data(self, feature_set, entity_keys, ts_range=None):
        """Get feature value from feast serving API.

        If server_url is not provided, the value stored in the environment variable
        FEAST_SERVING_URL is used to connect to the serving server instead.

        Args:
            feature_set (feast.sdk.resources.feature_set.FeatureSet): feature set
                representing the data wanted
            entity_keys (:obj: `list` of :obj: `str): list of entity keys
            ts_range (:obj: `list` of str, optional): size 2 list of start
                and end time, in datetime type. It will
                filter out any feature value having event timestamp outside
                of the ts_range.

        Returns:
            pandas.DataFrame: DataFrame of results
        """
        start = None
        end = None
        if ts_range is not None:
            if len(ts_range) != 2:
                raise ValueError("ts_range must have len 2")
            start = ts_range[0]
            end = ts_range[1]
            if type(start) is not datetime or type(end) is not datetime:
                raise TypeError("start and end must be datetime type")

        request = self._build_serving_request(feature_set, entity_keys)
        self._connect_serving()
        return self._response_to_df(
            feature_set, self._serving_service_stub.QueryFeatures(request), start, end
        )

    def download_dataset(
        self, dataset_info, dest, staging_location, file_type=FileType.CSV
    ):
        """
        Download training dataset as file
        Args:
            dataset_info (feast.sdk.resources.feature_set.DatasetInfo) :
                dataset_info to be downloaded
            dest (str): destination's file path
            staging_location (str): url to staging_location (currently
                support a folder in GCS)
            file_type (feast.sdk.resources.feature_set.FileType): (default:
                FileType.CSV) exported file format
        Returns:
            str: path to the downloaded file
        """
        return self._table_downloader.download_table_as_file(
            dataset_info.table_id, dest, staging_location, file_type
        )

    def download_dataset_to_df(self, dataset_info, staging_location):
        """
        Download training dataset as Pandas Dataframe
        Args:
            dataset_info (feast.sdk.resources.feature_set.DatasetInfo) :
                dataset_info to be downloaded
            staging_location: url to staging_location (currently
                support a folder in GCS)

        Returns: pandas.DataFrame: dataframe of the training dataset

        """
        return self._table_downloader.download_table_as_df(
            dataset_info.table_id, staging_location
        )

    def close(self):
        """
        Close underlying connection to Feast's core and serving end points.
        """
        self.__core_channel.close()
        self.__core_channel = None
        self.__serving_channel.close()
        self.__serving_channel = None

    def _connect_core(self):
        """Connect to core api"""
        if self.__core_channel is None:
            self.__core_channel = grpc.insecure_channel(self.core_url)
            self._core_service_stub = CoreServiceStub(self.__core_channel)
            self._job_service_stub = JobServiceStub(self.__core_channel)
            self._dataset_service_stub = DatasetServiceStub(self.__core_channel)

    def _connect_serving(self):
        """Connect to serving api"""
        if self.__serving_channel is None:
            self.__serving_channel = grpc.insecure_channel(self.serving_url)
            self._serving_service_stub = ServingAPIStub(self.__serving_channel)

    def _build_serving_request(self, feature_set, entity_keys):
        """Helper function to build serving service request."""
        return QueryFeaturesRequest(
            entityName=feature_set.entity,
            entityId=entity_keys,
            featureId=feature_set.features,
        )

    def _response_to_df(self, feature_set, response, start=None, end=None):
        is_filter_time = start is not None and end is not None
        df = pd.DataFrame(columns=[feature_set.entity] + feature_set.features)
        dtypes = {}
        for entity_id in response.entities:
            feature_map = response.entities[entity_id].features
            row = {response.entityName: entity_id}
            for feature_id in feature_map:
                v = feature_map[feature_id].value
                if is_filter_time:
                    ts = feature_map[feature_id].timestamp.ToDatetime()
                    if ts < start or ts > end:
                        continue
                feast_valuetype = v.WhichOneof("val")
                if feast_valuetype not in dtypes:
                    dtypes[feature_id] = types.FEAST_VALUETYPE_TO_DTYPE[feast_valuetype]
                v = getattr(v, v.WhichOneof("val"))
                row[feature_id] = v
            df = df.append(row, ignore_index=True)
        return df.astype(dtypes).reset_index(drop=True)

    def _apply(self, obj):
        """Applies a single object to feast core.

        Args:
            obj (object): one of
                [Feature, Entity, FeatureGroup, Storage, Importer]
        """
        if isinstance(obj, Feature):
            return self._apply_feature(obj)
        elif isinstance(obj, Entity):
            return self._apply_entity(obj)
        elif isinstance(obj, FeatureGroup):
            return self._apply_feature_group(obj)
        elif isinstance(obj, Storage):
            return self._apply_storage(obj)
        else:
            raise TypeError(
                "Apply can only be passed one of the following \
            types: [Feature, Entity, FeatureGroup, Storage, Importer]"
            )

    def _apply_feature(self, feature):
        """Apply the feature to the core API

        Args:
            feature (feast.sdk.resources.feature.Feature): feature to apply
        """
        self._connect_core()
        response = self._core_service_stub.ApplyFeature(feature.spec)
        if self.verbose:
            print(
                "Successfully applied feature with id: {}\n---\n{}".format(
                    response.featureId, feature
                )
            )
        return response.featureId

    def _apply_entity(self, entity):
        """Apply the entity to the core API

        Args:
            entity (feast.sdk.resources.entity.Entity): entity to apply
        """
        self._connect_core()
        response = self._core_service_stub.ApplyEntity(entity.spec)
        if self.verbose:
            print(
                "Successfully applied entity with name: {}\n---\n{}".format(
                    response.entityName, entity
                )
            )
        return response.entityName

    def _apply_feature_group(self, feature_group):
        """Apply the feature group to the core API

        Args:
            feature_group (feast.sdk.resources.feature_group.FeatureGroup):
                feature group to apply
        """
        self._connect_core()
        response = self._core_service_stub.ApplyFeatureGroup(feature_group.spec)
        if self.verbose:
            print(
                "Successfully applied feature group with id: "
                + "{}\n---\n{}".format(response.featureGroupId, feature_group)
            )
        return response.featureGroupId

    def _apply_storage(self, storage):
        """Apply the storage to the core API

        Args:
            storage (feast.sdk.resources.storage.Storage): storage to apply
        """
        self._connect_core()
        response = self._core_service_stub.ApplyStorage(storage.spec)
        if self.verbose:
            print(
                "Successfully applied storage with id: "
                + "{}\n{}".format(response.storageId, storage)
            )
        return response.storageId

    def _check_create_dataset_args(self, feature_set, start_date, end_date, limit):
        if len(feature_set.features) < 1:
            raise ValueError("feature set is empty")

        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if end < start:
            raise ValueError("end_date is before start_date")

        if limit is not None and limit < 1:
            raise ValueError("limit is not a positive integer")


def _parse_date(date):
    try:
        return datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")


def _timestamp_from_datetime(dt):
    """Convert datetime to protobuf timestamp

    Args:
        dt (datetime.datetime): datetime in datetime format

    Returns:
        google.protobuf.timestamp_pb2.Timestamp: timestamp in protobuf format
    """
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts
