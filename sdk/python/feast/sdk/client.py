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

import enum
import os
from datetime import datetime

import grpc
import pandas as pd
import dateutil.parser
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
from feast.serving.Serving_pb2 import QueryFeatures, RequestDetail, \
    TimestampRange
from feast.serving.Serving_pb2_grpc import ServingAPIStub


class ServingRequestType(enum.Enum):
    """
    Request type for serving api
    """
    LAST = 0
    """ Get last value of a feature """
    LIST = 1
    """ Get list of value of a feature """


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
                raise ValueError("Core API URL not set. Either set the " +
                                 "environment variable {} or set it explicitly."
                                 .format(FEAST_CORE_URL_ENV_KEY))
        return self._core_url

    @core_url.setter
    def core_url(self, value):
        self._core_url = value

    @property
    def serving_url(self):
        if self._serving_url is None:
            self._serving_url = os.getenv(FEAST_SERVING_URL_ENV_KEY)
            if self._serving_url is None:
                raise ValueError("Serving API URL not set. Either set the " +
                                 "environment variable {} or set it explicitly."
                                 .format(FEAST_SERVING_URL_ENV_KEY))
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

    def run(self, importer, name_override=None,
            apply_entity=False, apply_features=False):
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
        request = JobServiceTypes.SubmitImportJobRequest(
            importSpec=importer.spec
        )
        if name_override is not None:
            request.name = name_override

        if apply_entity:
            self._apply_entity(importer.entity)
        if apply_features:
            for feature in importer.features:
                self._apply_feature(feature)

        if importer.require_staging:
            print("Staging file to remote path {}"
                  .format(importer.remote_path))
            importer.stage()
        print("Submitting job with spec:\n {}"
              .format(spec_to_yaml(importer.spec)))
        self._connect_core()
        response = self._job_service_stub.SubmitJob(request)
        print("Submitted job with id: {}".format(response.jobId))
        return response.jobId

    def create_dataset(self, feature_set, start_date, end_date,
                                limit=None, name_prefix=None):
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
        self._check_create_dataset_args(feature_set, start_date,
                                                 end_date, limit)

        req = DatasetServiceTypes.CreateDatasetRequest(
            featureSet=feature_set.proto,
            startDate=_timestamp_from_datetime(_parse_date(start_date)),
            endDate=_timestamp_from_datetime(_parse_date(end_date)),
            limit=limit,
            namePrefix=name_prefix
        )
        if self.verbose:
            print("creating training dataset for features: " +
                  str(feature_set.features))
        self._connect_core()
        resp = self._dataset_service_stub.CreateDataset(req)

        if self.verbose:
            print("created dataset {}: {}".format(resp.datasetInfo.name,
                                                  resp.datasetInfo.tableUrl))
        return DatasetInfo(resp.datasetInfo.name, resp.datasetInfo.tableUrl)

    def get_serving_data(self, feature_set, entity_keys,
                         request_type=ServingRequestType.LAST,
                         ts_range=[], limit=10):
        """Get data from the feast serving layer. You can either retrieve the
        the latest value, or a list of the latest values, up to a provided
        limit.

        If server_url is not provided, the value stored in the environment variable
        FEAST_SERVING_URL is used to connect to the serving server instead.

        Args:
            feature_set (feast.sdk.resources.feature_set.FeatureSet): feature set
                representing the data wanted
            entity_keys (:obj: `list` of :obj: `str): list of entity keys
            request_type (feast.sdk.utils.types.ServingRequestType):
                (default: feast.sdk.utils.types.ServingRequestType.LAST) type of
                request: one of [LIST, LAST]
            ts_range (:obj: `list` of str, optional): size 2 list of start 
                timestamp and end timestamp, in ISO 8601 format. Only required if
                request_type is set to LIST
            limit (int, optional): (default: 10) number of values to get. Only
                required if request_type is set to LIST

        Returns:
            pandas.DataFrame: DataFrame of results
        """

        ts_range = [_timestamp_from_datetime(dateutil.parser.parse(dt))
                    for dt in ts_range]
        request = self._build_serving_request(feature_set, entity_keys,
                                              request_type, ts_range, limit)
        self._connect_serving()
        return self._response_to_df(feature_set, self._serving_service_stub
                                    .QueryFeatures(request))

    def download_dataset(self, dataset_info, dest, staging_location,
                         file_type=FileType.CSV):
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
            dataset_info.table_id,
            dest,
            staging_location,
            file_type)

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
            dataset_info.table_id,
            staging_location)

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

    def _build_serving_request(self, feature_set, entity_keys, request_type,
                               ts_range, limit):
        """Helper function to build serving service request."""
        request = QueryFeatures.Request(entityName=feature_set.entity,
                                        entityId=entity_keys)
        features = [RequestDetail(featureId=feat_id, type=request_type.value)
                    for feat_id in feature_set.features]

        if request_type == ServingRequestType.LIST:
            ts_range = TimestampRange(start=ts_range[0], end=ts_range[1])
            request.timestampRange.CopyFrom(ts_range)
            for feature in features:
                feature.limit = limit
        request.requestDetails.extend(features)
        return request

    def _response_to_df(self, feature_set, response):
        entity_tables = []
        for entity_key in response.entities:
            feature_tables = []
            features = response.entities[entity_key].features
            for feature_name in features:
                rows = []
                v_list = features[feature_name].valueList
                v_list = getattr(v_list, v_list.WhichOneof("valueList")).val
                for idx in range(len(v_list)):
                    row = {response.entityName: entity_key,
                           feature_name: v_list[idx]}
                    if features[feature_name].HasField("timestampList"):
                        ts_seconds = \
                            features[feature_name].timestampList.val[idx].seconds
                        row["timestamp"] = datetime.fromtimestamp(ts_seconds)
                    rows.append(row)
                feature_tables.append(pd.DataFrame(rows))
            entity_table = feature_tables[0]
            for idx in range(1, len(feature_tables)):
                entity_table = pd.merge(left=entity_table,
                                        right=feature_tables[idx], how='outer')
            entity_tables.append(entity_table)
        if len(entity_tables) == 0:
            return pd.DataFrame(columns=[feature_set.entity, "timestamp"] +
                                        feature_set.features)
        df = pd.concat(entity_tables)
        return df.reset_index(drop=True)

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
            raise TypeError('Apply can only be passed one of the following \
            types: [Feature, Entity, FeatureGroup, Storage, Importer]')

    def _apply_feature(self, feature):
        """Apply the feature to the core API

        Args:
            feature (feast.sdk.resources.feature.Feature): feature to apply
        """
        self._connect_core()
        response = self._core_service_stub.ApplyFeature(feature.spec)
        if self.verbose: print("Successfully applied feature with id: {}\n---\n{}"
                               .format(response.featureId, feature))
        return response.featureId

    def _apply_entity(self, entity):
        """Apply the entity to the core API

        Args:
            entity (feast.sdk.resources.entity.Entity): entity to apply
        """
        self._connect_core()
        response = self._core_service_stub.ApplyEntity(entity.spec)
        if self.verbose:
            print("Successfully applied entity with name: {}\n---\n{}"
                  .format(response.entityName, entity))
        return response.entityName

    def _apply_feature_group(self, feature_group):
        """Apply the feature group to the core API

        Args:
            feature_group (feast.sdk.resources.feature_group.FeatureGroup):
                feature group to apply
        """
        self._connect_core()
        response = self._core_service_stub.ApplyFeatureGroup(feature_group.spec)
        if self.verbose: print("Successfully applied feature group with id: " +
                               "{}\n---\n{}".format(response.featureGroupId,
                                                    feature_group))
        return response.featureGroupId

    def _apply_storage(self, storage):
        """Apply the storage to the core API

        Args:
            storage (feast.sdk.resources.storage.Storage): storage to apply
        """
        self._connect_core()
        response = self._core_service_stub.ApplyStorage(storage.spec)
        if self.verbose: print("Successfully applied storage with id: " +
                               "{}\n{}".format(response.storageId, storage))
        return response.storageId

    def _check_create_dataset_args(self, feature_set, start_date,
                                            end_date, limit):
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
