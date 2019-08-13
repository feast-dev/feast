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
import logging
import os
from datetime import datetime

import feast.sdk.utils.types
import grpc
import numpy as np
import pandas as pd
from feast.core.CoreService_pb2 import CoreServiceTypes
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.DatasetService_pb2 import DatasetServiceTypes
from feast.core.DatasetService_pb2_grpc import DatasetServiceStub
from feast.core.JobService_pb2 import JobServiceTypes
from feast.core.JobService_pb2_grpc import JobServiceStub
from feast.sdk.env import FEAST_CORE_URL_ENV_KEY, FEAST_SERVING_URL_ENV_KEY
from feast.sdk.resources.entity import Entity
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.feature_group import FeatureGroup
from feast.sdk.resources.feature_set import DatasetInfo, FileType
from feast.sdk.utils import types
from feast.sdk.utils.bq_util import TableDownloader
from feast.sdk.utils.print_utils import spec_to_yaml
from feast.sdk.utils.types import dtype_to_feast_value_attr
from feast.serving.Serving_pb2 import QueryFeaturesRequest
from feast.serving.Serving_pb2_grpc import ServingAPIStub
from feast.specs.EntitySpec_pb2 import EntitySpec
from feast.specs.FeatureSpec_pb2 import FeatureSpec
from feast.types import Feature_pb2
from feast.types.FeatureRow_pb2 import FeatureRow
from feast.types.Value_pb2 import Value
from google.protobuf.timestamp_pb2 import Timestamp
from kafka import KafkaProducer
from pandas.core.dtypes.common import is_datetime64_any_dtype
from tqdm import tqdm


def _feast_core_apply_entity_stub(entity):
    pass


def _feast_core_apply_features_stub(features):
    pass


def _feast_core_get_message_endpoints_stub(entity):
    return None, None


class FeatureRowProducer(object):
    def __init__(self, x, y) -> None:
        pass

    def send(self, dataframe):
        pass


class Client:
    def get_topic(self):
        return self._core_service_stub.GetTopic()

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
        self._core_service_stub: CoreServiceStub = None
        self._job_service_stub = None
        self._dataset_service_stub = None
        self._serving_service_stub = None
        self._message_producer = None

        self._verbose = verbose
        self._table_downloader = TableDownloader()
        self.logger = logging.getLogger(__name__)

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
        if apply_entity:
            self._apply_entity(importer.entity)
        if apply_features:
            for feature in importer.features:
                self._apply_feature(importer.features[feature])

        if importer.require_staging:
            print("Staging file to remote path {}".format(importer.remote_path))
            importer.stage(feast_client=self)

        request = JobServiceTypes.SubmitImportJobRequest(importSpec=importer.spec)
        if name_override is not None:
            request.name = name_override

        print("Submitting job with spec:\n {}".format(spec_to_yaml(importer.spec)))
        self._connect_core()
        response = self._job_service_stub.SubmitJob(request)
        print("Submitted job with id: {}".format(response.jobId))
        return response.jobId

    def create_dataset(
        self,
        feature_set,
        start_date,
        end_date,
        limit=None,
        name_prefix=None,
        filters=None,
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
            filters (dict, optional): (default: None) conditional clause
                that will be used to filter dataset. Keys of filters could be
                feature id or job_id.
        :return:
            feast.resources.feature_set.DatasetInfo: DatasetInfo containing
            the information of training dataset.
        """
        self._check_create_dataset_args(
            feature_set, start_date, end_date, limit, filters
        )

        conv_filters = None
        if filters is not None:
            conv_filters = {}
            for k, v in filters.items():
                conv_filters[str(k)] = str(v)

        req = DatasetServiceTypes.CreateDatasetRequest(
            featureSet=feature_set.proto,
            startDate=_timestamp_from_datetime(_parse_date(start_date)),
            endDate=_timestamp_from_datetime(_parse_date(end_date)),
            limit=limit,
            namePrefix=name_prefix,
            filters=conv_filters,
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

    def _ensure_valid_timestamp_in_dataframe(self, dataframe, timestamp_column=None):
        """
        Helper method to ensure the DataFrame has valid column containing feature timestamps.

        If timestamp_column is set to "None", a new column "_event_timestamp" will be
        created with value equal to the current time. All the features in the DataFrame
        will then have the same timestamp value of current time.

        Args:
            dataframe (pandas.DataFrame): DataFrame containing features
            timestamp_column (:obj:`str`, optional): Column in the DataFrame representing feature timestamp

        Returns:
            The "timestamp_column" passed in as argument or the newly created
            "timestamp_column" if the argument is "None". The return value can be used
            by the caller to know the "timestamp_column" created by this method, if any.

        """
        if timestamp_column is None:
            self.logger.info(
                'No "timestamp_column" is specified, Feast will assign current '
                "time as the event timestamp for the features"
            )
            dataframe["_event_timestamp"] = datetime.now().astimezone()
            timestamp_column = "_event_timestamp"
        elif timestamp_column not in dataframe.columns:
            raise ValueError(
                f'timestamp_column "{timestamp_column}" does not exist in the Dataframe'
            )

        # If user provides value with type "datetime64[ns]" i.e. no timezone info
        # Feast will assume it's using the user local timezone
        if dataframe[timestamp_column].dtype == np.dtype("datetime64[ns]"):
            local_timezone = datetime.now().astimezone().tzinfo
            dataframe[timestamp_column] = dataframe[timestamp_column].dt.tz_localize(
                tz=local_timezone
            )

        # Ensure timestamp value is in format that Feast accepts.
        # For example the timestamp_column may be in these types:
        # - datetime64[ns, tz]
        # - object (if the value is in string)
        # pandas.to_datetime ensures that it is normalized to datetime64[ns, UTC]
        dataframe[timestamp_column] = pd.to_datetime(
            dataframe[timestamp_column], utc=True
        )
        return timestamp_column

    def load_features_from_dataframe(
        self,
        dataframe: pd.DataFrame,
        entity_name: str,
        entity_key_column: str,
        timestamp_column: str = None,
        entity_description: str = "",
        entity_tags: list = None,
        timeout: int = None,
    ):
        """
        Write features from pandas DataFrame to Feast.

        The caller needs to specify the entity name this DataFrame represents and
        which column contains the key value for the entity. If "timestamp_column"
        is None then all features will be assigned the same default timestamp
        value of current time.

        All columns in the dataframe, except "timestamp_column" and "entity_key_column"
        will be treated as features in Feast, with column name as the feature name.
        If only a subset of columns or custom feature names are required, user is
        required to modify the DataFrame metadata (e.g. removing/renaming the
        columns) before calling this method.

        This method will publish all features to the message broker and wait until
        it receives confirmation from the broker that the features are received
        successfully. If timeout is set to "None" and the broker gets
        disconnected during transfer, the process may seem to hang. It is therefore
        recommended to set a timeout value (in seconds).

        Args:
            dataframe (pandas.Dataframe): DataFrame containing the features to load into Feast.
            entity_name (str): The name of the entity represented by this DataFrame.
            entity_key_column (str): Column in the DataFrame that contains the key value for the entity.
            timestamp_column (:obj:`str`,optional): Column in the DataFrame that contains timestamp value for a feature.
            entity_description (str): Human friendly description of the entity represented by the DataFrame.
            entity_tags (:obj:`list`,optional): The tags to assign to this entity. Tags can be used e.g. for filtering.
            timeout (:obj:`int`,optional): how long (in seconds) should the client wait for all the features to be
                published successfully. If set to "None", the client will wait indefinitely.

        Returns:
            NoneType: If all features loaded successfully

        Raises:
            ValueError: If the method is called with invalid arguments or the DataFrame contains invalid values
            TypeError: If there is a type mismatch between the actual type and the inferred type from the DataFrame
        """
        if entity_key_column not in dataframe.columns:
            raise ValueError(
                f'entity_key_column "{entity_key_column}" does not exist in the Dataframe'
            )

        # Ensure there is a column representing timestamp in the DataFrame
        timestamp_column = self._ensure_valid_timestamp_in_dataframe(
            dataframe, timestamp_column=timestamp_column
        )

        # Ensure that all "datetime64" columns can be mapped to Feast value type
        # All "datetime64" columns will be converted to type "datetime64[ns]"
        # which can be mapped to Feast value type
        #
        # If user provides value with type "datetime64[ns]" i.e. no timezone info
        # Feast will assume it's using the user local timezone
        for column in dataframe.columns:
            if column == entity_key_column or column == timestamp_column:
                continue
            # TODO: Refactor duplication with parts from "_ensure_valid_timestamp_in_dataframe" function
            if is_datetime64_any_dtype(dataframe[column]):
                if dataframe[column].dtype == np.dtype("datetime64[ns]"):
                    # Column has no timezone info so we assume it's local timezone
                    local_timezone = datetime.now().astimezone().tzinfo
                    dataframe[column] = dataframe[column].dt.tz_localize(
                        tz=local_timezone
                    )
                # Normalize to UTC timezone
                dataframe[column] = pd.to_datetime(dataframe[column], utc=True)

        if entity_tags is None:
            entity_tags = []

        if self._core_service_stub is None:
            self._connect_core()

        entity_spec = EntitySpec(
            name=entity_name, description=entity_description, tags=entity_tags
        )
        self.logger.info(f"Registering entity\n{entity_spec}")
        self._core_service_stub.ApplyEntity(entity_spec)

        apply_features_request = CoreServiceTypes.ApplyFeaturesRequest()
        for column in dataframe.columns:
            if column == entity_key_column or column == timestamp_column:
                continue
            feature_name = column
            feature_spec = FeatureSpec(
                id=f"{entity_name}.{feature_name}",
                name=feature_name,
                valueType=feast.sdk.utils.types.dtype_to_value_type(
                    dataframe[column].dtype
                ),
                entity=entity_name,
            )
            apply_features_request.featureSpecs.extend([feature_spec])
        self.logger.info(f"Registering features\n{apply_features_request.featureSpecs}")
        self._core_service_stub.ApplyFeatures(apply_features_request)

        get_topic_response = self._core_service_stub.GetTopic(
            CoreServiceTypes.GetTopicRequest(entityName=entity_name)
        )
        bootstrap_servers, topic_name = (
            get_topic_response.messageBrokerURI,
            get_topic_response.topicName,
        )

        if self._message_producer is None:
            # Feast 0.2 only supports Kafka brokers
            self._message_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

        self.logger.info(
            f"Publishing features to topic: '{topic_name}' in brokers: '{bootstrap_servers}'"
        )
        for index, row in tqdm(
            dataframe.iterrows(), unit="rows", total=dataframe.shape[0]
        ):
            event_timestamp = Timestamp()
            event_timestamp.FromNanoseconds(row[timestamp_column].value)
            feature_row = FeatureRow(
                entityKey=str(row[entity_key_column]), eventTimestamp=event_timestamp
            )
            for column in row.index:
                if column == entity_key_column or column == timestamp_column:
                    continue
                feature_name = column
                feature_value = Value()
                feature_value_attr = dtype_to_feast_value_attr(dataframe[column].dtype)
                if feature_value_attr == "timestampVal":
                    timestamp_value = Timestamp()
                    timestamp_value.FromNanoseconds(row[column].value)
                    feature_value.timestampVal.CopyFrom(timestamp_value)
                else:
                    try:
                        feature_value.__setattr__(feature_value_attr, row[column])
                    except TypeError as type_error:
                        # Numpy treats NaN as float. So if there is NaN values in column of
                        # "str" type, __setattr__ will raise TypeError. This handles that case.
                        if feature_value_attr == "stringVal" and pd.isnull(row[column]):
                            feature_value.__setattr__("stringVal", "")
                        else:
                            raise type_error
                feature_row.features.extend(
                    [
                        Feature_pb2.Feature(
                            id=f"{entity_name}.{feature_name}", value=feature_value
                        )
                    ]
                )
            self._message_producer.send(
                topic_name,
                feature_row.SerializeToString(),
                timestamp_ms=int(row[timestamp_column].value // 1e6),
            )

        # Wait for all messages to be completely sent
        self._message_producer.flush(timeout=timeout)
        return

    def get_serving_data(self, feature_set, entity_keys, ts_range=None):
        """
        Get feature value from feast serving API.

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
        self, dataset_info, dest, staging_location=None, file_type=FileType.CSV
    ):
        """
        Download training dataset as file
        Args:
            dataset_info (feast.sdk.resources.feature_set.DatasetInfo) :
                dataset_info to be downloaded
            dest (str): destination's file path
            staging_location (str, optional): url to staging_location (currently
                support a folder in GCS)
            file_type (feast.sdk.resources.feature_set.FileType): (default:
                FileType.CSV) exported file format
        Returns:
            str: path to the downloaded file
        """
        return self._table_downloader.download_table_as_file(
            dataset_info.full_table_id, dest, file_type, staging_location
        )

    def download_dataset_to_df(self, dataset_info, staging_location=None):
        """
        Download training dataset as Pandas Dataframe
        Args:
            dataset_info (feast.sdk.resources.feature_set.DatasetInfo) :
                dataset_info to be downloaded
            staging_location(str, optional): url to staging_location (currently
                support a folder in GCS)

        Returns: pandas.DataFrame: dataframe of the training dataset

        """
        return self._table_downloader.download_table_as_df(
            dataset_info.full_table_id, staging_location
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
        else:
            raise TypeError(
                "Apply can only be passed one of the following \
            types: [Feature, Entity, FeatureGroup, Importer]"
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

    def _check_create_dataset_args(
        self, feature_set, start_date, end_date, limit, filters
    ):
        if len(feature_set.features) < 1:
            raise ValueError("feature set is empty")

        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if end < start:
            raise ValueError("end_date is before start_date")

        if limit is not None and limit < 1:
            raise ValueError("limit is not a positive integer")

        if filters is not None and not isinstance(filters, dict):
            raise ValueError("filters is not dictionary")


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
