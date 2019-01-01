"""
Main interface for users to interact with the Core API. 
"""

import grpc
import os
import pandas as pd
import time
import re
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime

import feast.core.CoreService_pb2_grpc as core
import feast.core.JobService_pb2_grpc as jobs
from feast.core.JobService_pb2 import JobServiceTypes
from feast.core.CoreService_pb2 import CoreServiceTypes
from feast.serving.Serving_pb2 import QueryFeatures, RequestDetail, TimestampRange
import feast.serving.Serving_pb2_grpc as serving

from feast.sdk.env import FEAST_CORE_URL_ENV_KEY, FEAST_SERVING_URL_ENV_KEY
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.entity import Entity
from feast.sdk.resources.storage import Storage
from feast.sdk.resources.feature_group import FeatureGroup
from feast.sdk.resources.feature_set import DatasetInfo
from feast.sdk.utils.print_utils import spec_to_yaml
from feast.sdk.utils.types import ServingRequestType
from feast.sdk.utils.bq_util import get_table_name, TrainingDatasetCreator



class Client:
    def __init__(self, core_url=None, serving_url=None, bq_dataset=None,
                 verbose=False):
        """Create an instance of Feast client which is connected to feast
        endpoint specified in the parameter. If no url is provided, the
        client will default to the url specified in the environment variable
        FEAST_CORE_URL.

        Args:
            core_url (str, optional): feast's grpc endpoint URL
                                  (e.g.: "my.feast.com:8433")
            serving_url (str, optional): feast serving's grpc endpoint URL
                                  (e.g.: "my.feast.com:8433")
            bq_dataset (str, optional): BigQuery dataset to be used
                as default training location (e.g. "gcp-project.dataset-name")
        """

        if core_url is None:
            core_url = os.environ[FEAST_CORE_URL_ENV_KEY]
        self._core_url = core_url
        self.__core_channel = grpc.insecure_channel(core_url)

        if serving_url is None:
            serving_url = os.environ[FEAST_SERVING_URL_ENV_KEY]
        self._serving_url = serving_url
        self.__serving_channel = grpc.insecure_channel(serving_url)

        self._serving_service_stub = serving.ServingAPIStub(self.__serving_channel)
        self._core_service_stub = core.CoreServiceStub(self.__core_channel)
        self._job_service_stub = jobs.JobServiceStub(self.__core_channel)
        self._verbose = verbose
        self._bq_dataset = bq_dataset
        self._training_creator = TrainingDatasetCreator()

    @property
    def core_url(self):
        return self._core_url

    @property
    def serving_url(self):
        return self._serving_url

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
        response = self._job_service_stub.SubmitJob(request)
        print("Submitted job with id: {}".format(response.jobId))
        return response.jobId

    def create_training_dataset(self, feature_set, start_date, end_date,
                                limit=None, destination=None,
                                dataset_name=None):
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
            destination (str, optional): (default: None) destination table
                for creating the training dataset. If not specified it will
                create a table inside a dataset specified by bq_dataset.
            dataset_name (str, optional): (default: None) name of the
                training dataset. It is used as table name of the training
                dataset if destination is not specified.
        :return:
            feast.resources.feature_set.DatasetInfo: DatasetInfo containing
            the information of training dataset
        """
        self._check_create_training_dataset_args(feature_set, start_date,
                                                 end_date, limit)

        dataset_name = dataset_name if dataset_name is not None else \
            self._create_dataset_name(feature_set, start_date, end_date)
        destination = destination if destination is not None \
            else self._create_training_table(dataset_name)

        features = feature_set.features
        feature_specs = self._get_feature_spec_map(features)
        wh_storage_ids = (feature_spec.dataStores.warehouse.id for feature_spec
                          in feature_specs.values())
        wh_storage_map = self._get_storage_spec_map(wh_storage_ids)
        feature_table_tuples = []
        for feature_id, feature_spec in feature_specs.items():
            try:
                wh_spec = wh_storage_map.get(
                    feature_spec.dataStores.warehouse.id)
                if wh_spec.type is not "bigquery":
                    ValueError("feature set contains feature which has "
                               "warehouse store other than bigquery")
            except KeyError:
                raise ValueError("feature set contains feature which doesn't "
                                 "have warehouse store")
            feature_table_tuples.append((feature_id,
                                         get_table_name(feature_id, wh_spec)))

        table = self._training_creator.create_training_dataset(
                                                       feature_table_tuples,
                                                       start_date,
                                                       end_date,
                                                       limit,
                                                       destination)

        if self.verbose:
            print("Training dataset has been created in: {}".format(
                destination))

        return DatasetInfo(dataset_name, table)

    def close(self):
        self.__core_channel.close()
        self.__serving_channel.close()

    def get_serving_data(self, feature_set, entity_keys, request_type=ServingRequestType.LAST,
                         ts_range=None, limit=10):
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
            ts_range (:obj: `list` of :obj: `datetime.datetime`, optional): size
                2 list of start timestamp and end timestamp. Only required if
                request_type is set to LIST
            limit (int, optional): (default: 10) number of values to get. Only
                required if request_type is set to LIST

        Returns:
            pandas.DataFrame: DataFrame of results
        """

        ts_range = [_timestamp_from_datetime(dt) for dt in ts_range]
        request = self._build_serving_request(feature_set, entity_keys,
                                              request_type, ts_range, limit)
        return self._response_to_df(self._serving_service_stub
                                    .QueryFeatures(request))

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

    def _response_to_df(self, response):
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
        response = self._core_service_stub.ApplyFeature(feature.spec)
        if self.verbose: print("Successfully applied feature with id: {}\n---\n{}"
                               .format(response.featureId, feature))
        return response.featureId

    def _apply_entity(self, entity):
        """Apply the entity to the core API

        Args:
            entity (feast.sdk.resources.entity.Entity): entity to apply
        """
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
        response = self._core_service_stub.ApplyStorage(storage.spec)
        if self.verbose: print("Successfully applied storage with id: " +
                               "{}\n{}".format(response.storageId, storage))
        return response.storageId

    def _get_feature_spec_map(self, ids):
        get_features_request = CoreServiceTypes.GetFeaturesRequest(
            ids=ids
        )
        get_features_resp = self._core_service_stub.GetFeatures(
            get_features_request)
        feature_specs = get_features_resp.features
        return {feature_spec.id: feature_spec for feature_spec in
                feature_specs}

    def _get_storage_spec_map(self, ids):
        get_storage_request = CoreServiceTypes.GetStorageRequest(
            ids=ids
        )
        get_storage_resp = self._core_service_stub.GetStorage(
            get_storage_request)
        storage_specs = get_storage_resp.storageSpecs
        return {storage_spec.id: storage_spec for storage_spec in
                storage_specs}

    def _create_training_table(self, dataset_name):
        return ".".join([self._bq_dataset, dataset_name])

    def _create_dataset_name(self, feature_set, start_date, end_date):
        dataset_name = "_".join([feature_set.entity, start_date, end_date,
                         str(round(time.time()))])
        return re.sub('[^0-9a-zA-Z_]+', '', dataset_name)

    def _check_create_training_dataset_args(self, feature_set, start_date,
                                            end_date, limit):
        if len(feature_set.features) < 1:
            raise ValueError("feature set is empty")

        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        if end < start:
            raise ValueError("end_date is before start_date")

        if limit is not None and limit < 1:
            raise ValueError("limit is not a positive integer")

    def _parse_date(self, date):
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
