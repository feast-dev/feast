"""
Main interface for users to interact with the Core API. 
"""

import grpc
import os

from google.protobuf.timestamp_pb2 import Timestamp

import feast.core.CoreService_pb2_grpc as core
import feast.core.JobService_pb2_grpc as jobs
from feast.core.JobService_pb2 import JobServiceTypes
from feast.core.CoreService_pb2 import CoreServiceTypes
from feast.types.Granularity_pb2 import Granularity as Granularity_pb2
from feast.serving.Serving_pb2 import QueryFeatures, RequestDetail, TimestampRange
import feast.serving.Serving_pb2_grpc as serving

from feast.sdk.env import FEAST_CORE_URL_ENV_KEY, FEAST_SERVING_URL_ENV_KEY
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.entity import Entity
from feast.sdk.resources.storage import Storage
from feast.sdk.resources.feature_group import FeatureGroup
from feast.sdk.resources.feature_set import FeatureSet
from feast.sdk.utils.print_utils import spec_to_yaml
from feast.sdk.utils.types import ServingRequestType



class Client:
    def __init__(self, server_url=None, verbose=False):
        """Create an instance of Feast client which is connected to feast
        endpoint specified in the parameter. If no url is provided, the
        client will default to the url specified in the environment variable
        FEAST_CORE_URL.

        Args:
            server_url (str, optional): feast's endpoint URL
                                  (e.g.: "my.feast.com:8433")
        """

        if server_url is None:
            server_url = os.environ[FEAST_CORE_URL_ENV_KEY]
        self.__channel = grpc.insecure_channel(server_url)
        self.core_service_stub = core.CoreServiceStub(self.__channel)
        self.job_service_stub = jobs.JobServiceStub(self.__channel)
        self.verbose = verbose

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
        response = self.job_service_stub.SubmitJob(request)
        print("Submitted job with id: {}".format(response.jobId))
        return response.jobId

    # def create_feature_set(self, entity, granularity, features):
    #     feature_ids = ('.'.join([entity,
    #                              Granularity_pb2.Enum.Name(granularity.value),
    #                              feature]).lower() for feature in features)
    #     feature_spec_map = self._get_feature_spec_map(feature_ids)
    #     wh_storage_ids = (feature_spec.dataStores.warehouse.id for feature_spec
    #                       in
    #                       feature_spec_map.values())
    #     wh_storage_map = self._get_storage_spec_map(wh_storage_ids)
    #     feature_to_storage_spec_map = {}
    #     for feature_id, feature_spec in feature_spec_map:
    #         feature_to_storage_spec_map[feature_id] = wh_storage_map.get(
    #             feature_spec.dataStores.warehouse.id)
    #
    #     return FeatureSet(feature_to_storage_spec_map)

    def close(self):
        self.channel.close()

    def get_serving_data(self, feature_set, entity_keys, request_type=ServingRequestType.LAST,
                         ts_range=None, limit=10, server_url=None):
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
            server_url (str, optional): (default: None) serving server url. If
                not provided, feast will use the value at FEAST_SERVING_URL

        Returns:
            pandas.DataFrame: DataFrame of results
        """
        if server_url is None:
            server_url = os.environ[FEAST_SERVING_URL_ENV_KEY]
        conn = grpc.insecure_channel(server_url)
        cli = serving.ServingAPIStub(conn)

        ts_range = [_timestamp_from_datetime(dt) for dt in ts_range]
        request = self._build_serving_request(feature_set, entity_keys,
                                              request_type, ts_range, limit)
        response = cli.QueryFeatures(request)

    def _build_serving_request(self, feature_set, entity_keys, request_type,
                               ts_range, limit):
        """Helper function to build serving service request."""
        features = [RequestDetail(featureId=feat_id, type=request_type,
                                  limit=limit) for feat_id in feature_set.features]

        ts_range = TimestampRange(start=ts_range[0], end=ts_range[1])

        return QueryFeatures.Request(entityName=feature_set.entity,
                                     entityId=entity_keys,
                                     requestDetails=features,
                                     timestampRange=ts_range)

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
        response = self.core_service_stub.ApplyFeature(feature.spec)
        if self.verbose: print(
            "Successfully applied feature with id: {}\n---\n{}"
            .format(response.featureId, feature))
        return response.featureId

    def _apply_entity(self, entity):
        """Apply the entity to the core API

        Args:
            entity (feast.sdk.resources.entity.Entity): entity to apply
        """
        response = self.core_service_stub.ApplyEntity(entity.spec)
        if self.verbose: print(
            "Successfully applied entity with name: {}\n---\n{}"
            .format(response.entityName, entity))
        return response.entityName

    def _apply_feature_group(self, feature_group):
        """Apply the feature group to the core API

        Args:
            feature_group (feast.sdk.resources.feature_group.FeatureGroup):
                feature group to apply
        """
        response = self.core_service_stub.ApplyFeatureGroup(feature_group.spec)
        if self.verbose: print("Successfully applied feature group with id: " +
                               "{}\n---\n{}".format(response.featureGroupId,
                                                    feature_group))
        return response.featureGroupId

    def _apply_storage(self, storage):
        """Apply the storage to the core API

        Args:
            storage (feast.sdk.resources.storage.Storage): storage to apply
        """
        response = self.core_service_stub.ApplyStorage(storage.spec)
        if self.verbose: print("Successfully applied storage with id: " +
                               "{}\n{}".format(response.storageId, storage))
        return response.storageId

    def _get_feature_spec_map(self, ids):
        get_features_request = CoreServiceTypes.GetFeaturesRequest(
            ids=ids
        )
        get_features_resp = self.core_service_stub.GetFeatures(
            get_features_request)
        feature_specs = get_features_resp.features
        return {feature_spec.id: feature_spec for feature_spec in
                feature_specs}

    def _get_storage_spec_map(self, ids):
        get_storage_request = CoreServiceTypes.GetStorageRequest(
            ids=ids
        )
        get_storage_resp = self.core_service_stub.GetStorage(
            get_storage_request)
        storage_specs = get_storage_resp.storageSpecs
        return {storage_spec.id: storage_specs for storage_spec in
                storage_specs}


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
