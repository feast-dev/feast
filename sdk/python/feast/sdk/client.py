"""
Main interface for users to interact with the Core API. 
"""

import feast.core.CoreService_pb2_grpc as core
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.entity import Entity
from feast.sdk.resources.feature_group import FeatureGroup
import grpc


class Client:
    def __init__(self, serverURL):
        '''Create an instance of Feast client which is connected to feast 
        endpoint specified in the parameter
        
        Args:
            serverURI (string): feast's endpoint URL 
                                  (e.g.: "my.feast.com:8433")
        '''

        self.channel = grpc.insecure_channel(serverURL)
        self.stub = core.CoreServiceStub(self.channel)

    def apply(self, obj, create_entity=False, create_features=False):
        '''Create or update one or many feast's resource 
        (feature, entity, importer, storage).
        
        Args:
            object (object): one or many feast's resource
            create_entity (bool, optional):  (default: {None})
            create_features (bool, optional): [description] (default: {None})
        '''

        # object can be a single object or an interable
        if isinstance(obj, list):
            for resource in obj:
                self._apply(resource)
        else:
            self._apply(obj)

        # object can also be: Feature, Entity, Importer, Storage

        pass

    def _apply(self, obj):
        '''Applies a single object to feast core.
        
        Args:
            obj (object): one of 
                           [Feature, Entity, FeatureGroup, Storage, Importer]
        '''
        if isinstance(obj, Feature):
            print(self._apply_feature(obj).featureId)
        elif isinstance(obj, Entity):
            print("entity")
        elif isinstance(obj, FeatureGroup):
            print("fgroup")
        else:
            raise TypeError('Apply can only be passed one of the following \
            types: [Feature, Entity, FeatureGroup, Storage, Importer]')

    def _apply_feature(self, feature):
        '''Apply the feature to the core API
        
        Args:
            feature (Feature): feature to apply
        '''
        response = self.stub.ApplyFeature(feature.spec)
        return response

    def _apply_entity(self, entity):
        '''Apply the entity to the core API
        
        Args:
            entity (Entity): entity to apply
        '''
        response = self.stub.ApplyEntity(entity.spec)
        return response

    def close(self):
        self.channel.close()

    # def create_feature_set(self, entity=None, granularity=None, features=None):
    #     return feature_set()
