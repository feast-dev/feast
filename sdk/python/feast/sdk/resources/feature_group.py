import yaml
import json

import feast.specs.FeatureSpec_pb2 as feature_pb
import feast.specs.FeatureGroupSpec_pb2 as feature_group_pb
from feast.sdk.resources.resource import FeastResource

from google.protobuf.json_format import MessageToJson, Parse

'''
Wrapper class for feast feature groups
'''
class FeatureGroup(FeastResource):
    def __init__(self, id="", tags=[], warehouse_store=feature_pb.DataStore(),
        serving_store=feature_pb.DataStore()):
        '''
        Args:
            id (str): id of feature group
            warehouse_store (FeatureSpec_pb2.DataStore): warehouse store id and options
            serving_store (FeatureSpec_pb2.DataStore): serving store id and options
            tags (list): Defaults to []. tags assigned to feature group
                           as well as all children features.
        '''

        self.__spec = feature_group_pb.FeatureGroupSpec(id=id, tags=tags,
            dataStores=feature_pb.DataStores(
                warehouse=warehouse_store, serving=serving_store))

    @property
    def id(self):
        return self.__spec.id
    
    @id.setter
    def id(self, value):
        self.__spec.id = value
    
    @property
    def warehouse_store(self):
        return self.__spec.dataStores.warehouse

    @warehouse_store.setter
    def warehouse_store(self, value):
        self.__spec.dataStores.serving.CopyFrom(value)

    @property
    def serving_store(self):
        return self.__spec.dataStores.serving

    @serving_store.setter
    def serving_store(self, value):
        self.__spec.dataStores.warehouse.CopyFrom(value)
    
    @property
    def tags(self):
        return self.__spec.tags

    @tags.setter
    def tags(self, value):
        del self.__spec.tags[:]
        self.__spec.tags.extend(value)

    @classmethod
    def from_yaml_file(cls, path):
        '''Create an instance of feature group from a yaml spec file
        
        Args:
            path (str): path to yaml spec file
        '''
        with open(path, 'r') as file:
            content = yaml.safe_load(file.read())
            feature_group = cls()
            feature_group.__spec = Parse(
                json.dumps(content),
                feature_group_pb.FeatureGroupSpec(),
                ignore_unknown_fields=False)
            return feature_group