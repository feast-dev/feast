import yaml
import json

import feast.specs.StorageSpec_pb2 as storage_pb
import feast.specs.FeatureSpec_pb2 as feature_pb
from feast.sdk.utils.print_utils import spec_to_yaml
from google.protobuf.json_format import MessageToJson, Parse

'''
Wrapper class for feast storage
'''
class Storage:
    def __init__(self, id = "", type = "", options={}):
        '''Create Storage instance.

        Args:
            id (str): storage id
            type (str): storage type
            options (dict) : map of storage options
        '''
        self.__spec = storage_pb.StorageSpec(id = id, type = type, options = options)

    @property
    def spec(self):
        return self.__spec

    @property
    def id(self):
        return self.__spec.id

    @id.setter
    def id(self, value):
        self.__spec.name = id

    @property
    def type(self):
        return self.__spec.type

    @type.setter
    def type(self, value):
        self.__spec.type = value

    @property
    def options(self):
        return self.__spec.options

    @options.setter
    def options(self, value):
        self.__spec.options.clear()
        self.__spec.options.update(value)

    @classmethod
    def from_yaml(cls, path):
        '''Create an instance of storage from a yaml file
        
        Args:
            path (string): path to yaml file
        '''
        with open(path, 'r') as file:
            content = yaml.safe_load(file.read())
            storage = cls()
            storage.__spec = Parse(
                json.dumps(content),
                storage_pb.StorageSpec(),
                ignore_unknown_fields=False)
            return storage

    def __str__(self):
        '''Return string representation the storage in yaml format
        
        Returns:
            string: yaml formatted representation of the entity
        '''
        return spec_to_yaml(self.__spec)

    def dump(self, path):
        '''Dump the storage into a yaml file. 
            It will replace content of an existing file.
        
        Args:
            path (str): destination file path
        '''
        with open(path, 'w') as file:
            file.write(str(self))


class Datastore:
    def __init__(self, id, options={}):
        self.__spec = feature_pb.DataStore(id = id, options = options)

    def __str__(self):
        '''Print the datastore in yaml format
        
        Returns:
            string: yaml formatted representation of the Datastore
        '''
        return spec_to_yaml(self.__spec)

    @property
    def spec(self):
        return self.__spec