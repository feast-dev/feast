import yaml
import json

import feast.specs.EntitySpec_pb2 as entity_pb
from feast.sdk.utils.print_utils import spec_to_yaml

from google.protobuf.json_format import MessageToJson, Parse


'''
Wrapper class for feast entity
'''
class Entity:
    def __init__(self, name="", description="", tags=[]):
        '''Create Entity instance.

        Args:
            name (str): name of entity
            description (str): description of entity
            tags (list): [description] (default: {[]})
        '''
        self.__spec = entity_pb.EntitySpec(name=name, description=description,
            tags=tags)

    @property
    def spec(self):
        return self.__spec

    @property
    def name(self):
        return self.__spec.name

    @name.setter
    def name(self, value):
        self.__spec.name = value

    @property
    def description(self):
        return self.__spec.description

    @description.setter
    def description(self, value):
        self.__spec.description = value

    @property
    def tags(self):
        return self.__spec.tags

    @tags.setter
    def tags(self, value):
        del self.__spec.tags[:]
        self.__spec.tags.extend(value)

    @classmethod
    def from_yaml(cls, path):
        '''Create an instance of entity from a yaml file
        
        Args:
            path (string): path to yaml file
        '''
        with open(path, 'r') as file:
            content = yaml.safe_load(file.read())
            entity = cls()
            entity.__spec = Parse(
                json.dumps(content),
                entity_pb.EntitySpec(),
                ignore_unknown_fields=False)
            return entity

    def create_feature(self, name, granularity, value_type, owner,
                       description):
        '''Create a feature related to this entity
        
        Args:
            name (string): feature name
            granularity (Granularity): granularity of the feature. e.g.: 
                                    Granularity.NONE, Granularity.SECOND, etc
            value_type (ValueType): value type of the feature
            owner (string): owner of the feature
            description (string): feature's description
        '''
        pass

    def __str__(self):
        '''Print the feature in yaml format
        
        Returns:
            string: yaml formatted representation of the entity
        '''
        return spec_to_yaml(self.__spec)

    def dump(self, path):
        '''Dump the feature into a yaml file. 
            It will replace content of an existing file.
        
        Args:
            path (str): destination file path
        '''
        with open(path, 'w') as file:
            file.write(str(self))
        print("Saved spec to {}".format(path))