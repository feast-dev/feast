import yaml
import json

from feast.specs.StorageSpec_pb2 import StorageSpec
from feast.sdk.utils.print_utils import spec_to_yaml
from google.protobuf.json_format import Parse


class Storage:
    """
    Wrapper class for feast storage
    """
    def __init__(self, id = "", type = "", options={}):
        '''Create Storage instance.

        Args:
            id (str): storage id
            type (str): storage type
            options (dict, optional) : map of storage options
        '''
        self.__spec = StorageSpec(id = id, type = type, options = options)

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
            path (str): path to yaml file
        '''
        with open(path, 'r') as file:
            content = yaml.safe_load(file.read())
            storage = cls()
            storage.__spec = Parse(
                json.dumps(content),
                StorageSpec(),
                ignore_unknown_fields=False)
            return storage

    def __str__(self):
        '''Return string representation the storage in yaml format

        Returns:
            str: yaml formatted representation of the entity
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
        print("Saved spec to {}".format(path))
