import json
import yaml

from google.protobuf.json_format import MessageToJson, Parse

class FeastResource:
    def __init__(self, spec):
        self.__spec = spec

    @property
    def spec(self):
        return self.__spec

    def __str__(self):
        '''Print the feature in yaml format
        
        Returns:
            string: yaml formatted representation of the entity
        '''
        jsonStr = MessageToJson(self.spec)
        return yaml.dump(yaml.load(jsonStr))

    def dump(self, path):
        '''Dump the feature into a yaml file. 
            It will replace content of an existing file.
        
        Args:
            path (str): destination file path
        '''
        with open(path, 'w') as file:
            file.write(str(self))
