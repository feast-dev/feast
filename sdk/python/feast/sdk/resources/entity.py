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

import json
import yaml
from feast.specs.EntitySpec_pb2 import EntitySpec
from feast.sdk.utils.print_utils import spec_to_yaml
from google.protobuf.json_format import Parse


class Entity:
    """
    Wrapper class for feast entity
    """

    def __init__(self, name="", description="", tags=[]):
        """
        Create Entity instance.

        Args:
            name (str): name of entity
            description (str): description of entity
            tags (list[str], optional): defaults to []. 
                list of tags for this entity
        """
        self.__spec = EntitySpec(name=name, description=description, tags=tags)

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
        """Create an instance of entity from a yaml file
        
        Args:
            path (str): path to yaml file
        """
        with open(path, 'r') as file:
            content = yaml.safe_load(file.read())
            entity = cls()
            entity.__spec = Parse(
                json.dumps(content), EntitySpec(), ignore_unknown_fields=False)
            return entity

    def create_feature(self, name, value_type, owner, description):
        """Create a feature related to this entity
        
        Args:
            name (str): feature name
            value_type (feast.types.ValueType_pb2.ValueType): value type of
                the feature
            owner (str): owner of the feature
            description (str): feature's description
        """
        pass

    def __str__(self):
        """Print the feature in yaml format
        
        Returns:
            str: yaml formatted representation of the entity
        """
        return spec_to_yaml(self.__spec)

    def dump(self, path):
        """Dump the feature into a yaml file. 
            It will replace content of an existing file.
        
        Args:
            path (str): destination file path
        """
        with open(path, 'w') as file:
            file.write(str(self))
        print("Saved spec to {}".format(path))
