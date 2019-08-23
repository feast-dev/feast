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

import yaml
import json

from feast.specs.FeatureGroupSpec_pb2 import FeatureGroupSpec
from feast.sdk.utils.print_utils import spec_to_yaml
from google.protobuf.json_format import Parse


class FeatureGroup():
    """
    Wrapper class for feast feature group
    """

    def __init__(self, id, tags=[]):
        """Create FeatureGroup instance.

        Args:
            id (str): id of feature group
            tags (list): Defaults to []. tags assigned to feature group
                as well as all children features.
        """
        self.__spec = FeatureGroupSpec(id=id, tags=tags)

    @property
    def spec(self):
        return self.__spec

    @property
    def id(self):
        return self.__spec.id

    @id.setter
    def id(self, value):
        self.__spec.id = value

    @property
    def tags(self):
        return self.__spec.tags

    @tags.setter
    def tags(self, value):
        del self.__spec.tags[:]
        self.__spec.tags.extend(value)

    @classmethod
    def from_yaml(cls, path):
        """Create an instance of feature group from a yaml spec file
        
        Args:
            path (str): path to yaml spec file
        """
        with open(path, 'r') as file:
            content = yaml.safe_load(file.read())
            feature_group = cls.__new__(cls)
            feature_group.__spec = Parse(
                json.dumps(content),
                FeatureGroupSpec(),
                ignore_unknown_fields=False)
            return feature_group

    def __str__(self):
        """Return string representation of the feature group
        
        Returns:
            str: yaml formatted representation of the entity
        """
        return spec_to_yaml(self.__spec)

    def dump(self, path):
        """Dump the feature group into a yaml file. 
            It will replace content of an existing file.
        
        Args:
            path (str): destination file path
        """
        with open(path, 'w') as file:
            file.write(str(self))
        print("Saved spec to {}".format(path))
