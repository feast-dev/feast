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

import enum
import json

import yaml
from google.protobuf.json_format import Parse

from feast.sdk.utils.print_utils import spec_to_yaml
from feast.specs.FeatureSpec_pb2 import FeatureSpec, DataStores, DataStore


class ValueType(enum.Enum):
    """
    Type of the feature's value
    """
    UNKNOWN = 0
    BYTES = 1
    STRING = 2
    INT32 = 3
    INT64 = 4
    DOUBLE = 5
    FLOAT = 6
    BOOL = 7
    TIMESTAMP = 8


class Feature:
    """
    Wrapper class for feast feature
    """

    def __init__(self,
                 name='',
                 entity='',
                 owner='',
                 value_type=ValueType.DOUBLE,
                 description='',
                 uri='',
                 warehouse_store=None,
                 serving_store=None,
                 group='',
                 tags=[],
                 options={}):
        """Create feast feature instance.
        
        Args:
            name (str): name of feature, in lower snake case
            entity (str): entity the feature belongs to, in lower case
            owner (str): owner of the feature
            value_type (feast.sdk.resources.feature.ValueType): defaults to
                ValueType.DOUBLE. value type of the feature
            description (str): defaults to "". description of the feature
            uri (str): defaults to "". uri pointing to the source code or 
                origin of this feature
            warehouse_store (feast.sdk.resources.feature.Datastore):
                warehouse store id and options
            serving_store (feast.sdk.resources.feature.Datastore): serving
                store id and options
            group (str, optional): feature group to inherit from
            tags (list[str], optional): tags assigned to the feature
            options (dic, optional): additional options for the feature
        """
        id = '{}.{}'.format(entity, name).lower()
        warehouse_store_spec = None
        serving_store_spec = None
        if serving_store is not None:
            serving_store_spec = serving_store.spec
        if warehouse_store is not None:
            warehouse_store_spec = warehouse_store.spec
        data_stores = DataStores(
            serving=serving_store_spec, warehouse=warehouse_store_spec)
        self.__spec = FeatureSpec(
            id=id,
            name=name,
            entity=entity,
            owner=owner,
            dataStores=data_stores,
            description=description,
            uri=uri,
            valueType=value_type.value,
            group=group,
            tags=tags,
            options=options)

    @property
    def spec(self):
        return self.__spec

    @property
    def id(self):
        return self.__spec.id

    @property
    def name(self):
        return self.__spec.name

    @name.setter
    def name(self, value):
        self.__spec.name = value
        id_split = self.id.split('.')
        id_split[1] = value
        self.__spec.id = '.'.join(id_split)

    @property
    def entity(self):
        return self.__spec.entity

    @entity.setter
    def entity(self, value):
        self.__spec.entity = value
        id_split = self.id.split('.')
        id_split[0] = value
        self.__spec.id = '.'.join(id_split)

    @property
    def owner(self):
        return self.__spec.owner

    @owner.setter
    def owner(self, value):
        self.__spec.owner = value

    @property
    def warehouse_store(self):
        return self.__spec.dataStores.warehouse

    @warehouse_store.setter
    def warehouse_store(self, value):
        """Set warehouse store from given Datastore"""
        self.__spec.dataStores.warehouse.CopyFrom(value.spec)

    @property
    def serving_store(self):
        return self.__spec.dataStores.serving

    @serving_store.setter
    def serving_store(self, value):
        """Set serving store from given Datastore"""
        self.__spec.dataStores.serving.CopyFrom(value.spec)

    @property
    def description(self):
        return self.__spec.description

    @description.setter
    def description(self, value):
        self.__spec.description = value

    @property
    def uri(self):
        return self.__spec.uri

    @uri.setter
    def uri(self, value):
        self.__spec.uri = value

    @property
    def value_type(self):
        return ValueType(self.__spec.valueType)

    @value_type.setter
    def value_type(self, value):
        self.__spec.valueType = value

    @property
    def group(self):
        return self.__spec.group

    @group.setter
    def group(self, value):
        self.__spec.group = value

    @property
    def tags(self):
        return self.__spec.tags

    @tags.setter
    def tags(self, value):
        del self.__spec.tags[:]
        self.__spec.tags.extend(value)

    @property
    def options(self):
        return self.__spec.options

    @options.setter
    def options(self, value):
        for key in self.__spec.options:
            del self.__spec.options[key]
        for (key, value) in value.items():
            self.__spec.options[key] = value

    @classmethod
    def from_yaml(cls, path):
        """Create an instance of feature from a yaml spec file
        
        Args:
            path (str): path to yaml spec file
        """

        with open(path, 'r') as file:
            content = yaml.safe_load(file.read())
            feature = cls()
            feature.__spec = Parse(
                json.dumps(content),
                FeatureSpec(),
                ignore_unknown_fields=False)
            return feature

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


class Datastore:
    def __init__(self, id, options={}):
        self.__spec = DataStore(id=id, options=options)

    def __str__(self):
        """Print the datastore in yaml format

        Returns:
            str: yaml formatted representation of the Datastore
        """
        return spec_to_yaml(self.__spec)

    @property
    def spec(self):
        return self.__spec
