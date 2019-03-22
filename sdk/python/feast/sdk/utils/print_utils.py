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

from collections import OrderedDict

import yaml
from google.protobuf.json_format import MessageToDict


def spec_to_yaml(spec):
    '''Converts spec to yaml string
    
    Args:
        spec (google.protobuf.Message): feast spec object
    
    Returns:
        str: yaml string representation of spec
    '''
    mapping_tag = yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG
    yaml.add_representer(OrderedDict, _dict_representer)
    yaml.add_constructor(mapping_tag, _dict_constructor)
    dic = OrderedDict(MessageToDict(spec))
    return yaml.dump(dic, default_flow_style=False)


def _dict_representer(dumper, data):
    return dumper.represent_dict(data.items())


def _dict_constructor(loader, node):
    return OrderedDict(loader.construct_pairs(node))
