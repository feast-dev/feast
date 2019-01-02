from google.protobuf.json_format import MessageToDict

from collections import OrderedDict
import yaml


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