#
#  Copyright 2019 The Feast Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from os.path import expanduser, join
import logging
import os
import sys
from typing import Dict
from urllib.parse import urlparse
from urllib.parse import ParseResult

import toml

_logger = logging.getLogger(__name__)

feast_configuration_properties = {"core_url": "URL", "serving_url": "URL"}

CONFIGURATION_FILE_DIR = ".feast"
CONFIGURATION_FILE_NAME = "config.toml"


def get_or_create_config() -> Dict:
    """
    Creates or gets the Feast users active configuration
    :return: dictionary of Feast properties
    """

    user_config_file_dir, user_config_file_path = _get_config_file_locations()

    if not os.path.exists(os.path.dirname(user_config_file_dir)):
        os.makedirs(os.path.dirname(user_config_file_dir))

    if not os.path.isfile(user_config_file_path):
        _save_config(user_config_file_path, _fproperties_to_dict())

    try:
        return toml.load(user_config_file_path)
    except FileNotFoundError:
        _logger.error(
            "Could not find Feast configuration file " + user_config_file_path
        )
        sys.exit(1)
    except toml.decoder.TomlDecodeError:
        _logger.error(
            "Could not decode Feast configuration file " + user_config_file_path
        )
        sys.exit(1)
    except Exception as e:
        _logger.error(e)
        sys.exit(1)


def set_property(fproperty: str, value: str):
    """
    Sets a single property in the Feast users local configuration file
    :param fproperty: Feast property name
    :param value: Feast property value
    """

    if _is_valid_property(fproperty, value):
        active_feast_config = get_or_create_config()
        active_feast_config[fproperty] = value
        _, user_config_file_path = _get_config_file_locations()
        _save_config(user_config_file_path, active_feast_config)
        print("Updated property [%s]" % fproperty)
    else:
        _logger.error("Invalid property selected")
        sys.exit(1)


def get_config_property_or_fail(fproperty):
    active_feast_config = get_or_create_config()
    if _is_valid_property(fproperty, active_feast_config[fproperty]):
        return active_feast_config[fproperty]
    _logger.error("Could not load Feast property from configuration: %s" % fproperty)
    sys.exit(1)


def _fproperties_to_dict() -> Dict[str, str]:
    prop_dict = {}
    for fproperty in feast_configuration_properties:
        prop_dict[fproperty] = ""
    return prop_dict


def _is_valid_property(fproperty: str, value: str) -> bool:
    """
    Validates both a Feast property as well as value
    :param fproperty: Feast property name
    :param value: Feast property value
    :return: Returns True if property and value are valid
    """

    if fproperty not in feast_configuration_properties:
        _logger.error("You are trying to set an invalid property")
        sys.exit(1)

    fprop_type = feast_configuration_properties[fproperty]

    if fprop_type == "URL":
        if "//" not in value:
            value = "%s%s" % ("grpc://", value)
        parsed_value = urlparse(value)  # type: ParseResult
        if parsed_value.netloc:
            return True

    _logger.error("The property you are trying to set could not be identified")
    sys.exit(1)


def _save_config(user_config_file_path: str, config_string: Dict[str, str]):
    """
    Saves Feast configuration
    :param user_config_file_path: Local file system path to save configuration
    :param config_string: Contents in dictionary format to save to path
    """

    try:
        with open(user_config_file_path, "w+") as f:
            toml.dump(config_string, f)
    except Exception as e:
        _logger.error("Could not update configuration file for Feast")
        print(e)
        sys.exit(1)


def _get_config_file_locations() -> (str, str):
    user_config_file_dir = join(expanduser("~"), CONFIGURATION_FILE_DIR)
    user_config_file_path = join(user_config_file_dir, CONFIGURATION_FILE_NAME)
    return user_config_file_dir, user_config_file_path
