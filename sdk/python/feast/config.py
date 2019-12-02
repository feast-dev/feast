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

CONFIGURATION_FILE_DIR = os.environ.get("FEAST_CONFIG", ".feast")
CONFIGURATION_FILE_NAME = "config.toml"


def _get_or_create_config() -> Dict:
    """Get user configuration file or create it and return"""

    user_config_file_dir, user_config_file_path = _get_config_file_locations()
    user_config_file_dir = user_config_file_dir.rstrip("/") + "/"
    if not os.path.exists(os.path.dirname(user_config_file_dir)):
        os.makedirs(os.path.dirname(user_config_file_dir))

    if not os.path.isfile(user_config_file_path):
        _save_config(user_config_file_path, _props_to_dict())

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


def set_property(prop: str, value: str):
    """
    Sets a single property in the Feast users local configuration file

    Args:
        prop: Feast property name
        value: Feast property value
    """
    if _is_valid_property(prop, value):
        active_feast_config = _get_or_create_config()
        active_feast_config[prop] = value
        _, user_config_file_path = _get_config_file_locations()
        _save_config(user_config_file_path, active_feast_config)
        print("Updated property [%s]" % prop)
    else:
        _logger.error("Invalid property selected")
        sys.exit(1)


def get_config_property_or_fail(prop: str, force_config: Dict[str, str] = None) -> str:
    """
    Gets a single property in the users configuration

    Args:
        prop: Property to retrieve
        force_config: Configuration dictionary containing properties that should
            be overridden. This will take precedence over file based properties.

    Returns:
        Returns a string property
    """
    if (
        isinstance(force_config, dict)
        and prop in force_config
        and force_config[prop] is not None
    ):
        return force_config[prop]

    active_feast_config = _get_or_create_config()
    if _is_valid_property(prop, active_feast_config[prop]):
        return active_feast_config[prop]
    _logger.error("Could not load Feast property from configuration: %s" % prop)
    sys.exit(1)


def _props_to_dict() -> Dict[str, str]:
    """Create empty dictionary of all Feast properties"""
    prop_dict = {}
    for prop in feast_configuration_properties:
        prop_dict[prop] = ""
    return prop_dict


def _is_valid_property(prop: str, value: str) -> bool:
    """
    Validates both a Feast property as well as value

    Args:
        prop: Feast property name
        value: Feast property value

    Returns:
        Returns True if property and value are valid
    """
    if prop not in feast_configuration_properties:
        _logger.error("You are trying to set an invalid property")
        sys.exit(1)

    prop_type = feast_configuration_properties[prop]

    if prop_type == "URL":
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

    Args:
        user_config_file_path: Local file system path to save configuration
        config_string: Contents in dictionary format to save to path
    """
    try:
        with open(user_config_file_path, "w+") as f:
            toml.dump(config_string, f)
    except Exception as e:
        _logger.error("Could not update configuration file for Feast")
        print(e)
        sys.exit(1)


def _get_config_file_locations() -> (str, str):
    """Gets the local user configuration directory and file path"""
    user_config_file_dir = join(expanduser("~"), CONFIGURATION_FILE_DIR)
    user_config_file_path = join(user_config_file_dir, CONFIGURATION_FILE_NAME)
    return user_config_file_dir, user_config_file_path
