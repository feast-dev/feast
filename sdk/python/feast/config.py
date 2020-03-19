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
from configparser import ConfigParser
from os.path import expanduser, join
import logging
import os
import sys
from typing import Dict, Union
from urllib.parse import urlparse
from urllib.parse import ParseResult
from typing import TextIO
import toml
from typing import Optional

_logger = logging.getLogger(__name__)

FEAST_DEFAULT_OPTIONS = {
    "CORE_URL": "localhost:6565",
    "SERVING_URL": "localhost:6565"
}

CONFIGURATION_FILE_DIR = os.environ.get("FEAST_CONFIG", ".feast")
CONFIGURATION_FILE_NAME = "config"
CONFIGURATION_FILE_SECTION = "general"
FEAST_ENV_VAR_PREFIX = 'FEAST_'


def _init_config(path: str):
    """
    Returns a ConfigParser that reads in a feast configuration file. If the
    file does not exist it will be created.

    Args:
        path: Optional path to initialize as Feast configuration

    Returns: ConfigParser of the Feast configuration file, with defaults
    preloaded

    """
    # Create the configuration file directory if needed
    config_dir = os.path.dirname(path)
    config_dir = config_dir.rstrip("/") + "/"

    if not os.path.exists(os.path.dirname(config_dir)):
        os.makedirs(os.path.dirname(config_dir))

    # Create the configuration file itself
    config = ConfigParser(defaults=FEAST_DEFAULT_OPTIONS)
    if os.path.exists(path):
        config.read(path)

    # Store all configuration in a single section
    if not config.has_section(CONFIGURATION_FILE_SECTION):
        config.add_section(CONFIGURATION_FILE_SECTION)

    # Save the current configuration
    config.write(open(path, 'w'))

    return config


def _get_feast_env_vars():
    """
    Get environmental variables that start with FEAST_
    Returns: Dict of Feast environmental variables (stripped of prefix)
    """
    feast_env_vars = {}
    for key in os.environ.keys():
        if key.upper().startswith(FEAST_ENV_VAR_PREFIX):
            feast_env_vars[key[len(FEAST_ENV_VAR_PREFIX):]] = os.environ[key]
    return feast_env_vars


class Config:
    """
    Maintains and provides access to Feast configuration

    Configuration is stored as key/value pairs. The user can specify options
    through either input arguments to this class, environmental variables, or
    by setting the config in a configuration file

    """

    def __init__(self,
        options: Optional[Dict[str, str]] = None,
        path: Optional[str] = None,
    ):
        """
        Configuration options are returned as follows (higher replaces lower)
        1. Initialized options ("options" argument)
        2. Environmental variables (reloaded on every "get")
        3. Configuration file options (loaded once)
        4. Default options (loaded once from memory)

        Args:
            options: (optional) A list of initialized/hardcoded options.
            path: (optional) File path to configuration file
        """
        if not path:
            path = join(expanduser("~"), CONFIGURATION_FILE_DIR,
                        CONFIGURATION_FILE_NAME)

        config = _init_config(path)

        self._options = {}
        if options and isinstance(options, dict):
            self._options = options
        self._config = config  # type: ConfigParser

    def get(self, option):
        """
        Returns a single configuration option as a string

        Args:
            option: Name of the option

        Returns: String option that is returned

        """
        return self._config.get(CONFIGURATION_FILE_SECTION, option,
                                vars={**_get_feast_env_vars(),
                                      **self._options})

    def getboolean(self, option):
        """
         Returns a single configuration option as a boolean

         Args:
             option: Name of the option

         Returns: Boolean option value that is returned

         """
        return self._config.getboolean(CONFIGURATION_FILE_SECTION, option,
                                       vars={**_get_feast_env_vars(),
                                             **self._options})

    def getint(self, option):
        """
         Returns a single configuration option as an integer

         Args:
             option: Name of the option

         Returns: Integer option value that is returned

         """
        return self._config.getint(CONFIGURATION_FILE_SECTION, option,
                                   vars={**_get_feast_env_vars(),
                                         **self._options})

    def getfloat(self, option):
        """
         Returns a single configuration option as an integer

         Args:
             option: Name of the option

         Returns: Float option value that is returned

         """
        return self._config.getfloat(CONFIGURATION_FILE_SECTION, option,
                                     vars={**_get_feast_env_vars(),
                                           **self._options})
