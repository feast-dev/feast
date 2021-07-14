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
import logging
import os
from configparser import ConfigParser, NoOptionError
from os.path import expanduser, join
from typing import Dict, Optional

from feast.constants import (
    CONFIG_FEAST_ENV_VAR_PREFIX,
    CONFIG_FILE_DEFAULT_DIRECTORY,
    CONFIG_FILE_NAME,
    CONFIG_FILE_SECTION,
    FEAST_CONFIG_FILE_ENV,
)
from feast.constants import ConfigOptions as opt

_logger = logging.getLogger(__name__)
_UNSET = object()


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

    os.makedirs(os.path.dirname(config_dir), exist_ok=True)

    # Create the configuration file itself
    config = ConfigParser(defaults=opt().defaults(), allow_no_value=True)
    if os.path.exists(path):
        config.read(path)

    # Store all configuration in a single section
    if not config.has_section(CONFIG_FILE_SECTION):
        config.add_section(CONFIG_FILE_SECTION)

    return config


def _get_feast_env_vars():
    """
    Get environmental variables that start with "FEAST_"

    Returns: Dict of Feast environmental variables (stripped of prefix)
    """
    feast_env_vars = {}
    for key in os.environ.keys():
        if key.upper().startswith(CONFIG_FEAST_ENV_VAR_PREFIX):
            feast_env_vars[key[len(CONFIG_FEAST_ENV_VAR_PREFIX) :]] = os.environ[key]
    return feast_env_vars


class Config:
    """
    Maintains and provides access to Feast configuration

    Configuration is stored as key/value pairs. The user can specify options
    through either input arguments to this class, environmental variables, or
    by setting the config in a configuration file

    """

    def __init__(
        self, options: Optional[Dict[str, str]] = None, path: Optional[str] = None,
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
            path = join(
                expanduser("~"),
                os.environ.get(FEAST_CONFIG_FILE_ENV, CONFIG_FILE_DEFAULT_DIRECTORY,),
                CONFIG_FILE_NAME,
            )

        config = _init_config(path)

        self._options = {}
        if options and isinstance(options, dict):
            self._options = options

        self._config = config
        self._path = path

    def _get(self, option, default, get_method):
        fallback = {} if default is _UNSET else {"fallback": default}
        return get_method(
            CONFIG_FILE_SECTION,
            option,
            vars={**_get_feast_env_vars(), **self._options},
            **fallback,
        )

    def get(self, option, default=_UNSET):
        """
        Returns a single configuration option as a string

        Args:
            option: Name of the option
            default: Default value to return if option is not found

        Returns: String option that is returned

        """
        return self._get(option, default, self._config.get)

    def getboolean(self, option, default=_UNSET):
        """
         Returns a single configuration option as a boolean

         Args:
             option: Name of the option
             default: Default value to return if option is not found

         Returns: Boolean option value that is returned

         """
        return self._get(option, default, self._config.getboolean)

    def getint(self, option, default=_UNSET):
        """
         Returns a single configuration option as an integer

         Args:
             option: Name of the option
             default: Default value to return if option is not found

         Returns: Integer option value that is returned

         """
        return self._get(option, default, self._config.getint)

    def getfloat(self, option, default=_UNSET):
        """
         Returns a single configuration option as an integer

         Args:
             option: Name of the option
             default: Default value to return if option is not found

         Returns: Float option value that is returned

         """
        return self._get(option, default, self._config.getfloat)

    def set(self, option, value):
        """
        Sets a configuration option. Must be serializable to string
        Args:
            option: Option name to use as key
            value: Value to store under option
        """
        self._config.set(CONFIG_FILE_SECTION, option, value=str(value))

    def exists(self, option):
        """
        Tests whether a specific option is available

        Args:
            option: Name of the option to check

        Returns: Boolean true/false whether the option is set

        """
        try:
            self.get(option=option)
            return True
        except NoOptionError:
            return False

    def save(self):
        """
        Save the current configuration to disk. This does not include
        environmental variables or initialized options
        """
        defaults = self._config.defaults()
        try:
            self._config._defaults = {}
            self._config.write(open(self._path, "w"))
        finally:
            self._config._defaults = defaults

    def __str__(self):
        result = ""
        for section_name in self._config.sections():
            result += "\n[" + section_name + "]\n"
            for name, value in self._config.items(section_name):
                result += name + " = " + value + "\n"
        return result
