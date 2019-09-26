# Copyright 2019 The Feast Authors
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

import sys
import logging
import click
from feast import config as feast_config
from feast.client import Client
import toml
import pkg_resources

_logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command()
@click.option('--client-only', '-c', is_flag=True, help="Print only the version of the CLI")
def version(client_only: bool):
    """
    Displays version and connectivity information
    """

    try:
        feast_versions = {"sdk": {"version": pkg_resources.get_distribution("feast")}}

        if not client_only:
            feast_client = Client(
                core_url=feast_config.get_config_property_or_fail("core_url"),
                serving_url=feast_config.get_config_property_or_fail("serving_url"),
            )
            feast_versions.update(feast_client.version())

        print(feast_versions)
    except Exception as e:
        _logger.error("Error initializing backend store")
        _logger.exception(e)
        sys.exit(1)


@cli.group()
def config():
    """
    View and edit Feast properties
    """
    pass


@config.command(name="list")
def config_list():
    """
    List Feast properties for the currently active configuration
    """

    try:
        feast_config_string = toml.dumps(feast_config.get_or_create_config())
        if not feast_config_string.strip():
            print("Configuration has not been set")
        else:
            print(feast_config_string.replace('""', "").strip())
    except Exception as e:
        _logger.error("Error occurred when reading Feast configuration file")
        _logger.exception(e)
        sys.exit(1)


@config.command(name="set")
@click.argument("fproperty")
@click.argument("value")
def config_set(fproperty, value):
    """
    Set a Feast properties for the currently active configuration
    """
    try:
        feast_config.set_property(fproperty.strip(), value.strip())
    except Exception as e:
        _logger.error("Error in reading config file")
        _logger.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    cli()
