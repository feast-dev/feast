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
from feast.feature_set import FeatureSet
import toml
import pkg_resources
from feast import resource
import yaml
import json

_logger = logging.getLogger(__name__)

_common_options = [
    click.option("--core-url", help="Set Feast core URL to connect to"),
    click.option("--serving-url", help="Set Feast serving URL to connect to"),
]


def common_options(func):
    for option in reversed(_common_options):
        func = option(func)
    return func


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--client-only", "-c", is_flag=True, help="Print only the version of the CLI"
)
@common_options
def version(client_only: bool, **kwargs):
    """
    Displays version and connectivity information
    """

    try:
        feast_versions_dict = {
            "sdk": {"version": str(pkg_resources.get_distribution("feast"))}
        }

        if not client_only:
            feast_client = Client(
                core_url=feast_config.get_config_property_or_fail(
                    "core_url", cli_config=kwargs
                ),
                serving_url=feast_config.get_config_property_or_fail(
                    "serving_url", cli_config=kwargs
                ),
            )
            feast_versions_dict.update(feast_client.version())

        print(json.dumps(feast_versions_dict))
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


@cli.group(name="feature-sets")
def feature_set():
    """
    Create and manage feature sets
    """
    pass


@feature_set.command()
def list():
    """
    List all feature sets
    """
    feast_client = Client(
        core_url=feast_config.get_config_property_or_fail("core_url"),
        serving_url=feast_config.get_config_property_or_fail("serving_url"),
    )  # type: Client

    for fs in feast_client.feature_sets:
        print(f"{fs.name}:{fs.version}")


@feature_set.command()
@click.argument("name")
def create(name):
    """
    Create a feature set
    """
    feast_client = Client(
        core_url=feast_config.get_config_property_or_fail("core_url"),
        serving_url=feast_config.get_config_property_or_fail("serving_url"),
    )  # type: Client

    feast_client.apply(FeatureSet(name=name))


@feature_set.command()
@click.argument("name")
@click.argument("version")
def describe(name, version):
    """
    Describe a feature set
    """
    feast_client = Client(
        core_url=feast_config.get_config_property_or_fail("core_url"),
        serving_url=feast_config.get_config_property_or_fail("serving_url"),
    )  # type: Client

    fs = feast_client.get_feature_set(name=name, version=version)
    if not fs:
        print(
            f'Feature set with name "{name}" and version "{version}" could not be found'
        )
        return

    print(yaml.dump(yaml.safe_load(str(fs)), default_flow_style=False, sort_keys=False))


@cli.command()
@click.option(
    "--filename",
    "-f",
    help="Path to the configuration file that will be applied",
    type=click.File("r"),
)
def apply(filename):
    """
    Apply a configuration to a resource by filename or stdin
    """
    resources = []
    # resources can be divided by a separator of '---'
    for resource_yaml in filename.read().split("---"):
        resources.append(resource.from_yaml(resource_yaml))

    feast_client = Client(
        core_url=feast_config.get_config_property_or_fail("core_url"),
        serving_url=feast_config.get_config_property_or_fail("serving_url"),
    )  # type: Client

    feast_client.apply(resources)


if __name__ == "__main__":
    cli()
