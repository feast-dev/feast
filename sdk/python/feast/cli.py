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

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import click
import pkg_resources
import yaml

from feast.client import Client
from feast.entity import Entity
from feast.feature_store import FeatureStore
from feast.loaders.yaml import yaml_loader
from feast.repo_config import load_repo_config
from feast.repo_operations import (
    apply_total,
    cli_check_repo,
    generate_project_name,
    init_repo,
    registry_dump,
    teardown,
)

_logger = logging.getLogger(__name__)
DATETIME_ISO = "%Y-%m-%dT%H:%M:%s"


@click.group()
def cli():
    """
    Feast CLI

    For more information, see our public docs at https://docs.feast.dev/

    For any questions, you can reach us at https://slack.feast.dev/
    """
    pass


@cli.command()
def version():
    """
    Display Feast SDK version
    """
    print(f'Feast SDK Version: "{pkg_resources.get_distribution("feast")}"')


@cli.group(name="entities")
def entity():
    """
    Create and manage entities
    """
    pass


@entity.command("apply")
@click.option(
    "--filename",
    "-f",
    help="Path to an entity configuration file that will be applied",
    type=click.Path(exists=True),
)
@click.option(
    "--project",
    "-p",
    help="Project that entity belongs to",
    type=click.STRING,
    default="default",
)
def entity_create(filename, project):
    """
    Create or update an entity
    """

    entities = [Entity.from_dict(entity_dict) for entity_dict in yaml_loader(filename)]
    feast_client = Client()  # type: Client
    feast_client.apply(entities, project)


@entity.command("describe")
@click.argument("name", type=click.STRING)
@click.option(
    "--project",
    "-p",
    help="Project that entity belongs to",
    type=click.STRING,
    default="default",
)
def entity_describe(name: str, project: str):
    """
    Describe an entity
    """
    feast_client = Client()  # type: Client
    entity = feast_client.get_entity(name=name, project=project)

    if not entity:
        print(f'Entity with name "{name}" could not be found')
        return

    print(
        yaml.dump(
            yaml.safe_load(str(entity)), default_flow_style=False, sort_keys=False
        )
    )


@entity.command(name="list")
@click.option(
    "--project",
    "-p",
    help="Project that entity belongs to",
    type=click.STRING,
    default="",
)
@click.option(
    "--labels",
    "-l",
    help="Labels to filter for entities",
    type=click.STRING,
    default="",
)
def entity_list(project: str, labels: str):
    """
    List all entities
    """
    feast_client = Client()  # type: Client

    labels_dict = _get_labels_dict(labels)

    table = []
    for entity in feast_client.list_entities(project=project, labels=labels_dict):
        table.append([entity.name, entity.description, entity.value_type])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "DESCRIPTION", "TYPE"], tablefmt="plain"))


def _get_labels_dict(label_str: str) -> Dict[str, str]:
    """
    Converts CLI input labels string to dictionary format if provided string is valid.

    Args:
        label_str: A comma-separated string of key-value pairs

    Returns:
        Dict of key-value label pairs
    """
    labels_dict: Dict[str, str] = {}
    labels_kv = label_str.split(",")
    if label_str == "":
        return labels_dict
    if len(labels_kv) % 2 == 1:
        raise ValueError("Uneven key-value label pairs were entered")
    for k, v in zip(labels_kv[0::2], labels_kv[1::2]):
        labels_dict[k] = v
    return labels_dict


@cli.command("apply")
def apply_total_command():
    """
    Create or update a feature store deployment
    """
    cli_check_repo(Path.cwd())
    repo_config = load_repo_config(Path.cwd())

    apply_total(repo_config, Path.cwd())


@cli.command("teardown")
def teardown_command():
    """
    Tear down deployed feature store infrastructure
    """
    cli_check_repo(Path.cwd())
    repo_config = load_repo_config(Path.cwd())

    teardown(repo_config, Path.cwd())


@cli.command("registry-dump")
def registry_dump_command():
    """
    Print contents of the metadata registry
    """
    cli_check_repo(Path.cwd())
    repo_config = load_repo_config(Path.cwd())

    registry_dump(repo_config)


@cli.command("materialize")
@click.argument("start_ts")
@click.argument("end_ts")
@click.option(
    "--views", "-v", help="Feature views to materialize", multiple=True,
)
def materialize_command(start_ts: str, end_ts: str, views: List[str]):
    """
    Run a (non-incremental) materialization job to ingest data into the online store. Feast
    will read all data between START_TS and END_TS from the offline store and write it to the
    online store. If you don't specify feature view names using --views, all registered Feature
    Views will be materialized.

    START_TS and END_TS should be in ISO 8601 format, e.g. '2021-07-16T19:20:01'
    """
    cli_check_repo(Path.cwd())
    store = FeatureStore(repo_path=str(Path.cwd()))
    store.materialize(
        feature_views=None if not views else views,
        start_date=datetime.fromisoformat(start_ts),
        end_date=datetime.fromisoformat(end_ts),
    )


@cli.command("materialize-incremental")
@click.argument("end_ts")
@click.option(
    "--views", "-v", help="Feature views to incrementally materialize", multiple=True,
)
def materialize_incremental_command(end_ts: str, views: List[str]):
    """
    Run an incremental materialization job to ingest new data into the online store. Feast will read
    all data from the previously ingested point to END_TS from the offline store and write it to the
    online store. If you don't specify feature view names using --views, all registered Feature
    Views will be incrementally materialized.

    END_TS should be in ISO 8601 format, e.g. '2021-07-16T19:20:01'
    """
    cli_check_repo(Path.cwd())
    store = FeatureStore(repo_path=str(Path.cwd()))
    store.materialize_incremental(
        feature_views=None if not views else views,
        end_date=datetime.fromisoformat(end_ts),
    )


@cli.command("init")
@click.argument("PROJECT_DIRECTORY", required=False)
@click.option(
    "--minimal", "-m", is_flag=True, help="Create an empty project repository"
)
@click.option(
    "--template",
    "-t",
    type=click.Choice(["local", "gcp"], case_sensitive=False),
    help="Specify a template for the created project",
    default="local",
)
def init_command(project_directory, minimal: bool, template: str):
    """Create a new Feast repository"""
    if not project_directory:
        project_directory = generate_project_name()
    if template and minimal:
        from colorama import Fore, Style

        click.echo(
            f"Please select either a {Style.BRIGHT + Fore.GREEN}template{Style.RESET_ALL} or "
            f"{Style.BRIGHT + Fore.GREEN}minimal{Style.RESET_ALL}, not both"
        )
        exit(1)

    if minimal:
        template = "minimal"

    init_repo(project_directory, template)


if __name__ == "__main__":
    cli()
