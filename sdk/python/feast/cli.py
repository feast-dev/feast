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
from typing import List

import click
import pkg_resources
import yaml

from feast.errors import FeastObjectNotFoundException, FeastProviderLoginError
from feast.feature_store import FeatureStore
from feast.repo_config import load_repo_config
from feast.repo_operations import (
    apply_total,
    cli_check_repo,
    generate_project_name,
    init_repo,
    registry_dump,
    teardown,
)
from feast.telemetry import Telemetry

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
def entities_cmd():
    """
    Access entities
    """
    pass


@entities_cmd.command("describe")
@click.argument("name", type=click.STRING)
def entity_describe(name: str):
    """
    Describe an entity
    """
    cli_check_repo(Path.cwd())
    store = FeatureStore(repo_path=str(Path.cwd()))

    try:
        entity = store.get_entity(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(entity)), default_flow_style=False, sort_keys=False
        )
    )


@entities_cmd.command(name="list")
def entity_list():
    """
    List all entities
    """
    cli_check_repo(Path.cwd())
    store = FeatureStore(repo_path=str(Path.cwd()))
    table = []
    for entity in store.list_entities():
        table.append([entity.name, entity.description, entity.value_type])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "DESCRIPTION", "TYPE"], tablefmt="plain"))


@cli.group(name="feature-views")
def feature_views_cmd():
    """
    Access feature views
    """
    pass


@feature_views_cmd.command("describe")
@click.argument("name", type=click.STRING)
def feature_view_describe(name: str):
    """
    Describe a feature view
    """
    cli_check_repo(Path.cwd())
    store = FeatureStore(repo_path=str(Path.cwd()))

    try:
        feature_view = store.get_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(feature_view)), default_flow_style=False, sort_keys=False
        )
    )


@feature_views_cmd.command(name="list")
def feature_view_list():
    """
    List all feature views
    """
    cli_check_repo(Path.cwd())
    store = FeatureStore(repo_path=str(Path.cwd()))
    table = []
    for feature_view in store.list_feature_views():
        table.append([feature_view.name, feature_view.entities])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "ENTITIES"], tablefmt="plain"))


@cli.command("apply")
def apply_total_command():
    """
    Create or update a feature store deployment
    """
    cli_check_repo(Path.cwd())
    repo_config = load_repo_config(Path.cwd())
    tele = Telemetry()
    tele.log("apply")
    try:
        apply_total(repo_config, Path.cwd())
    except FeastProviderLoginError as e:
        print(str(e))


@cli.command("teardown")
def teardown_command():
    """
    Tear down deployed feature store infrastructure
    """
    cli_check_repo(Path.cwd())
    repo_config = load_repo_config(Path.cwd())
    tele = Telemetry()
    tele.log("teardown")

    teardown(repo_config, Path.cwd())


@cli.command("registry-dump")
def registry_dump_command():
    """
    Print contents of the metadata registry
    """
    cli_check_repo(Path.cwd())
    repo_config = load_repo_config(Path.cwd())
    tele = Telemetry()
    tele.log("registry-dump")

    registry_dump(repo_config, repo_path=Path.cwd())


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

    if minimal:
        template = "minimal"

    tele = Telemetry()
    tele.log("init")
    init_repo(project_directory, template)


if __name__ == "__main__":
    cli()
