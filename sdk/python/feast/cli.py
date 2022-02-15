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
from typing import List, Optional

import click
import pkg_resources
import yaml
from colorama import Fore, Style

from feast import flags, flags_helper, utils
from feast.constants import DEFAULT_FEATURE_TRANSFORMATION_SERVER_PORT
from feast.errors import FeastObjectNotFoundException, FeastProviderLoginError
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import load_repo_config
from feast.repo_operations import (
    apply_total,
    cli_check_repo,
    generate_project_name,
    init_repo,
    plan,
    registry_dump,
    teardown,
)

_logger = logging.getLogger(__name__)
DATETIME_ISO = "%Y-%m-%dT%H:%M:%s"


class NoOptionDefaultFormat(click.Command):
    def format_options(self, ctx: click.Context, formatter: click.HelpFormatter):
        """Writes all the options into the formatter if they exist."""
        opts = []
        for param in self.get_params(ctx):
            rv = param.get_help_record(ctx)
            if rv is not None:
                opts.append(rv)
        if opts:
            with formatter.section("Options(No current command options)"):
                formatter.write_dl(opts)


@click.group()
@click.option(
    "--chdir",
    "-c",
    help="Switch to a different feature repository directory before executing the given subcommand.",
)
@click.option(
    "--log-level",
    default="info",
    help="The logging level. One of DEBUG, INFO, WARNING, ERROR, and CRITICAL (case-insensitive).",
)
@click.pass_context
def cli(ctx: click.Context, chdir: Optional[str], log_level: str):
    """
    Feast CLI

    For more information, see our public docs at https://docs.feast.dev/

    For any questions, you can reach us at https://slack.feast.dev/
    """
    ctx.ensure_object(dict)
    ctx.obj["CHDIR"] = Path.cwd() if chdir is None else Path(chdir).absolute()
    try:
        level = getattr(logging, log_level.upper())
        logging.basicConfig(
            format="%(asctime)s %(levelname)s:%(message)s",
            datefmt="%m/%d/%Y %I:%M:%S %p",
            level=level,
        )
        # Override the logging level for already created loggers (due to loggers being created at the import time)
        # Note, that format & datefmt does not need to be set, because by default child loggers don't override them

        # Also note, that mypy complains that logging.root doesn't have "manager" because of the way it's written.
        # So we have to put a type ignore hint for mypy.
        for logger_name in logging.root.manager.loggerDict:  # type: ignore
            if "feast" in logger_name:
                logger = logging.getLogger(logger_name)
                logger.setLevel(level)

    except Exception as e:
        raise e
    pass


@cli.command()
def version():
    """
    Display Feast SDK version
    """
    print(f'Feast SDK Version: "{pkg_resources.get_distribution("feast")}"')


@cli.command()
@click.pass_context
def endpoint(ctx: click.Context):
    """
    Display feature server endpoints.
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    endpoint = store.get_feature_server_endpoint()
    if endpoint is not None:
        _logger.info(
            f"Feature server endpoint: {Style.BRIGHT + Fore.GREEN}{endpoint}{Style.RESET_ALL}"
        )
    else:
        _logger.info("There is no active feature server.")


@cli.group(name="entities")
def entities_cmd():
    """
    Access entities
    """
    pass


@entities_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def entity_describe(ctx: click.Context, name: str):
    """
    Describe an entity
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))

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
@click.pass_context
def entity_list(ctx: click.Context):
    """
    List all entities
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    table = []
    for entity in store.list_entities():
        table.append([entity.name, entity.description, entity.value_type])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "DESCRIPTION", "TYPE"], tablefmt="plain"))


@cli.group(name="feature-services")
def feature_services_cmd():
    """
    Access feature services
    """
    pass


@feature_services_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def feature_service_describe(ctx: click.Context, name: str):
    """
    Describe a feature service
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))

    try:
        feature_service = store.get_feature_service(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(feature_service)),
            default_flow_style=False,
            sort_keys=False,
        )
    )


@feature_services_cmd.command(name="list")
@click.pass_context
def feature_service_list(ctx: click.Context):
    """
    List all feature services
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    feature_services = []
    for feature_service in store.list_feature_services():
        feature_names = []
        for projection in feature_service.feature_view_projections:
            feature_names.extend(
                [f"{projection.name}:{feature.name}" for feature in projection.features]
            )
        feature_services.append([feature_service.name, ", ".join(feature_names)])

    from tabulate import tabulate

    print(tabulate(feature_services, headers=["NAME", "FEATURES"], tablefmt="plain"))


@cli.group(name="feature-views")
def feature_views_cmd():
    """
    Access feature views
    """
    pass


@feature_views_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def feature_view_describe(ctx: click.Context, name: str):
    """
    Describe a feature view
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))

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
@click.pass_context
def feature_view_list(ctx: click.Context):
    """
    List all feature views
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    table = []
    for feature_view in [
        *store.list_feature_views(),
        *store.list_request_feature_views(),
        *store.list_on_demand_feature_views(),
    ]:
        entities = set()
        if isinstance(feature_view, FeatureView):
            entities.update(feature_view.entities)
        elif isinstance(feature_view, OnDemandFeatureView):
            for backing_fv in feature_view.input_feature_view_projections.values():
                entities.update(store.get_feature_view(backing_fv.name).entities)
        table.append(
            [
                feature_view.name,
                entities if len(entities) > 0 else "n/a",
                type(feature_view).__name__,
            ]
        )

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "ENTITIES", "TYPE"], tablefmt="plain"))


@cli.group(name="on-demand-feature-views")
def on_demand_feature_views_cmd():
    """
    [Experimental] Access on demand feature views
    """
    pass


@on_demand_feature_views_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def on_demand_feature_view_describe(ctx: click.Context, name: str):
    """
    [Experimental] Describe an on demand feature view
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))

    try:
        on_demand_feature_view = store.get_on_demand_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(on_demand_feature_view)),
            default_flow_style=False,
            sort_keys=False,
        )
    )


@on_demand_feature_views_cmd.command(name="list")
@click.pass_context
def on_demand_feature_view_list(ctx: click.Context):
    """
    [Experimental] List all on demand feature views
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    table = []
    for on_demand_feature_view in store.list_on_demand_feature_views():
        table.append([on_demand_feature_view.name])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME"], tablefmt="plain"))


@cli.command("plan", cls=NoOptionDefaultFormat)
@click.option(
    "--skip-source-validation",
    is_flag=True,
    help="Don't validate the data sources by checking for that the tables exist.",
)
@click.pass_context
def plan_command(ctx: click.Context, skip_source_validation: bool):
    """
    Create or update a feature store deployment
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_config = load_repo_config(repo)
    try:
        plan(repo_config, repo, skip_source_validation)
    except FeastProviderLoginError as e:
        print(str(e))


@cli.command("apply", cls=NoOptionDefaultFormat)
@click.option(
    "--skip-source-validation",
    is_flag=True,
    help="Don't validate the data sources by checking for that the tables exist.",
)
@click.pass_context
def apply_total_command(ctx: click.Context, skip_source_validation: bool):
    """
    Create or update a feature store deployment
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_config = load_repo_config(repo)
    try:
        apply_total(repo_config, repo, skip_source_validation)
    except FeastProviderLoginError as e:
        print(str(e))


@cli.command("teardown", cls=NoOptionDefaultFormat)
@click.pass_context
def teardown_command(ctx: click.Context):
    """
    Tear down deployed feature store infrastructure
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_config = load_repo_config(repo)

    teardown(repo_config, repo)


@cli.command("registry-dump")
@click.pass_context
def registry_dump_command(ctx: click.Context):
    """
    Print contents of the metadata registry
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_config = load_repo_config(repo)

    registry_dump(repo_config, repo_path=repo)


@cli.command("materialize")
@click.argument("start_ts")
@click.argument("end_ts")
@click.option(
    "--views", "-v", help="Feature views to materialize", multiple=True,
)
@click.pass_context
def materialize_command(
    ctx: click.Context, start_ts: str, end_ts: str, views: List[str]
):
    """
    Run a (non-incremental) materialization job to ingest data into the online store. Feast
    will read all data between START_TS and END_TS from the offline store and write it to the
    online store. If you don't specify feature view names using --views, all registered Feature
    Views will be materialized.

    START_TS and END_TS should be in ISO 8601 format, e.g. '2021-07-16T19:20:01'
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    store.materialize(
        feature_views=None if not views else views,
        start_date=utils.make_tzaware(datetime.fromisoformat(start_ts)),
        end_date=utils.make_tzaware(datetime.fromisoformat(end_ts)),
    )


@cli.command("materialize-incremental")
@click.argument("end_ts")
@click.option(
    "--views", "-v", help="Feature views to incrementally materialize", multiple=True,
)
@click.pass_context
def materialize_incremental_command(ctx: click.Context, end_ts: str, views: List[str]):
    """
    Run an incremental materialization job to ingest new data into the online store. Feast will read
    all data from the previously ingested point to END_TS from the offline store and write it to the
    online store. If you don't specify feature view names using --views, all registered Feature
    Views will be incrementally materialized.

    END_TS should be in ISO 8601 format, e.g. '2021-07-16T19:20:01'
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    store.materialize_incremental(
        feature_views=None if not views else views,
        end_date=utils.make_tzaware(datetime.fromisoformat(end_ts)),
    )


@cli.command("init")
@click.argument("PROJECT_DIRECTORY", required=False)
@click.option(
    "--minimal", "-m", is_flag=True, help="Create an empty project repository"
)
@click.option(
    "--template",
    "-t",
    type=click.Choice(["local", "gcp", "aws", "snowflake"], case_sensitive=False),
    help="Specify a template for the created project",
    default="local",
)
def init_command(project_directory, minimal: bool, template: str):
    """Create a new Feast repository"""
    if not project_directory:
        project_directory = generate_project_name()

    if minimal:
        template = "minimal"

    init_repo(project_directory, template)


@cli.command("serve")
@click.option(
    "--host",
    "-h",
    type=click.STRING,
    default="127.0.0.1",
    help="Specify a host for the server [default: 127.0.0.1]",
)
@click.option(
    "--port",
    "-p",
    type=click.INT,
    default=6566,
    help="Specify a port for the server [default: 6566]",
)
@click.option(
    "--no-access-log", is_flag=True, help="Disable the Uvicorn access log.",
)
@click.pass_context
def serve_command(ctx: click.Context, host: str, port: int, no_access_log: bool):
    """Start a feature server locally on a given port."""
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))

    store.serve(host, port, no_access_log)


@cli.command("serve_transformations")
@click.option(
    "--port",
    "-p",
    type=click.INT,
    default=DEFAULT_FEATURE_TRANSFORMATION_SERVER_PORT,
    help="Specify a port for the server",
)
@click.pass_context
def serve_transformations_command(ctx: click.Context, port: int):
    """[Experimental] Start a the feature consumption server locally on a given port."""
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))

    store.serve_transformations(port)


@cli.group(name="alpha")
def alpha_cmd():
    """
    Access alpha features
    """
    pass


@alpha_cmd.command("list")
@click.pass_context
def list_alpha_features(ctx: click.Context):
    """
    Lists all alpha features
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_path = str(repo)
    store = FeatureStore(repo_path=repo_path)

    flags_to_show = flags.FLAG_NAMES.copy()
    flags_to_show.remove(flags.FLAG_ALPHA_FEATURES_NAME)
    print("Alpha features:")
    for flag in flags_to_show:
        enabled_string = (
            "enabled"
            if flags_helper.feature_flag_enabled(store.config, flag)
            else "disabled"
        )
        print(f"{flag}: {enabled_string}")


@alpha_cmd.command("enable-all")
@click.pass_context
def enable_alpha_features(ctx: click.Context):
    """
    Enables all alpha features
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_path = str(repo)
    store = FeatureStore(repo_path=repo_path)

    if store.config.flags is None:
        store.config.flags = {}
    for flag_name in flags.FLAG_NAMES:
        store.config.flags[flag_name] = True
    store.config.write_to_path(Path(repo_path))


@alpha_cmd.command("enable")
@click.argument("name", type=click.STRING)
@click.pass_context
def enable_alpha_feature(ctx: click.Context, name: str):
    """
    Enables an alpha feature
    """
    if name not in flags.FLAG_NAMES:
        raise ValueError(f"Flag name, {name}, not valid.")

    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_path = str(repo)
    store = FeatureStore(repo_path=repo_path)

    if store.config.flags is None:
        store.config.flags = {}
    store.config.flags[flags.FLAG_ALPHA_FEATURES_NAME] = True
    store.config.flags[name] = True
    store.config.write_to_path(Path(repo_path))


@alpha_cmd.command("disable")
@click.argument("name", type=click.STRING)
@click.pass_context
def disable_alpha_feature(ctx: click.Context, name: str):
    """
    Disables an alpha feature
    """
    if name not in flags.FLAG_NAMES:
        raise ValueError(f"Flag name, {name}, not valid.")

    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_path = str(repo)
    store = FeatureStore(repo_path=repo_path)

    if store.config.flags is None or name not in store.config.flags:
        return
    store.config.flags[name] = False
    store.config.write_to_path(Path(repo_path))


@alpha_cmd.command("disable-all")
@click.pass_context
def disable_alpha_features(ctx: click.Context):
    """
    Disables all alpha features
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_path = str(repo)
    store = FeatureStore(repo_path=repo_path)

    store.config.flags = None
    store.config.write_to_path(Path(repo_path))


if __name__ == "__main__":
    cli()
