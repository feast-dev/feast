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
import base64
import json
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import click
import pkg_resources
import yaml
from colorama import Fore, Style
from dateutil import parser
from pygments import formatters, highlight, lexers

from feast import utils
from feast.constants import (
    DEFAULT_FEATURE_TRANSFORMATION_SERVER_PORT,
    FEATURE_STORE_YAML_ENV_NAME,
)
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
from feast.repo_upgrade import RepoUpgrader
from feast.utils import maybe_local_tz

_logger = logging.getLogger(__name__)


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
@click.option(
    "--feature-store-yaml",
    help="Override the directory where the CLI should look for the feature_store.yaml file.",
)
@click.pass_context
def cli(
    ctx: click.Context,
    chdir: Optional[str],
    log_level: str,
    feature_store_yaml: Optional[str],
):
    """
    Feast CLI

    For more information, see our public docs at https://docs.feast.dev/

    For any questions, you can reach us at https://slack.feast.dev/
    """
    ctx.ensure_object(dict)
    ctx.obj["CHDIR"] = Path.cwd() if chdir is None else Path(chdir).absolute()
    ctx.obj["FS_YAML_FILE"] = (
        Path(feature_store_yaml).absolute()
        if feature_store_yaml
        else utils.get_default_yaml_file_path(ctx.obj["CHDIR"])
    )
    try:
        level = getattr(logging, log_level.upper())
        logging.basicConfig(
            format="%(asctime)s %(name)s %(levelname)s: %(message)s",
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
@click.option(
    "--host",
    "-h",
    type=click.STRING,
    default="0.0.0.0",
    show_default=True,
    help="Specify a host for the server",
)
@click.option(
    "--port",
    "-p",
    type=click.INT,
    default=8888,
    show_default=True,
    help="Specify a port for the server",
)
@click.option(
    "--registry_ttl_sec",
    "-r",
    help="Number of seconds after which the registry is refreshed",
    type=click.INT,
    default=5,
    show_default=True,
)
@click.option(
    "--root_path",
    help="Provide root path to make the UI working behind proxy",
    type=click.STRING,
    default="",
)
@click.pass_context
def ui(
    ctx: click.Context,
    host: str,
    port: int,
    registry_ttl_sec: int,
    root_path: Optional[str] = "",
):
    """
    Shows the Feast UI over the current directory
    """
    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)
    # Pass in the registry_dump method to get around a circular dependency
    store.serve_ui(
        host=host,
        port=port,
        get_registry_dump=registry_dump,
        registry_ttl_sec=registry_ttl_sec,
        root_path=root_path,
    )


@cli.command()
@click.pass_context
def endpoint(ctx: click.Context):
    """
    Display feature server endpoints
    """
    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)
    endpoint = store.get_feature_server_endpoint()
    if endpoint is not None:
        _logger.info(
            f"Feature server endpoint: {Style.BRIGHT + Fore.GREEN}{endpoint}{Style.RESET_ALL}"
        )
    else:
        _logger.info("There is no active feature server.")


@cli.group(name="data-sources")
def data_sources_cmd():
    """
    Access data sources
    """
    pass


@data_sources_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def data_source_describe(ctx: click.Context, name: str):
    """
    Describe a data source
    """
    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)

    try:
        data_source = store.get_data_source(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(data_source)), default_flow_style=False, sort_keys=False
        )
    )


@data_sources_cmd.command(name="list")
@click.pass_context
def data_source_list(ctx: click.Context):
    """
    List all data sources
    """
    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)
    table = []
    for datasource in store.list_data_sources():
        table.append([datasource.name, datasource.__class__])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "CLASS"], tablefmt="plain"))


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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)

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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)
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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)

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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)
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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)

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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]

    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)
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
            for backing_fv in feature_view.source_feature_view_projections.values():
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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)

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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)
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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    repo_config = load_repo_config(repo, fs_yaml_file)
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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)

    repo_config = load_repo_config(repo, fs_yaml_file)
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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    repo_config = load_repo_config(repo, fs_yaml_file)

    teardown(repo_config, repo)


@cli.command("registry-dump")
@click.pass_context
def registry_dump_command(ctx: click.Context):
    """
    Print contents of the metadata registry
    """
    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    repo_config = load_repo_config(repo, fs_yaml_file)

    click.echo(registry_dump(repo_config, repo_path=repo))


@cli.command("materialize")
@click.argument("start_ts")
@click.argument("end_ts")
@click.option(
    "--views",
    "-v",
    help="Feature views to materialize",
    multiple=True,
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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)
    store.materialize(
        feature_views=None if not views else views,
        start_date=utils.make_tzaware(parser.parse(start_ts)),
        end_date=utils.make_tzaware(parser.parse(end_ts)),
    )


@cli.command("materialize-incremental")
@click.argument("end_ts")
@click.option(
    "--views",
    "-v",
    help="Feature views to incrementally materialize",
    multiple=True,
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
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)
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
    type=click.Choice(
        [
            "local",
            "gcp",
            "aws",
            "snowflake",
            "spark",
            "postgres",
            "hbase",
            "cassandra",
            "rockset",
        ],
        case_sensitive=False,
    ),
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
    show_default=True,
    help="Specify a host for the server",
)
@click.option(
    "--port",
    "-p",
    type=click.INT,
    default=6566,
    show_default=True,
    help="Specify a port for the server",
)
@click.option(
    "--type",
    "-t",
    "type_",
    type=click.STRING,
    default="http",
    show_default=True,
    help="Specify a server type: 'http' or 'grpc'",
)
@click.option(
    "--no-access-log",
    is_flag=True,
    show_default=True,
    help="Disable the Uvicorn access log",
)
@click.option(
    "--no-feature-log",
    is_flag=True,
    show_default=True,
    help="Disable logging served features",
)
@click.pass_context
def serve_command(
    ctx: click.Context,
    host: str,
    port: int,
    type_: str,
    no_access_log: bool,
    no_feature_log: bool,
):
    """Start a feature server locally on a given port."""
    repo = ctx.obj["CHDIR"]

    # If we received a base64 encoded version of feature_store.yaml, use that
    config_base64 = os.getenv(FEATURE_STORE_YAML_ENV_NAME)
    if config_base64:
        print("Received base64 encoded feature_store.yaml")
        config_bytes = base64.b64decode(config_base64)
        # Create a new unique directory for writing feature_store.yaml
        repo_path = Path(tempfile.mkdtemp())
        with open(repo_path / "feature_store.yaml", "wb") as f:
            f.write(config_bytes)
        store = FeatureStore(repo_path=str(repo_path.resolve()))
    else:
        fs_yaml_file = ctx.obj["FS_YAML_FILE"]
        cli_check_repo(repo, fs_yaml_file)
        store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)

    store.serve(host, port, type_, no_access_log, no_feature_log)


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
    """[Experimental] Start a feature consumption server locally on a given port."""
    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)

    store.serve_transformations(port)


@cli.command("validate")
@click.option(
    "--feature-service",
    "-f",
    help="Specify a feature service name",
)
@click.option(
    "--reference",
    "-r",
    help="Specify a validation reference name",
)
@click.option(
    "--no-profile-cache",
    is_flag=True,
    help="Do not store cached profile in registry",
)
@click.argument("start_ts")
@click.argument("end_ts")
@click.pass_context
def validate(
    ctx: click.Context,
    feature_service: str,
    reference: str,
    start_ts: str,
    end_ts: str,
    no_profile_cache,
):
    """
    Perform validation of logged features (produced by a given feature service) against provided reference.

    START_TS and END_TS should be in ISO 8601 format, e.g. '2021-07-16T19:20:01'
    """
    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = FeatureStore(repo_path=str(repo), fs_yaml_file=fs_yaml_file)

    feature_service = store.get_feature_service(name=feature_service)
    reference = store.get_validation_reference(reference)

    result = store.validate_logged_features(
        source=feature_service,
        reference=reference,
        start=maybe_local_tz(datetime.fromisoformat(start_ts)),
        end=maybe_local_tz(datetime.fromisoformat(end_ts)),
        throw_exception=False,
        cache_profile=not no_profile_cache,
    )

    if not result:
        print(f"{Style.BRIGHT + Fore.GREEN}Validation successful!{Style.RESET_ALL}")
        return

    errors = [e.to_dict() for e in result.report.errors]
    formatted_json = json.dumps(errors, indent=4)
    colorful_json = highlight(
        formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter()
    )
    print(f"{Style.BRIGHT + Fore.RED}Validation failed!{Style.RESET_ALL}")
    print(colorful_json)
    exit(1)


@cli.command("repo-upgrade", cls=NoOptionDefaultFormat)
@click.option(
    "--write",
    is_flag=True,
    default=False,
    help="Upgrade a feature repo to use the API expected by feast 0.23.",
)
@click.pass_context
def repo_upgrade(ctx: click.Context, write: bool):
    """
    Upgrade a feature repo in place.
    """
    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    try:
        RepoUpgrader(repo, write).upgrade()
    except FeastProviderLoginError as e:
        print(str(e))


if __name__ == "__main__":
    cli()
