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
import json
import logging
from datetime import datetime
from importlib.metadata import version as importlib_version
from pathlib import Path
from typing import List, Optional

import click
import yaml
from colorama import Fore, Style
from dateutil import parser
from pygments import formatters, highlight, lexers

from feast import utils
from feast.cli.data_sources import data_sources_cmd
from feast.cli.entities import entities_cmd
from feast.cli.feature_services import feature_services_cmd
from feast.cli.feature_views import feature_views_cmd
from feast.cli.features import (
    features_cmd,
    get_historical_features,
    get_online_features,
)
from feast.cli.on_demand_feature_views import on_demand_feature_views_cmd
from feast.cli.permissions import feast_permissions_cmd
from feast.cli.projects import projects_cmd
from feast.cli.saved_datasets import saved_datasets_cmd
from feast.cli.serve import (
    serve_command,
    serve_offline_command,
    serve_registry_command,
    serve_transformations_command,
)
from feast.cli.stream_feature_views import stream_feature_views_cmd
from feast.cli.ui import ui
from feast.cli.validation_references import validation_references_cmd
from feast.errors import FeastProviderLoginError
from feast.repo_config import load_repo_config
from feast.repo_operations import (
    apply_total,
    cli_check_repo,
    create_feature_store,
    generate_project_name,
    init_repo,
    plan,
    registry_dump,
    teardown,
)
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
    default="warning",
    help="The logging level. One of DEBUG, INFO, WARNING, ERROR, and CRITICAL (case-insensitive).",
)
@click.option(
    "--feature-store-yaml",
    "-f",
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
    print(f'Feast SDK Version: "{importlib_version("feast")}"')


@cli.command()
@click.pass_context
def configuration(ctx: click.Context):
    """
    Display Feast configuration
    """
    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    repo_config = load_repo_config(repo, fs_yaml_file)
    if repo_config:
        config_dict = repo_config.model_dump(by_alias=True, exclude_unset=True)
        config_dict.pop("repo_path", None)
        print(yaml.dump(config_dict, default_flow_style=False, sort_keys=False))
    else:
        print("No configuration found.")


@cli.command()
@click.pass_context
def endpoint(ctx: click.Context):
    """
    Display feature server endpoints
    """
    store = create_feature_store(ctx)
    endpoint = store.get_feature_server_endpoint()
    if endpoint is not None:
        _logger.info(
            f"Feature server endpoint: {Style.BRIGHT + Fore.GREEN}{endpoint}{Style.RESET_ALL}"
        )
    else:
        _logger.info("There is no active feature server.")


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
    store = create_feature_store(ctx)

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
    store = create_feature_store(ctx)
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
            "hazelcast",
            "ikv",
            "couchbase",
            "milvus",
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


@cli.command("listen")
@click.option(
    "--address",
    "-a",
    type=click.STRING,
    default="localhost:50051",
    show_default=True,
    help="Address of the gRPC server",
)
@click.option(
    "--max_workers",
    "-w",
    type=click.INT,
    default=10,
    show_default=False,
    help="The maximum number of threads that can be used to execute the gRPC calls",
)
@click.option(
    "--registry_ttl_sec",
    "-r",
    help="Number of seconds after which the registry is refreshed",
    type=click.INT,
    default=5,
    show_default=True,
)
@click.pass_context
def listen_command(
    ctx: click.Context,
    address: str,
    max_workers: int,
    registry_ttl_sec: int,
):
    """Start a gRPC feature server to ingest streaming features on given address"""
    from feast.infra.contrib.grpc_server import get_grpc_server

    store = create_feature_store(ctx)
    server = get_grpc_server(address, store, max_workers, registry_ttl_sec)
    server.start()
    server.wait_for_termination()


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
    store = create_feature_store(ctx)

    _feature_service = store.get_feature_service(name=feature_service)
    _reference = store.get_validation_reference(reference)

    result = store.validate_logged_features(
        source=_feature_service,
        reference=_reference,
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


cli.add_command(data_sources_cmd)
cli.add_command(entities_cmd)
cli.add_command(feature_services_cmd)
cli.add_command(feature_views_cmd)
cli.add_command(features_cmd)
cli.add_command(get_historical_features)
cli.add_command(get_online_features)
cli.add_command(on_demand_feature_views_cmd)
cli.add_command(feast_permissions_cmd)
cli.add_command(projects_cmd)
cli.add_command(saved_datasets_cmd)
cli.add_command(stream_feature_views_cmd)
cli.add_command(validation_references_cmd)
cli.add_command(ui)
cli.add_command(serve_command)
cli.add_command(serve_offline_command)
cli.add_command(serve_registry_command)
cli.add_command(serve_transformations_command)

if __name__ == "__main__":
    cli()
