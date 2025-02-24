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
from feast.constants import (
    DEFAULT_FEATURE_TRANSFORMATION_SERVER_PORT,
    DEFAULT_REGISTRY_SERVER_PORT,
)
from feast.errors import FeastObjectNotFoundException, FeastProviderLoginError
from feast.feature_view import FeatureView
from feast.data_source import PushSource
from feast.on_demand_feature_view import OnDemandFeatureView
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

from feast.infra.registry.graph import GraphRegistry


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
    store = create_feature_store(ctx)
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
    store = create_feature_store(ctx)
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
    store = create_feature_store(ctx)

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
    store = create_feature_store(ctx)
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
    store = create_feature_store(ctx)

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
    store = create_feature_store(ctx)
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
    store = create_feature_store(ctx)

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
    store = create_feature_store(ctx)
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
    store = create_feature_store(ctx)

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
    store = create_feature_store(ctx)
    table = []
    for feature_view in [
        *store.list_feature_views(),
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
    store = create_feature_store(ctx)

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
    store = create_feature_store(ctx)
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
            "rockset",
            "hazelcast",
            "ikv",
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
@click.option(
    "--workers",
    "-w",
    type=click.INT,
    default=1,
    show_default=True,
    help="Number of worker",
)
@click.option(
    "--keep-alive-timeout",
    type=click.INT,
    default=5,
    show_default=True,
    help="Timeout for keep alive",
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
def serve_command(
    ctx: click.Context,
    host: str,
    port: int,
    type_: str,
    no_access_log: bool,
    no_feature_log: bool,
    workers: int,
    keep_alive_timeout: int,
    registry_ttl_sec: int = 5,
):
    """Start a feature server locally on a given port."""
    store = create_feature_store(ctx)

    store.serve(
        host=host,
        port=port,
        type_=type_,
        no_access_log=no_access_log,
        no_feature_log=no_feature_log,
        workers=workers,
        keep_alive_timeout=keep_alive_timeout,
        registry_ttl_sec=registry_ttl_sec,
    )


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
    store = create_feature_store(ctx)

    store.serve_transformations(port)


@cli.command("serve_registry")
@click.option(
    "--port",
    "-p",
    type=click.INT,
    default=DEFAULT_REGISTRY_SERVER_PORT,
    help="Specify a port for the server",
)
@click.pass_context
def serve_registry_command(ctx: click.Context, port: int):
    """Start a registry server locally on a given port."""
    store = create_feature_store(ctx)

    store.serve_registry(port)


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

@cli.group(name="graph")
def graph_cmd():
    """
    Query graph registry contents
    """
    pass

@graph_cmd.command("execute-query")
@click.option("--query", "-q", required=True, help="Cypher query to execute.")
@click.pass_context
def graph_execute_query(ctx, query):
    """
    Execute Cypher query
    """
    store = create_feature_store(ctx)
    registry = store._registry
    if not isinstance(registry, GraphRegistry):
        print("Graph commands are not supported for this type of registry.")
        exit(1)
    else:
        with registry.driver.session(database=registry.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(query)
                results = result.data()
        if results:
            from tabulate import tabulate

            headers = None
            rows = []
            for item in results:
                row = {}
                for top_key, sub_dict in item.items():
                    for key, value in sub_dict.items():
                        if 'proto' not in key:
                            if 'last_updated_timestamp' in key:
                                value = datetime.fromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S')
                            header_key = f"{top_key}:{key}"
                            row[header_key] = value

                if headers is None:
                    headers = sorted(row.keys(), key=lambda x: ('name' not in x, x))
                        
                rows.append([row.get(header, '') for header in headers])

            print(tabulate(rows, headers=headers, tablefmt='plain'))

        else:
            click.echo("No results found.")
        
@graph_cmd.command("most-used")
@click.option(
    "--object",
    "-o",
    type=click.Choice(["feature-views", "on-demand-feature-views", "data-sources", "entities", "fields"]),
    required=True,
    help="Count relationships and sort for the most used object of the specified type."
)
@click.option(
    "--limit",
    "-l",
    type=click.INT,
    help="Limit the number of objects shown."
)
@click.pass_context
def graph_most_used(ctx, object, limit: Optional[int]):
    """
    Count relationships and sort for the most used object of the specified type
    """
    store = create_feature_store(ctx)
    registry = store._registry
    if not isinstance(registry, GraphRegistry):
        if object == "data-sources":
            dict = {}
            for fv in [
                *store.list_feature_views(),
            ]:                
                if fv.stream_source:
                    # print(f"FV: {fv.name}. Batch source: {fv.batch_source.name}. Stream sourceL {fv.stream_source.name}")
                    name = fv.stream_source.name
                else:
                    # print(f"FV: {fv.name}. Batch source: {fv.batch_source.name}.")
                    name = fv.batch_source.name
                if name in dict.keys():
                    dict[name] += 1
                else:
                    dict[name] = 1
            for odfv in [
                *store.list_on_demand_feature_views(),
            ]:
                rs = odfv.source_request_sources.values()
                for s in rs:
                    name = s.name
                    if name in dict.keys():
                        dict[name] += 1
                    else:
                        dict[name] = 1
            
            list = [{'name':k, 'usage_count':v} for k,v in dict.items()]
            list.sort(key=lambda e: (-e['usage_count'],e['name']))
        elif object == "fields":
            dict = {}
            for fv in [
                *store.list_feature_views(),
            ]:
                if fv.stream_source:
                    # print(f"FV: {fv.name}. Batch source: {fv.batch_source.name}. Stream sourceL {fv.stream_source.name}")
                    source_name = fv.stream_source.name
                else:
                    # print(f"FV: {fv.name}. Batch source: {fv.batch_source.name}.")
                    source_name = fv.batch_source.name
                for field in fv.features:
                    field_id = f"{field.name}-{source_name}"
                    if field_id in dict.keys():
                        dict[field_id]["usage_count"] += 1
                    else:
                        dict[field_id] = {
                            "name": field.name,
                            "source": source_name,
                            "usage_count": 1
                        }
            list = [{'name':v["name"], 'source': v["source"], 'usage_count':v["usage_count"]} for v in dict.values()]
            list.sort(key=lambda e: (-e['usage_count'], e['name']))

        if limit:
            results = list[:limit]
        else:
            results = list

        # print(f"Results: {results}")
        from tabulate import tabulate

        print(f"Most used {object.replace('-', ' ')}:")
        print(tabulate(results, headers="keys", tablefmt="plain"))
        # print("Graph commands are not supported for this type of registry.")
        # exit(1)
    else:
        query = ""
        if object == "feature-views":
            query = """
                MATCH (fv:FeatureView)<-[c:CONSUMES]-(fs:FeatureService)
                RETURN fv.feature_view_name AS name, COUNT(fs) AS usage_count
                ORDER BY usage_count DESC
            """
        elif object == "on-demand-feature-views":
            query = """
                MATCH (odfv:OnDemandFeatureView)<-[c:CONSUMES]-(fs:FeatureService)
                RETURN odfv.feature_view_name AS name, COUNT(fs) AS usage_count
                ORDER BY usage_count DESC
            """
        elif object == "data-sources":
            query = """
                MATCH (ds:DataSource)
                OPTIONAL MATCH (ds)<-[p:POPULATED_FROM]-(fv:FeatureView)
                OPTIONAL MATCH (ds)<-[b:BASED_ON]-(odfv:OnDemandFeatureView)
                RETURN ds.data_source_name AS name,
                       (COUNT(fv) + COUNT(odfv)) AS usage_count
                ORDER BY usage_count DESC
            """
        elif object == "entities":
            query = """
                MATCH (e:Entity)<-[u:USES]-(fv:FeatureView)
                RETURN e.entity_name AS name, COUNT(fv) AS usage_count
                ORDER BY usage_count DESC
            """
        elif object == "fields":
            query = """
                MATCH (ds:DataSource)-[h2:HAS]->(f:Field)<-[h1:HAS]-(fv:FeatureView)
                RETURN f.field_name AS name, ds.data_source_name AS source_name, COUNT(fv) AS usage_count
                ORDER BY usage_count DESC
            """

        if query and limit:
            query += f"LIMIT {limit}"

        with registry.driver.session(database=registry.database) as session:
            with session.begin_transaction() as tx:                
                result = tx.run(query)
                results = result.data()
        if results:
            # print(f"Results: {results}")
            from tabulate import tabulate

            print(f"Most used {object.replace('-', ' ')}:")
            print(tabulate(results, headers="keys", tablefmt="plain"))

        else:
            click.echo(f"No {object.replace('-', ' ')} found.")

@graph_cmd.command("most-dependencies")
@click.option(
    "--object",
    "-o",
    type=click.Choice(["feature-views", "fields"]),
    required=True,
    help="Count relationships and sort for the most used object of the specified type as a dependency for transformations."
)
@click.option(
    "--limit",
    "-l",
    type=click.INT,
    help="Limit the number of objects shown."
)
@click.pass_context
def graph_most_dependencies(ctx, object, limit: Optional[int]):
    """
    Count relationships and sort for the most used object of the specified type as a dependency for transformations
    """
    store = create_feature_store(ctx)
    registry = store._registry
    if not isinstance(registry, GraphRegistry):
        dict = {}
        for odfv in [
            *store.list_on_demand_feature_views(),
        ]:
            fvs = odfv.source_feature_view_projections.values()
            for fv in fvs:
                name = fv.name
                if name in dict.keys():
                    dict[name] += 1
                else:
                    dict[name] = 1
        
        list = [{'name':k, 'dependency_count':v} for k,v in dict.items()]
        list.sort(key=lambda e: (-e['dependency_count'], e['name']))
        if limit:
            results = list[:limit]
        else:
            results = list

        # print(f"Results: {results}")
        from tabulate import tabulate

        print(f"Most used {object.replace('-', ' ')} as dependencies:")
        print(tabulate(results, headers="keys", tablefmt="plain"))
        #print("Graph commands are not supported for this type of registry.")
        #exit(1)
    else:
        query = ""
        if object == "feature-views":
            query = """
                MATCH (fv:FeatureView)<-[b:BASED_ON]-(odfv:OnDemandFeatureView)
                RETURN fv.feature_view_name AS name, COUNT(odfv) AS dependency_count
                ORDER BY dependency_count DESC
            """
        elif object == "fields":
            query = """
                MATCH (of:Field)<-[u:USED_FOR]-(f:Field)
                MATCH (ds:DataSource)-[h:HAS]->(f)
                RETURN f.field_name AS name, ds.data_source_name AS source_name, COUNT(of) AS dependency_count
                ORDER BY dependency_count DESC
            """
        if query and limit:
            query += f"LIMIT {limit}"

        with registry.driver.session(database=registry.database) as session:
            with session.begin_transaction() as tx:                
                result = tx.run(query)
                results = result.data()
        if results:
            # print(f"Results: {results}")
            from tabulate import tabulate

            print(f"Most used {object.replace('-', ' ')} as dependencies:")
            print(tabulate(results, headers="keys", tablefmt="plain"))

        else:
            click.echo(f"No {object.replace('-', ' ')} found.")


@graph_cmd.command("common-tags")
@click.pass_context
def graph_common_tags(ctx):
    """
    Group objects based on their tags
    """
    store = create_feature_store(ctx)
    registry = store._registry
    if not isinstance(registry, GraphRegistry):
        print("Graph commands are not supported for this type of registry.")
        exit(1)
    else:
        query = """
            MATCH (n)-[:TAG]->(t)
            WITH 
                t.value AS value, 
                HEAD(labels(t)) AS tag, 
                CASE 
                    WHEN "Field" IN labels(n) THEN n.field_name
                    WHEN "Entity" IN labels(n) THEN n.entity_name
                    WHEN "DataSource" IN labels(n) THEN n.data_source_name
                    WHEN "FeatureView" IN labels(n) THEN n.feature_view_name
                    WHEN "OnDemandFeatureView" IN labels(n) THEN n.feature_view_name
                    WHEN "FeatureService" IN labels(n) THEN n.feature_service_name
                    ELSE 'Unknown'
                END AS object_name,
                HEAD(labels(n)) AS object_label
            RETURN 
                tag, 
                value, 
                COLLECT({object_name: object_name, object_label: object_label}) AS objects
            ORDER BY tag, value
        """
        with registry.driver.session(database=registry.database) as session:
            with session.begin_transaction() as tx:                
                result = tx.run(query)
                results = result.data()
        if results:
            from collections import defaultdict

            for result in results:
                new_objects = {}
                objects = result.get('objects', [])
                for obj in objects:
                    label = obj.get('object_label')
                    name = obj.get('object_name')
                    if label in new_objects.keys():
                        new_objects[label].append(name)
                    else:
                        new_objects[label] = [name]
                result["objects"] = new_objects

            from tabulate import tabulate

            # print(f"Most used {object.replace('-', ' ')}:")
            print(tabulate(results, headers="keys", tablefmt="plain"))

        else:
            click.echo(f"No tags found.")


@graph_cmd.command("common-owner")
@click.pass_context
def graph_common_owner(ctx):
    """
    Group objects based on their owner
    """
    store = create_feature_store(ctx)
    registry = store._registry
    if not isinstance(registry, GraphRegistry):
        from collections import defaultdict
        from tabulate import tabulate

        results = defaultdict(list)

        for obj_list, label in [
            (store.list_entities(), 'Entity'),
            (store.list_data_sources(), 'DataSource'),
            (store.list_feature_views(), 'FeatureView'),
            (store.list_on_demand_feature_views(), 'OnDemandFeatureView'),
            (store.list_feature_services(), 'FeatureService')
        ]:
            for obj in obj_list:
                if obj.owner:
                    results[obj.owner].append((label, obj.name))
        if not results:
            click.echo(f"No owners found.")
        else:
            results_list = []
            for owner in sorted(results.keys()):
                new_objects = defaultdict(list)
                for obj_type, obj_name in results[owner]:
                    new_objects[obj_type].append(obj_name)
                
                results_list.append({
                    'Owner': owner,
                    'Objects': dict(new_objects)  
                })
            
            print(f"Result list length: {len(results_list)}")
            # print(tabulate(results_list, headers="keys", tablefmt="plain"))
    else:
        from collections import defaultdict
        from tabulate import tabulate

        query = """
            MATCH (o:Owner)-[:OWNS]->(n)
            WITH 
                o.name AS owner, 
                COALESCE(n.entity_name, n.data_source_name, n.feature_view_name, n.feature_service_name, 'Unknown') AS object_name,
                LABELS(n)[0] AS object_label
            RETURN 
                owner, 
                COLLECT({object_name: object_name, object_label: object_label}) AS Objects
            ORDER BY owner
        """

        with registry.driver.session(database=registry.database) as session:
            with session.begin_transaction() as tx:
                result = tx.run(query)
                results = result.data()

        if results:
            for result in results:
                new_objects = defaultdict(list)
                for obj in result.get('Objects', []):
                    new_objects[obj['object_label']].append(obj['object_name'])
                result["Objects"] = dict(new_objects)  

            print(f"Result list length: {len(results)}")
            # print(tabulate(results, headers="keys", tablefmt="plain"))
        else:
            click.echo(f"No owners found.")

        

@graph_cmd.command("upstream-impact")
@click.option(
    "--data-source",
    "-d",
    type=click.STRING,
    required=True,
    help="The name of the data source for which impacted objects will be gathered."
)
@click.option(
    "--field",
    "-f",
    type=click.STRING,
    help="The specific field from the data source for which impacted objects will be gathered."
)
@click.pass_context
def graph_upstream_impact(ctx, data_source, field: Optional[str]):
    """
    Return the data sources, feature views, on-demand feature views and feature services that use the data source or specific field from the data source if specified
    """
    store = create_feature_store(ctx)
    registry = store._registry
    if not isinstance(registry, GraphRegistry):
        data_sources = set()
        data_sources.add(data_source)
        for ds in [
            *store.list_data_sources(),
        ]:
            if isinstance(ds, PushSource):
                if ds.batch_source.name == data_source:
                    data_sources.add(ds.name)
        feature_views = set()
        for fv in [
            *store.list_feature_views(),
        ]:
            if fv.batch_source.name in data_sources:
                feature_views.add(fv.name)
            if fv.stream_source and fv.stream_source.name in data_sources:
                feature_views.add(fv.name)
        on_demand_feature_views = set()
        for odfv in [
            *store.list_on_demand_feature_views(),
        ]:
            fvs = odfv.source_feature_view_projections.values()
            for fv in fvs:
                if fv.name in feature_views:
                    on_demand_feature_views.add(odfv.name)
            rs = odfv.source_request_sources.values()
            for s in rs:
                if s.name in data_sources:
                    on_demand_feature_views.add(odfv.name)
        feature_services = set()
        for fs in [
            *store.list_feature_services(),
        ]:
            fvs = fs.feature_view_projections
            for fv in fvs:
                if fv.name in feature_views or fv.name in on_demand_feature_views:
                    feature_services.add(fs.name)
        
        data_sources.remove(data_source)
        d = {}
        d["Data Sources"] = sorted(list(data_sources))
        d["Feature Views"] = sorted(list(feature_views))
        d["On-Demand Feature Views"] = sorted(list(on_demand_feature_views))
        d["Feature Services"] = sorted(list(feature_services))

        from tabulate import tabulate

        print(f"Upstream impact of " + (f"field '{field}' from " if field else "") + f"data source '{data_source}':")
        print(tabulate(d, headers="keys", tablefmt="plain"))
        # print("Graph commands are not supported for this type of registry.")
        # exit(1)
    else:
        query = f"""
            MATCH (s:DataSource {{ data_source_name: $data_source_name }})<-[:POPULATED_FROM|RETRIEVES_FROM|BASED_ON|CONSUMES*1..4]-(d)
            WITH DISTINCT d
            RETURN 
                COLLECT(DISTINCT CASE WHEN "DataSource" IN labels(d) THEN d.data_source_name END) AS data_sources,
                COLLECT(DISTINCT CASE WHEN "FeatureView" IN labels(d) THEN d.feature_view_name END) AS feature_views,
                COLLECT(DISTINCT CASE WHEN "OnDemandFeatureView" IN labels(d) THEN d.feature_view_name END) AS on_demand_feature_views,
                COLLECT(DISTINCT CASE WHEN "FeatureService" IN labels(d) THEN d.feature_service_name END) AS feature_services

        """ 
        with registry.driver.session(database=registry.database) as session:
            with session.begin_transaction() as tx:     
                params = {"data_source_name": data_source}

                result = tx.run(query, params)
                # print(f"Relationships: {result.data()}")
                results = result.single()           
                
        if results:
            d = {}
            d["Data Sources"] = sorted(results["data_sources"])
            d["Feature Views"] = sorted(results["feature_views"])
            d["On-Demand Feature Views"] = sorted(results["on_demand_feature_views"])
            d["Feature Services"] = sorted(results["feature_services"])

            from tabulate import tabulate

            print(f"Upstream impact of " + (f"field '{field}' from " if field else "") + f"data source '{data_source}':")
            print(tabulate(d, headers="keys", tablefmt="plain"))

        else:
            click.echo(f"No impacted objects found.")

@graph_cmd.command("served-by")
@click.option(
    "--data-source",
    "-d",
    type=click.STRING,
    required=True,
    help="The name of the data source where the desired field is located."
)
@click.option(
    "--field",
    "-f",
    type=click.STRING,
    required=True,
    help="The specific field from the data source for which feature services will be gathered."
)
@click.pass_context
def graph_served_by(ctx, data_source, field: Optional[str]):
    """
    Return the feature services that serve the field from the specified data source
    """
    store = create_feature_store(ctx)
    registry = store._registry
    if not isinstance(registry, GraphRegistry):
        feature_services = set()
        for fs in [
            *store.list_feature_services(),
        ]:
            fvs = fs.feature_view_projections
            for fv in fvs:
                has_field = False
                for f in fv.features:
                    if f.name == field:
                        has_field = True
                if has_field:
                    # fv_obj = store.get_feature_view(fv.name) or store.get_on_demand_feature_view(fv.name)
                    fv_obj = store.get_feature_view(fv.name)
                    if fv_obj.stream_source:
                        if fv_obj.stream_source.name == data_source:
                            feature_services.add(fs.name)
                    elif fv_obj.batch_source.name == data_source:
                        feature_services.add(fs.name)                    
        
        d = {}
        d["Feature Services"] = sorted(list(feature_services))

        from tabulate import tabulate

        print(f"Feature services that serve field '{field}' from data source '{data_source}':")
        print(tabulate(d, headers="keys", tablefmt="plain"))
        # print("Graph commands are not supported for this type of registry.")
        # exit(1)
    else:
        query = f"""
            MATCH (s:DataSource {{ data_source_name: $data_source_name }})-[:HAS]->(f:Field {{ field_name: $field_name }})
            OPTIONAL MATCH (fs:FeatureService)-[:SERVES]->(f)
            RETURN 
                COLLECT(DISTINCT fs.feature_service_name) AS feature_services
        """ 
        
        with registry.driver.session(database=registry.database) as session:
            with session.begin_transaction() as tx:     
                params = {
                    "data_source_name": data_source,
                    "field_name": field
                }

                result = tx.run(query, params)
                # print(f"Relationships: {result.data()}")
                results = result.single()           
                
        if results:
            d = {}
            d["Feature Services"] = sorted(results["feature_services"])

            from tabulate import tabulate

            print(f"Feature services that serve field '{field}' from data source '{data_source}':")
            print(tabulate(d, headers="keys", tablefmt="plain"))

        else:
            click.echo(f"No impacted objects found.")

if __name__ == "__main__":
    cli()  