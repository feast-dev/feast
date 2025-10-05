import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.repo_operations import create_feature_store


@click.group(name="data-sources")
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
@tagsOption
@click.pass_context
def data_source_list(ctx: click.Context, tags: list[str]):
    """
    List all data sources
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for datasource in store.list_data_sources(tags=tags_filter):
        table.append([datasource.name, datasource.__class__])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "CLASS"], tablefmt="plain"))
