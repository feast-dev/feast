import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.repo_operations import create_feature_store


@click.group(name="on-demand-feature-views")
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
@tagsOption
@click.pass_context
def on_demand_feature_view_list(ctx: click.Context, tags: list[str]):
    """
    [Experimental] List all on demand feature views
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for on_demand_feature_view in store.list_on_demand_feature_views(tags=tags_filter):
        table.append([on_demand_feature_view.name])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME"], tablefmt="plain"))
