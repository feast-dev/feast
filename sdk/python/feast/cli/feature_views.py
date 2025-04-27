import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_operations import create_feature_store


@click.group(name="feature-views")
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
@tagsOption
@click.pass_context
def feature_view_list(ctx: click.Context, tags: list[str]):
    """
    List all feature views
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for feature_view in [
        *store.list_batch_feature_views(tags=tags_filter),
        *store.list_on_demand_feature_views(tags=tags_filter),
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
