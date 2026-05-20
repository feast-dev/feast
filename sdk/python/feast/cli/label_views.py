import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.repo_operations import create_feature_store


@click.group(name="label-views")
def label_views_cmd():
    """
    [Alpha] Access label views
    """
    pass


@label_views_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def label_view_describe(ctx: click.Context, name: str):
    """
    [Alpha] Describe a label view
    """
    store = create_feature_store(ctx)

    try:
        label_view = store.get_label_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(label_view)),
            default_flow_style=False,
            sort_keys=False,
        )
    )


@label_views_cmd.command(name="list")
@tagsOption
@click.pass_context
def label_view_list(ctx: click.Context, tags: list[str]):
    """
    [Alpha] List all label views
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for label_view in store.list_label_views(tags=tags_filter):
        table.append(
            [
                label_view.name,
                ", ".join(label_view.entities) if label_view.entities else "n/a",
                str(label_view.conflict_policy.value),
            ]
        )

    from tabulate import tabulate

    print(
        tabulate(
            table,
            headers=["NAME", "ENTITIES", "CONFLICT_POLICY"],
            tablefmt="plain",
        )
    )
