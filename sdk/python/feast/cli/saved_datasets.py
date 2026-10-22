import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.repo_operations import create_feature_store


@click.group(name="saved-datasets")
def saved_datasets_cmd():
    """
    [Experimental] Access saved datasets
    """
    pass


@saved_datasets_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def saved_datasets_describe(ctx: click.Context, name: str):
    """
    [Experimental] Describe a saved dataset
    """
    store = create_feature_store(ctx)

    try:
        saved_dataset = store.get_saved_dataset(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(saved_dataset)),
            default_flow_style=False,
            sort_keys=False,
        )
    )


@saved_datasets_cmd.command(name="list")
@tagsOption
@click.pass_context
def saved_datasets_list(ctx: click.Context, tags: list[str]):
    """
    [Experimental] List all saved datasets
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for saved_dataset in store.list_saved_datasets(tags=tags_filter):
        table.append([saved_dataset.name])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME"], tablefmt="plain"))
