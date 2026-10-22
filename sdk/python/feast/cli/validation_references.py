import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.repo_operations import create_feature_store


@click.group(name="validation-references")
def validation_references_cmd():
    """
    [Experimental] Access validation references
    """
    pass


@validation_references_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def validation_references_describe(ctx: click.Context, name: str):
    """
    [Experimental] Describe a validation reference
    """
    store = create_feature_store(ctx)

    try:
        validation_reference = store.get_validation_reference(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(validation_reference)),
            default_flow_style=False,
            sort_keys=False,
        )
    )


@validation_references_cmd.command(name="list")
@tagsOption
@click.pass_context
def validation_references_list(ctx: click.Context, tags: list[str]):
    """
    [Experimental] List all validation references
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for validation_reference in store.list_validation_references(tags=tags_filter):
        table.append([validation_reference.name])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME"], tablefmt="plain"))
