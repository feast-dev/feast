import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.repo_operations import create_feature_store


@click.group(name="entities")
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
@tagsOption
@click.pass_context
def entity_list(ctx: click.Context, tags: list[str]):
    """
    List all entities
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for entity in store.list_entities(tags=tags_filter):
        table.append([entity.name, entity.description, entity.value_type])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "DESCRIPTION", "TYPE"], tablefmt="plain"))
