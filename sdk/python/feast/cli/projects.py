import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.repo_operations import create_feature_store


@click.group(name="projects")
def projects_cmd():
    """
    Access projects
    """
    pass


@projects_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def project_describe(ctx: click.Context, name: str):
    """
    Describe a project
    """
    store = create_feature_store(ctx)

    try:
        project = store.get_project(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(project)), default_flow_style=False, sort_keys=False
        )
    )


@projects_cmd.command("current_project")
@click.pass_context
def project_current(ctx: click.Context):
    """
    Returns the current project configured with FeatureStore object
    """
    store = create_feature_store(ctx)

    try:
        project = store.get_project(name=None)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(project)), default_flow_style=False, sort_keys=False
        )
    )


@projects_cmd.command(name="list")
@tagsOption
@click.pass_context
def project_list(ctx: click.Context, tags: list[str]):
    """
    List all projects
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for project in store.list_projects(tags=tags_filter):
        table.append([project.name, project.description, project.tags, project.owner])

    from tabulate import tabulate

    print(
        tabulate(
            table, headers=["NAME", "DESCRIPTION", "TAGS", "OWNER"], tablefmt="plain"
        )
    )
