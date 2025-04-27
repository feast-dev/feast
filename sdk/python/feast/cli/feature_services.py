import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.repo_operations import create_feature_store


@click.group(name="feature-services")
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
@tagsOption
@click.pass_context
def feature_service_list(ctx: click.Context, tags: list[str]):
    """
    List all feature services
    """
    store = create_feature_store(ctx)
    feature_services = []
    tags_filter = utils.tags_list_to_dict(tags)
    for feature_service in store.list_feature_services(tags=tags_filter):
        feature_names = []
        for projection in feature_service.feature_view_projections:
            feature_names.extend(
                [f"{projection.name}:{feature.name}" for feature in projection.features]
            )
        feature_services.append([feature_service.name, ", ".join(feature_names)])

    from tabulate import tabulate

    print(tabulate(feature_services, headers=["NAME", "FEATURES"], tablefmt="plain"))
