from typing import Any

import click
import yaml
from bigtree import Node
from colorama import Fore, Style

from feast import (
    BatchFeatureView,
    Entity,
    FeatureService,
    FeatureView,
    OnDemandFeatureView,
    StreamFeatureView,
    utils,
)
from feast.cli import cli_utils
from feast.cli.cli_options import tagsOption
from feast.data_source import DataSource
from feast.errors import FeastObjectNotFoundException
from feast.permissions.policy import RoleBasedPolicy
from feast.repo_operations import create_feature_store
from feast.saved_dataset import SavedDataset, ValidationReference


@click.group(name="permissions")
def feast_permissions_cmd():
    """
    Access permissions
    """
    pass


@feast_permissions_cmd.command(name="list")
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Print the resources matching each configured permission",
)
@tagsOption
@click.pass_context
def feast_permissions_list_command(ctx: click.Context, verbose: bool, tags: list[str]):
    from tabulate import tabulate

    table: list[Any] = []
    tags_filter = utils.tags_list_to_dict(tags)

    store = create_feature_store(ctx)

    permissions = store.list_permissions(tags=tags_filter)

    root_node = Node("permissions")
    roles: set[str] = set()

    for p in permissions:
        policy = p.policy
        if not verbose:
            cli_utils.handle_not_verbose_permissions_command(p, policy, table)
        else:
            if isinstance(policy, RoleBasedPolicy) and len(policy.get_roles()) > 0:
                roles = set(policy.get_roles())
                permission_node = Node(
                    p.name + " " + str(list(roles)), parent=root_node
                )
            else:
                permission_node = Node(p.name, parent=root_node)

            for feast_type in p.types:
                if feast_type in [
                    FeatureView,
                    OnDemandFeatureView,
                    BatchFeatureView,
                    StreamFeatureView,
                ]:
                    cli_utils.handle_fv_verbose_permissions_command(
                        feast_type,  # type: ignore[arg-type]
                        p,
                        permission_node,
                        store,
                        tags_filter,
                    )
                elif feast_type == Entity:
                    cli_utils.handle_entity_verbose_permissions_command(
                        feast_type,  # type: ignore[arg-type]
                        p,
                        permission_node,
                        store,
                        tags_filter,
                    )
                elif feast_type == FeatureService:
                    cli_utils.handle_fs_verbose_permissions_command(
                        feast_type,  # type: ignore[arg-type]
                        p,
                        permission_node,
                        store,
                        tags_filter,
                    )
                elif feast_type == DataSource:
                    cli_utils.handle_ds_verbose_permissions_command(
                        feast_type,  # type: ignore[arg-type]
                        p,
                        permission_node,
                        store,
                        tags_filter,
                    )
                elif feast_type == ValidationReference:
                    cli_utils.handle_vr_verbose_permissions_command(
                        feast_type,  # type: ignore[arg-type]
                        p,
                        permission_node,
                        store,
                        tags_filter,
                    )
                elif feast_type == SavedDataset:
                    cli_utils.handle_sd_verbose_permissions_command(
                        feast_type,  # type: ignore[arg-type]
                        p,
                        permission_node,
                        store,
                        tags_filter,
                    )

    if not verbose:
        print(
            tabulate(
                table,
                headers=[
                    "NAME",
                    "TYPES",
                    "NAME_PATTERNS",
                    "ACTIONS",
                    "ROLES",
                    "REQUIRED_TAGS",
                ],
                tablefmt="plain",
            )
        )
    else:
        cli_utils.print_permission_verbose_example()

        print("Permissions:")
        print("")
        root_node.show()


@feast_permissions_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def permission_describe(ctx: click.Context, name: str):
    """
    Describe a permission
    """
    store = create_feature_store(ctx)

    try:
        permission = store.get_permission(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(permission)), default_flow_style=False, sort_keys=False
        )
    )


@feast_permissions_cmd.command(name="check")
@click.pass_context
def feast_permissions_check_command(ctx: click.Context):
    """
    Validate the permissions configuration
    """
    from tabulate import tabulate

    all_unsecured_table: list[Any] = []
    store = create_feature_store(ctx)
    permissions = store.list_permissions()
    objects = cli_utils.fetch_all_feast_objects(
        store=store,
    )

    print(
        f"{Style.BRIGHT + Fore.RED}The following resources are not secured by any permission configuration:{Style.RESET_ALL}"
    )
    for o in objects:
        cli_utils.handle_permissions_check_command(
            object=o, permissions=permissions, table=all_unsecured_table
        )
    print(
        tabulate(
            all_unsecured_table,
            headers=[
                "NAME",
                "TYPE",
            ],
            tablefmt="plain",
        )
    )

    all_unsecured_actions_table: list[Any] = []
    print(
        f"{Style.BRIGHT + Fore.RED}The following actions are not secured by any permission configuration (Note: this might not be a security concern, depending on the used APIs):{Style.RESET_ALL}"
    )
    for o in objects:
        cli_utils.handle_permissions_check_command_with_actions(
            object=o, permissions=permissions, table=all_unsecured_actions_table
        )
    print(
        tabulate(
            all_unsecured_actions_table,
            headers=[
                "NAME",
                "TYPE",
                "UNSECURED ACTIONS",
            ],
            tablefmt="plain",
        )
    )


@feast_permissions_cmd.command(name="list-roles")
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Print the resources and actions permitted to each configured role",
)
@click.pass_context
def feast_permissions_list_roles_command(ctx: click.Context, verbose: bool):
    """
    List all the configured roles
    """
    from tabulate import tabulate

    table: list[Any] = []
    store = create_feature_store(ctx)
    permissions = store.list_permissions()
    if not verbose:
        cli_utils.handler_list_all_permissions_roles(
            permissions=permissions, table=table
        )
        print(
            tabulate(
                table,
                headers=[
                    "ROLE NAME",
                ],
                tablefmt="grid",
            )
        )
    else:
        objects = cli_utils.fetch_all_feast_objects(
            store=store,
        )
        cli_utils.handler_list_all_permissions_roles_verbose(
            objects=objects, permissions=permissions, table=table
        )
        print(
            tabulate(
                table,
                headers=[
                    "ROLE NAME",
                    "RESOURCE NAME",
                    "RESOURCE TYPE",
                    "PERMITTED ACTIONS",
                ],
                tablefmt="plain",
            )
        )
