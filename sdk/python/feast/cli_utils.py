from typing import Any, Optional

from bigtree import Node
from colorama import Fore, Style

from feast import (
    BatchFeatureView,
    FeatureService,
    FeatureStore,
    FeatureView,
    OnDemandFeatureView,
    StreamFeatureView,
)
from feast.feast_object import FeastObject
from feast.permissions.permission import Permission
from feast.permissions.policy import Policy, RoleBasedPolicy


def print_permission_verbose_example():
    print("")
    print(
        f"{Style.BRIGHT + Fore.GREEN}The structure of the {Style.BRIGHT + Fore.WHITE}feast-permissions list --verbose {Style.BRIGHT + Fore.GREEN}command will be as in the following example:"
    )
    print("")
    print(f"{Style.DIM}For example: {Style.RESET_ALL}{Style.BRIGHT + Fore.GREEN}")
    print("")
    explanation_root_node = Node("permissions")
    explanation_permission_node = Node(
        "permission_1" + " " + str(["role names list"]),
        parent=explanation_root_node,
    )
    Node(
        FeatureView.__name__ + ": " + str(["feature view names"]),
        parent=explanation_permission_node,
    )
    Node(FeatureService.__name__ + ": none", parent=explanation_permission_node)
    Node("..", parent=explanation_permission_node)
    Node(
        "permission_2" + " " + str(["role names list"]),
        parent=explanation_root_node,
    )
    Node("..", parent=explanation_root_node)
    explanation_root_node.show()
    print(
        f"""
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------{Style.RESET_ALL}
            """
    )


def handle_sd_verbose_permissions_command(
    feast_type: list[FeastObject],
    p: Permission,
    policy_node: Node,
    store: FeatureStore,
    tags_filter: Optional[dict[str, str]],
):
    saved_datasets = store.list_saved_datasets(tags=tags_filter)
    saved_datasets_names = set()
    for sd in saved_datasets:
        if p.match_resource(sd):
            saved_datasets_names.add(sd.name)
    if len(saved_datasets_names) > 0:
        Node(
            feast_type.__name__ + ": " + str(list(saved_datasets_names)),  # type: ignore[union-attr, attr-defined]
            parent=policy_node,
        )
    else:
        Node(feast_type.__name__ + ": none", parent=policy_node)  # type: ignore[union-attr, attr-defined]


def handle_vr_verbose_permissions_command(
    feast_type: list[FeastObject],
    p: Permission,
    policy_node: Node,
    store: FeatureStore,
    tags_filter: Optional[dict[str, str]],
):
    validation_references = store.list_validation_references(tags=tags_filter)
    validation_references_names = set()
    for vr in validation_references:
        if p.match_resource(vr):
            validation_references_names.add(vr.name)
    if len(validation_references_names) > 0:
        Node(
            feast_type.__name__ + ": " + str(list(validation_references_names)),  # type: ignore[union-attr, attr-defined]
            parent=policy_node,
        )
    else:
        Node(feast_type.__name__ + ": none", parent=policy_node)  # type: ignore[union-attr, attr-defined]


def handle_ds_verbose_permissions_command(
    feast_type: list[FeastObject],
    p: Permission,
    policy_node: Node,
    store: FeatureStore,
    tags_filter: Optional[dict[str, str]],
):
    data_sources = store.list_data_sources(tags=tags_filter)
    data_sources_names = set()
    for ds in data_sources:
        if p.match_resource(ds):
            data_sources_names.add(ds.name)
    if len(data_sources_names) > 0:
        Node(
            feast_type.__name__ + ": " + str(list(data_sources_names)),  # type: ignore[union-attr, attr-defined]
            parent=policy_node,
        )
    else:
        Node(feast_type.__name__ + ": none", parent=policy_node)  # type: ignore[union-attr, attr-defined]


def handle_fs_verbose_permissions_command(
    feast_type: list[FeastObject],
    p: Permission,
    policy_node: Node,
    store: FeatureStore,
    tags_filter: Optional[dict[str, str]],
):
    feature_services = store.list_feature_services(tags=tags_filter)
    feature_services_names = set()
    for fs in feature_services:
        if p.match_resource(fs):
            feature_services_names.add(fs.name)
    if len(feature_services_names) > 0:
        Node(
            feast_type.__name__ + ": " + str(list(feature_services_names)),  # type: ignore[union-attr, attr-defined]
            parent=policy_node,
        )
    else:
        Node(feast_type.__name__ + ": none", parent=policy_node)  # type: ignore[union-attr, attr-defined]


def handle_entity_verbose_permissions_command(
    feast_type: list[FeastObject],
    p: Permission,
    policy_node: Node,
    store: FeatureStore,
    tags_filter: Optional[dict[str, str]],
):
    entities = store.list_entities(tags=tags_filter)
    entities_names = set()
    for e in entities:
        if p.match_resource(e):
            entities_names.add(e.name)
    if len(entities_names) > 0:
        Node(feast_type.__name__ + ": " + str(list(entities_names)), parent=policy_node)  # type: ignore[union-attr, attr-defined]
    else:
        Node(feast_type.__name__ + ": none", parent=policy_node)  # type: ignore[union-attr, attr-defined]


def handle_fv_verbose_permissions_command(
    feast_type: list[FeastObject],
    p: Permission,
    policy_node: Node,
    store: FeatureStore,
    tags_filter: Optional[dict[str, str]],
):
    feature_views = []
    feature_views_names = set()
    if feast_type == FeatureView:
        feature_views = store.list_all_feature_views(tags=tags_filter)  # type: ignore[assignment]
    elif feast_type == OnDemandFeatureView:
        feature_views = store.list_on_demand_feature_views(
            tags=tags_filter  # type: ignore[assignment]
        )
    elif feast_type == BatchFeatureView:
        feature_views = store.list_batch_feature_views(tags=tags_filter)  # type: ignore[assignment]
    elif feast_type == StreamFeatureView:
        feature_views = store.list_stream_feature_views(
            tags=tags_filter  # type: ignore[assignment]
        )
    for fv in feature_views:
        if p.match_resource(fv):
            feature_views_names.add(fv.name)
    if len(feature_views_names) > 0:
        Node(
            feast_type.__name__ + " " + str(list(feature_views_names)),  # type: ignore[union-attr, attr-defined]
            parent=policy_node,
        )
    else:
        Node(feast_type.__name__ + ": none", parent=policy_node)  # type: ignore[union-attr, attr-defined]


def handle_not_verbose_permissions_command(
    p: Permission, policy: Policy, table: list[Any]
):
    roles: set[str] = set()
    if isinstance(policy, RoleBasedPolicy):
        roles = set(policy.get_roles())
    table.append(
        [
            p.name,
            [t.__name__ for t in p.types],  # type: ignore[union-attr, attr-defined]
            p.with_subclasses,
            p.name_pattern,
            p.actions,
            roles,
        ]
    )
