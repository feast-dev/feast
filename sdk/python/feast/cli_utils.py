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
from feast.permissions.action import ALL_ACTIONS
from feast.permissions.decision import DecisionEvaluator
from feast.permissions.permission import Permission
from feast.permissions.policy import Policy, RoleBasedPolicy
from feast.permissions.user import User


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
            _to_multi_line([t.__name__ for t in p.types]),  # type: ignore[union-attr, attr-defined]
            p.with_subclasses,
            p.name_pattern,
            _to_multi_line([a.value.upper() for a in p.actions]),
            _to_multi_line(sorted(roles)),
        ]
    )


def fetch_all_feast_objects(store: FeatureStore) -> list[FeastObject]:
    objects: list[FeastObject] = []
    objects.extend(store.list_entities())
    objects.extend(store.list_all_feature_views())
    objects.extend(store.list_batch_feature_views())
    objects.extend(store.list_feature_services())
    objects.extend(store.list_data_sources())
    objects.extend(store.list_validation_references())
    objects.extend(store.list_saved_datasets())
    objects.extend(store.list_permissions())
    return objects


def handle_permissions_check_command(
    object: FeastObject, permissions: list[Permission], table: list[Any]
):
    for p in permissions:
        if p.match_resource(object):
            return
    table.append(
        [
            object.name,
            type(object).__name__,
        ]
    )


def handle_permissions_check_command_with_actions(
    object: FeastObject, permissions: list[Permission], table: list[Any]
):
    unmatched_actions = ALL_ACTIONS.copy()
    for p in permissions:
        if p.match_resource(object):
            for action in ALL_ACTIONS:
                if p.match_actions([action]) and action in unmatched_actions:
                    unmatched_actions.remove(action)

    if unmatched_actions:
        table.append(
            [
                object.name,
                type(object).__name__,
                _to_multi_line([a.value.upper() for a in unmatched_actions]),
            ]
        )


def fetch_all_permission_roles(permissions: list[Permission]) -> list[str]:
    all_roles = set()
    for p in permissions:
        if isinstance(p.policy, RoleBasedPolicy) and len(p.policy.get_roles()) > 0:
            all_roles.update(p.policy.get_roles())

    return sorted(all_roles)


def handler_list_all_permissions_roles(permissions: list[Permission], table: list[Any]):
    all_roles = fetch_all_permission_roles(permissions)
    for role in all_roles:
        table.append(
            [
                role,
            ]
        )


def handler_list_all_permissions_roles_verbose(
    objects: list[FeastObject], permissions: list[Permission], table: list[Any]
):
    all_roles = fetch_all_permission_roles(permissions)

    for role in all_roles:
        for o in objects:
            permitted_actions = ALL_ACTIONS.copy()
            for action in ALL_ACTIONS:
                # Following code is derived from enforcer.enforce_policy but has a different return type and does not raise PermissionError
                matching_permissions = [
                    p
                    for p in permissions
                    if p.match_resource(o) and p.match_actions([action])
                ]

                if matching_permissions:
                    evaluator = DecisionEvaluator(
                        len(matching_permissions),
                    )
                    for p in matching_permissions:
                        permission_grant, permission_explanation = (
                            p.policy.validate_user(user=User(username="", roles=[role]))
                        )
                        evaluator.add_grant(
                            permission_grant,
                            f"Permission {p.name} denied access: {permission_explanation}",
                        )

                        if evaluator.is_decided():
                            grant, explanations = evaluator.grant()
                            if not grant:
                                permitted_actions.remove(action)
                            break
                else:
                    permitted_actions.remove(action)

            table.append(
                [
                    role,
                    o.name,
                    type(o).__name__,
                    _to_multi_line([a.value.upper() for a in permitted_actions]),
                ]
            )


def _to_multi_line(values: list[str]) -> str:
    if not values:
        return "-"
    return "\n".join(values)
