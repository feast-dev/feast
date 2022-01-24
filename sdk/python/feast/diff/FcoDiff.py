from dataclasses import dataclass
from typing import Any, Dict, Generic, Iterable, List, Set, Tuple, TypeVar

from feast.base_feature_view import BaseFeatureView
from feast.diff.property_diff import PropertyDiff, TransitionType
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView as OnDemandFeatureViewProto,
)
from feast.protos.feast.core.RequestFeatureView_pb2 import (
    RequestFeatureView as RequestFeatureViewProto,
)
from feast.registry import REGISTRY_OBJECT_TYPE_TO_STR, REGISTRY_OBJECT_TYPES, Registry
from feast.repo_contents import RepoContents

Fco = TypeVar("Fco", Entity, BaseFeatureView, FeatureService)


@dataclass
class FcoDiff(Generic[Fco]):
    name: str
    fco_type: str
    current_fco: Fco
    new_fco: Fco
    fco_property_diffs: List[PropertyDiff]
    transition_type: TransitionType


@dataclass
class RegistryDiff:
    fco_diffs: List[FcoDiff]

    def __init__(self):
        self.fco_diffs = []

    def add_fco_diff(self, fco_diff: FcoDiff):
        self.fco_diffs.append(fco_diff)


def tag_objects_for_keep_delete_update_add(
    existing_objs: Iterable[Fco], desired_objs: Iterable[Fco]
) -> Tuple[Set[Fco], Set[Fco], Set[Fco], Set[Fco]]:
    existing_obj_names = {e.name for e in existing_objs}
    desired_obj_names = {e.name for e in desired_objs}

    objs_to_add = {e for e in desired_objs if e.name not in existing_obj_names}
    objs_to_update = {e for e in desired_objs if e.name in existing_obj_names}
    objs_to_keep = {e for e in existing_objs if e.name in desired_obj_names}
    objs_to_delete = {e for e in existing_objs if e.name not in desired_obj_names}

    return objs_to_keep, objs_to_delete, objs_to_update, objs_to_add


FcoProto = TypeVar(
    "FcoProto",
    EntityProto,
    FeatureViewProto,
    FeatureServiceProto,
    OnDemandFeatureViewProto,
    RequestFeatureViewProto,
)


def tag_proto_objects_for_keep_delete_add(
    existing_objs: Iterable[FcoProto], desired_objs: Iterable[FcoProto]
) -> Tuple[Iterable[FcoProto], Iterable[FcoProto], Iterable[FcoProto]]:
    existing_obj_names = {e.spec.name for e in existing_objs}
    desired_obj_names = {e.spec.name for e in desired_objs}

    objs_to_add = [e for e in desired_objs if e.spec.name not in existing_obj_names]
    objs_to_keep = [e for e in desired_objs if e.spec.name in existing_obj_names]
    objs_to_delete = [e for e in existing_objs if e.spec.name not in desired_obj_names]

    return objs_to_keep, objs_to_delete, objs_to_add


FIELDS_TO_IGNORE = {"project"}


def diff_registry_objects(current: Fco, new: Fco, object_type: str) -> FcoDiff:
    current_proto = current.to_proto()
    new_proto = new.to_proto()
    assert current_proto.DESCRIPTOR.full_name == new_proto.DESCRIPTOR.full_name
    property_diffs = []
    transition: TransitionType = TransitionType.UNCHANGED
    if current_proto.spec != new_proto.spec:
        for _field in current_proto.spec.DESCRIPTOR.fields:
            if _field.name in FIELDS_TO_IGNORE:
                continue
            if getattr(current_proto.spec, _field.name) != getattr(
                new_proto.spec, _field.name
            ):
                transition = TransitionType.UPDATE
                property_diffs.append(
                    PropertyDiff(
                        _field.name,
                        getattr(current_proto.spec, _field.name),
                        getattr(new_proto.spec, _field.name),
                    )
                )
    return FcoDiff(
        new_proto.spec.name, object_type, current, new, property_diffs, transition,
    )


def extract_objects_for_keep_delete_update_add(
    registry: Registry, current_project: str, desired_repo_contents: RepoContents,
) -> Tuple[
    Dict[str, Set[Fco]], Dict[str, Set[Fco]], Dict[str, Set[Fco]], Dict[str, Set[Fco]]
]:
    """
    Returns the objects in the registry that must be modified to achieve the desired repo state.

    Args:
        registry: The registry storing the current repo state.
        current_project: The Feast project whose objects should be compared.
        desired_repo_contents: The desired repo state.
    """
    objs_to_keep = {}
    objs_to_delete = {}
    objs_to_update = {}
    objs_to_add = {}

    registry_object_type_to_objects: Dict[str, List[Any]]
    registry_object_type_to_objects = {
        "entities": registry.list_entities(project=current_project),
        "feature_views": registry.list_feature_views(project=current_project),
        "on_demand_feature_views": registry.list_on_demand_feature_views(
            project=current_project
        ),
        "request_feature_views": registry.list_request_feature_views(
            project=current_project
        ),
        "feature_services": registry.list_feature_services(project=current_project),
    }

    for object_type in REGISTRY_OBJECT_TYPES:
        (
            to_keep,
            to_delete,
            to_update,
            to_add,
        ) = tag_objects_for_keep_delete_update_add(
            registry_object_type_to_objects[object_type],
            getattr(desired_repo_contents, object_type),
        )

        objs_to_keep[object_type] = to_keep
        objs_to_delete[object_type] = to_delete
        objs_to_update[object_type] = to_update
        objs_to_add[object_type] = to_add

    return objs_to_keep, objs_to_delete, objs_to_update, objs_to_add


def diff_between(
    registry: Registry, current_project: str, desired_repo_contents: RepoContents,
) -> RegistryDiff:
    """
    Returns the difference between the current and desired repo states.

    Args:
        registry: The registry storing the current repo state.
        current_project: The Feast project for which the diff is being computed.
        desired_repo_contents: The desired repo state.
    """
    diff = RegistryDiff()

    (
        objs_to_keep,
        objs_to_delete,
        objs_to_update,
        objs_to_add,
    ) = extract_objects_for_keep_delete_update_add(
        registry, current_project, desired_repo_contents
    )

    for object_type in REGISTRY_OBJECT_TYPES:
        objects_to_keep = objs_to_keep[object_type]
        objects_to_delete = objs_to_delete[object_type]
        objects_to_update = objs_to_update[object_type]
        objects_to_add = objs_to_add[object_type]

        for e in objects_to_add:
            diff.add_fco_diff(
                FcoDiff(
                    e.name,
                    REGISTRY_OBJECT_TYPE_TO_STR[object_type],
                    None,
                    e,
                    [],
                    TransitionType.CREATE,
                )
            )
        for e in objects_to_delete:
            diff.add_fco_diff(
                FcoDiff(
                    e.name,
                    REGISTRY_OBJECT_TYPE_TO_STR[object_type],
                    e,
                    None,
                    [],
                    TransitionType.DELETE,
                )
            )
        for e in objects_to_update:
            current_obj = [_e for _e in objects_to_keep if _e.name == e.name][0]
            diff.add_fco_diff(
                diff_registry_objects(
                    current_obj, e, REGISTRY_OBJECT_TYPE_TO_STR[object_type]
                )
            )

    return diff
