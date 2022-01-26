from dataclasses import dataclass
from typing import Any, Dict, Generic, Iterable, List, Set, Tuple, TypeVar

from feast.base_feature_view import BaseFeatureView
from feast.diff.property_diff import PropertyDiff, TransitionType
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_view import DUMMY_ENTITY_NAME
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
from feast.registry import FEAST_OBJECT_TYPES, FeastObjectType, Registry
from feast.repo_contents import RepoContents

Fco = TypeVar("Fco", Entity, BaseFeatureView, FeatureService)


@dataclass
class FcoDiff(Generic[Fco]):
    name: str
    fco_type: FeastObjectType
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

    def to_string(self):
        from colorama import Fore, Style

        log_string = ""

        message_action_map = {
            TransitionType.CREATE: ("Created", Fore.GREEN),
            TransitionType.DELETE: ("Deleted", Fore.RED),
            TransitionType.UNCHANGED: ("Unchanged", Fore.LIGHTBLUE_EX),
            TransitionType.UPDATE: ("Updated", Fore.YELLOW),
        }
        for fco_diff in self.fco_diffs:
            if fco_diff.name == DUMMY_ENTITY_NAME:
                continue
            action, color = message_action_map[fco_diff.transition_type]
            log_string += f"{action} {fco_diff.fco_type.value} {Style.BRIGHT + color}{fco_diff.name}{Style.RESET_ALL}\n"
            if fco_diff.transition_type == TransitionType.UPDATE:
                for _p in fco_diff.fco_property_diffs:
                    log_string += f"\t{_p.property_name}: {Style.BRIGHT + color}{_p.val_existing}{Style.RESET_ALL} -> {Style.BRIGHT + Fore.LIGHTGREEN_EX}{_p.val_declared}{Style.RESET_ALL}\n"

        return log_string


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


def diff_registry_objects(
    current: Fco, new: Fco, object_type: FeastObjectType
) -> FcoDiff:
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
        name=new_proto.spec.name,
        fco_type=object_type,
        current_fco=current,
        new_fco=new,
        fco_property_diffs=property_diffs,
        transition_type=transition,
    )


def extract_objects_for_keep_delete_update_add(
    registry: Registry, current_project: str, desired_repo_contents: RepoContents,
) -> Tuple[
    Dict[FeastObjectType, Set[Fco]],
    Dict[FeastObjectType, Set[Fco]],
    Dict[FeastObjectType, Set[Fco]],
    Dict[FeastObjectType, Set[Fco]],
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

    registry_object_type_to_objects: Dict[FeastObjectType, List[Any]]
    registry_object_type_to_objects = {
        FeastObjectType.ENTITY: registry.list_entities(project=current_project),
        FeastObjectType.FEATURE_VIEW: registry.list_feature_views(
            project=current_project
        ),
        FeastObjectType.ON_DEMAND_FEATURE_VIEW: registry.list_on_demand_feature_views(
            project=current_project
        ),
        FeastObjectType.REQUEST_FEATURE_VIEW: registry.list_request_feature_views(
            project=current_project
        ),
        FeastObjectType.FEATURE_SERVICE: registry.list_feature_services(
            project=current_project
        ),
    }
    registry_object_type_to_repo_contents: Dict[FeastObjectType, Set[Any]]
    registry_object_type_to_repo_contents = {
        FeastObjectType.ENTITY: desired_repo_contents.entities,
        FeastObjectType.FEATURE_VIEW: desired_repo_contents.feature_views,
        FeastObjectType.ON_DEMAND_FEATURE_VIEW: desired_repo_contents.on_demand_feature_views,
        FeastObjectType.REQUEST_FEATURE_VIEW: desired_repo_contents.request_feature_views,
        FeastObjectType.FEATURE_SERVICE: desired_repo_contents.feature_services,
    }

    for object_type in FEAST_OBJECT_TYPES:
        (
            to_keep,
            to_delete,
            to_update,
            to_add,
        ) = tag_objects_for_keep_delete_update_add(
            registry_object_type_to_objects[object_type],
            registry_object_type_to_repo_contents[object_type],
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

    for object_type in FEAST_OBJECT_TYPES:
        objects_to_keep = objs_to_keep[object_type]
        objects_to_delete = objs_to_delete[object_type]
        objects_to_update = objs_to_update[object_type]
        objects_to_add = objs_to_add[object_type]

        for e in objects_to_add:
            diff.add_fco_diff(
                FcoDiff(
                    name=e.name,
                    fco_type=object_type,
                    current_fco=None,
                    new_fco=e,
                    fco_property_diffs=[],
                    transition_type=TransitionType.CREATE,
                )
            )
        for e in objects_to_delete:
            diff.add_fco_diff(
                FcoDiff(
                    name=e.name,
                    fco_type=object_type,
                    current_fco=e,
                    new_fco=None,
                    fco_property_diffs=[],
                    transition_type=TransitionType.DELETE,
                )
            )
        for e in objects_to_update:
            current_obj = [_e for _e in objects_to_keep if _e.name == e.name][0]
            diff.add_fco_diff(diff_registry_objects(current_obj, e, object_type))

    return diff


def apply_diff_to_registry(
    registry: Registry, registry_diff: RegistryDiff, project: str, commit: bool = True
):
    """
    Applies the given diff to the given Feast project in the registry.

    Args:
        registry: The registry to be updated.
        registry_diff: The diff to apply.
        project: Feast project to be updated.
        commit: Whether the change should be persisted immediately
    """
    for fco_diff in registry_diff.fco_diffs:
        # There is no need to delete the FCO on an update, since applying the new FCO
        # will automatically delete the existing FCO.
        if fco_diff.transition_type == TransitionType.DELETE:
            if fco_diff.fco_type == FeastObjectType.ENTITY:
                registry.delete_entity(fco_diff.current_fco.name, project, False)
            elif fco_diff.fco_type == FeastObjectType.FEATURE_SERVICE:
                registry.delete_feature_service(
                    fco_diff.current_fco.name, project, False
                )
            elif fco_diff.fco_type in [
                FeastObjectType.FEATURE_VIEW,
                FeastObjectType.ON_DEMAND_FEATURE_VIEW,
                FeastObjectType.REQUEST_FEATURE_VIEW,
            ]:
                registry.delete_feature_view(
                    fco_diff.current_fco.name, project, False,
                )

        if fco_diff.transition_type in [
            TransitionType.CREATE,
            TransitionType.UPDATE,
        ]:
            if fco_diff.fco_type == FeastObjectType.ENTITY:
                registry.apply_entity(fco_diff.new_fco, project, False)
            elif fco_diff.fco_type == FeastObjectType.FEATURE_SERVICE:
                registry.apply_feature_service(fco_diff.new_fco, project, False)
            elif fco_diff.fco_type in [
                FeastObjectType.FEATURE_VIEW,
                FeastObjectType.ON_DEMAND_FEATURE_VIEW,
                FeastObjectType.REQUEST_FEATURE_VIEW,
            ]:
                registry.apply_feature_view(fco_diff.new_fco, project, False)

    if commit:
        registry.commit()
