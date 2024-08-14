from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, TypeVar, cast

from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.diff.property_diff import PropertyDiff, TransitionType
from feast.entity import Entity
from feast.feast_object import FeastObject, FeastObjectSpecProto
from feast.feature_service import FeatureService
from feast.feature_view import DUMMY_ENTITY_NAME
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.registry.registry import FEAST_OBJECT_TYPES, FeastObjectType
from feast.permissions.permission import Permission
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView as OnDemandFeatureViewProto,
)
from feast.protos.feast.core.OnDemandFeatureView_pb2 import OnDemandFeatureViewSpec
from feast.protos.feast.core.Permission_pb2 import Permission as PermissionProto
from feast.protos.feast.core.SavedDataset_pb2 import SavedDataset as SavedDatasetProto
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView as StreamFeatureViewProto,
)
from feast.protos.feast.core.ValidationProfile_pb2 import (
    ValidationReference as ValidationReferenceProto,
)
from feast.repo_contents import RepoContents


@dataclass
class FeastObjectDiff:
    name: str
    feast_object_type: FeastObjectType
    current_feast_object: Optional[FeastObject]
    new_feast_object: Optional[FeastObject]
    feast_object_property_diffs: List[PropertyDiff]
    transition_type: TransitionType


@dataclass
class RegistryDiff:
    feast_object_diffs: List[FeastObjectDiff]

    def __init__(self):
        self.feast_object_diffs = []

    def add_feast_object_diff(self, feast_object_diff: FeastObjectDiff):
        self.feast_object_diffs.append(feast_object_diff)

    def to_string(self):
        from colorama import Fore, Style

        log_string = ""

        message_action_map = {
            TransitionType.CREATE: ("Created", Fore.GREEN),
            TransitionType.DELETE: ("Deleted", Fore.RED),
            TransitionType.UNCHANGED: ("Unchanged", Fore.LIGHTBLUE_EX),
            TransitionType.UPDATE: ("Updated", Fore.YELLOW),
        }
        for feast_object_diff in self.feast_object_diffs:
            if feast_object_diff.name == DUMMY_ENTITY_NAME:
                continue
            if feast_object_diff.transition_type == TransitionType.UNCHANGED:
                continue
            if feast_object_diff.feast_object_type == FeastObjectType.DATA_SOURCE:
                # TODO(adchia): Print statements out starting in Feast 0.24
                continue
            action, color = message_action_map[feast_object_diff.transition_type]
            log_string += f"{action} {feast_object_diff.feast_object_type.value} {Style.BRIGHT + color}{feast_object_diff.name}{Style.RESET_ALL}\n"
            if feast_object_diff.transition_type == TransitionType.UPDATE:
                for _p in feast_object_diff.feast_object_property_diffs:
                    log_string += f"\t{_p.property_name}: {Style.BRIGHT + color}{_p.val_existing}{Style.RESET_ALL} -> {Style.BRIGHT + Fore.LIGHTGREEN_EX}{_p.val_declared}{Style.RESET_ALL}\n"

        log_string = (
            f"{Style.BRIGHT + Fore.LIGHTBLUE_EX}No changes to registry"
            if not log_string
            else log_string
        )

        return log_string


def tag_objects_for_keep_delete_update_add(
    existing_objs: Iterable[FeastObject], desired_objs: Iterable[FeastObject]
) -> Tuple[Set[FeastObject], Set[FeastObject], Set[FeastObject], Set[FeastObject]]:
    # TODO(adchia): Remove the "if X.name" condition when data sources are forced to have names
    existing_obj_names = {e.name for e in existing_objs if e.name}
    desired_objs = [obj for obj in desired_objs if obj.name]
    existing_objs = [obj for obj in existing_objs if obj.name]
    desired_obj_names = {e.name for e in desired_objs if e.name}

    objs_to_add = {e for e in desired_objs if e.name not in existing_obj_names}
    objs_to_update = {e for e in desired_objs if e.name in existing_obj_names}
    objs_to_keep = {e for e in existing_objs if e.name in desired_obj_names}
    objs_to_delete = {e for e in existing_objs if e.name not in desired_obj_names}

    return objs_to_keep, objs_to_delete, objs_to_update, objs_to_add


FeastObjectProto = TypeVar(
    "FeastObjectProto",
    DataSourceProto,
    EntityProto,
    FeatureViewProto,
    FeatureServiceProto,
    OnDemandFeatureViewProto,
    StreamFeatureViewProto,
    ValidationReferenceProto,
    SavedDatasetProto,
    PermissionProto,
)


FIELDS_TO_IGNORE = {"project"}


def diff_registry_objects(
    current: FeastObject, new: FeastObject, object_type: FeastObjectType
) -> FeastObjectDiff:
    current_proto = current.to_proto()
    new_proto = new.to_proto()
    assert current_proto.DESCRIPTOR.full_name == new_proto.DESCRIPTOR.full_name
    property_diffs = []
    transition: TransitionType = TransitionType.UNCHANGED

    current_spec: FeastObjectSpecProto
    new_spec: FeastObjectSpecProto
    if isinstance(
        current_proto, (DataSourceProto, ValidationReferenceProto)
    ) or isinstance(new_proto, (DataSourceProto, ValidationReferenceProto)):
        assert type(current_proto) == type(new_proto)
        current_spec = cast(DataSourceProto, current_proto)
        new_spec = cast(DataSourceProto, new_proto)
    else:
        current_spec = current_proto.spec
        new_spec = new_proto.spec
    if current != new:
        for _field in current_spec.DESCRIPTOR.fields:
            if _field.name in FIELDS_TO_IGNORE:
                continue
            elif getattr(current_spec, _field.name) != getattr(new_spec, _field.name):
                if _field.name == "feature_transformation":
                    current_spec = cast(OnDemandFeatureViewSpec, current_spec)
                    new_spec = cast(OnDemandFeatureViewSpec, new_spec)
                    # Check if the old proto is populated and use that if it is
                    feature_transformation_udf = (
                        current_spec.feature_transformation.user_defined_function
                    )
                    if (
                        current_spec.HasField("user_defined_function")
                        and not feature_transformation_udf
                    ):
                        deprecated_udf = current_spec.user_defined_function
                    else:
                        deprecated_udf = None
                    current_udf = (
                        deprecated_udf
                        if deprecated_udf is not None
                        else feature_transformation_udf
                    )
                    new_udf = new_spec.feature_transformation.user_defined_function
                    for _udf_field in current_udf.DESCRIPTOR.fields:
                        if _udf_field.name == "body":
                            continue
                        if getattr(current_udf, _udf_field.name) != getattr(
                            new_udf, _udf_field.name
                        ):
                            transition = TransitionType.UPDATE
                            property_diffs.append(
                                PropertyDiff(
                                    _field.name + "." + _udf_field.name,
                                    getattr(current_udf, _udf_field.name),
                                    getattr(new_udf, _udf_field.name),
                                )
                            )
                else:
                    transition = TransitionType.UPDATE
                    property_diffs.append(
                        PropertyDiff(
                            _field.name,
                            getattr(current_spec, _field.name),
                            getattr(new_spec, _field.name),
                        )
                    )
    return FeastObjectDiff(
        name=new_spec.name,
        feast_object_type=object_type,
        current_feast_object=current,
        new_feast_object=new,
        feast_object_property_diffs=property_diffs,
        transition_type=transition,
    )


def extract_objects_for_keep_delete_update_add(
    registry: BaseRegistry,
    current_project: str,
    desired_repo_contents: RepoContents,
) -> Tuple[
    Dict[FeastObjectType, Set[FeastObject]],
    Dict[FeastObjectType, Set[FeastObject]],
    Dict[FeastObjectType, Set[FeastObject]],
    Dict[FeastObjectType, Set[FeastObject]],
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

    registry_object_type_to_objects: Dict[FeastObjectType, List[Any]] = (
        FeastObjectType.get_objects_from_registry(registry, current_project)
    )
    registry_object_type_to_repo_contents: Dict[FeastObjectType, List[Any]] = (
        FeastObjectType.get_objects_from_repo_contents(desired_repo_contents)
    )

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
    registry: BaseRegistry,
    current_project: str,
    desired_repo_contents: RepoContents,
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
            diff.add_feast_object_diff(
                FeastObjectDiff(
                    name=e.name,
                    feast_object_type=object_type,
                    current_feast_object=None,
                    new_feast_object=e,
                    feast_object_property_diffs=[],
                    transition_type=TransitionType.CREATE,
                )
            )
        for e in objects_to_delete:
            diff.add_feast_object_diff(
                FeastObjectDiff(
                    name=e.name,
                    feast_object_type=object_type,
                    current_feast_object=e,
                    new_feast_object=None,
                    feast_object_property_diffs=[],
                    transition_type=TransitionType.DELETE,
                )
            )
        for e in objects_to_update:
            current_obj = [_e for _e in objects_to_keep if _e.name == e.name][0]
            diff.add_feast_object_diff(
                diff_registry_objects(current_obj, e, object_type)
            )

    return diff


def apply_diff_to_registry(
    registry: BaseRegistry,
    registry_diff: RegistryDiff,
    project: str,
    commit: bool = True,
):
    """
    Applies the given diff to the given Feast project in the registry.

    Args:
        registry: The registry to be updated.
        registry_diff: The diff to apply.
        project: Feast project to be updated.
        commit: Whether the change should be persisted immediately
    """
    for feast_object_diff in registry_diff.feast_object_diffs:
        # There is no need to delete the object on an update, since applying the new object
        # will automatically delete the existing object.
        if feast_object_diff.transition_type == TransitionType.DELETE:
            if feast_object_diff.feast_object_type == FeastObjectType.ENTITY:
                entity_obj = cast(Entity, feast_object_diff.current_feast_object)
                registry.delete_entity(entity_obj.name, project, commit=False)
            elif feast_object_diff.feast_object_type == FeastObjectType.FEATURE_SERVICE:
                feature_service_obj = cast(
                    FeatureService, feast_object_diff.current_feast_object
                )
                registry.delete_feature_service(
                    feature_service_obj.name, project, commit=False
                )
            elif feast_object_diff.feast_object_type in [
                FeastObjectType.FEATURE_VIEW,
                FeastObjectType.ON_DEMAND_FEATURE_VIEW,
                FeastObjectType.STREAM_FEATURE_VIEW,
            ]:
                feature_view_obj = cast(
                    BaseFeatureView, feast_object_diff.current_feast_object
                )
                registry.delete_feature_view(
                    feature_view_obj.name,
                    project,
                    commit=False,
                )
            elif feast_object_diff.feast_object_type == FeastObjectType.DATA_SOURCE:
                ds_obj = cast(DataSource, feast_object_diff.current_feast_object)
                registry.delete_data_source(
                    ds_obj.name,
                    project,
                    commit=False,
                )
            elif feast_object_diff.feast_object_type == FeastObjectType.PERMISSION:
                permission_obj = cast(
                    Permission, feast_object_diff.current_feast_object
                )
                registry.delete_permission(
                    permission_obj.name,
                    project,
                    commit=False,
                )

        if feast_object_diff.transition_type in [
            TransitionType.CREATE,
            TransitionType.UPDATE,
        ]:
            if feast_object_diff.feast_object_type == FeastObjectType.DATA_SOURCE:
                registry.apply_data_source(
                    cast(DataSource, feast_object_diff.new_feast_object),
                    project,
                    commit=False,
                )
            if feast_object_diff.feast_object_type == FeastObjectType.ENTITY:
                registry.apply_entity(
                    cast(Entity, feast_object_diff.new_feast_object),
                    project,
                    commit=False,
                )
            elif feast_object_diff.feast_object_type == FeastObjectType.FEATURE_SERVICE:
                registry.apply_feature_service(
                    cast(FeatureService, feast_object_diff.new_feast_object),
                    project,
                    commit=False,
                )
            elif feast_object_diff.feast_object_type in [
                FeastObjectType.FEATURE_VIEW,
                FeastObjectType.ON_DEMAND_FEATURE_VIEW,
                FeastObjectType.STREAM_FEATURE_VIEW,
            ]:
                registry.apply_feature_view(
                    cast(BaseFeatureView, feast_object_diff.new_feast_object),
                    project,
                    commit=False,
                )
            elif feast_object_diff.feast_object_type == FeastObjectType.PERMISSION:
                registry.apply_permission(
                    cast(Permission, feast_object_diff.new_feast_object),
                    project,
                    commit=False,
                )

    if commit:
        registry.commit()
