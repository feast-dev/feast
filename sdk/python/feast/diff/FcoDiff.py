from dataclasses import dataclass
from typing import Generic, Iterable, List, Set, Tuple, TypeVar

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


def diff_between(current: Fco, new: Fco, object_type: str) -> FcoDiff:
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
