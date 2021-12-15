from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, List, Set, Tuple, TypeVar

from feast.base_feature_view import BaseFeatureView
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_table import FeatureTable
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto


@dataclass
class PropertyDiff:
    property_name: str
    val_existing: str
    val_declared: str


class TransitionType(Enum):
    UNKNOWN = 0
    CREATE = 1
    DELETE = 2
    UPDATE = 3
    UNCHANGED = 4


@dataclass
class FcoDiff:
    current_fco: Any
    new_fco: Any
    fco_property_diffs: List[PropertyDiff]
    transition_type: TransitionType


@dataclass
class RegistryDiff:
    fco_diffs: List[FcoDiff]

    def __init__(self):
        self.fco_diffs = []

    def add_fco_diff(self, fco_diff: FcoDiff):
        self.fco_diffs.append(fco_diff)


T = TypeVar("T", Entity, BaseFeatureView, FeatureService, FeatureTable)


def tag_objects_for_keep_delete_add(
    existing_objs: Iterable[T], desired_objs: Iterable[T]
) -> Tuple[Set[T], Set[T], Set[T]]:
    existing_obj_names = {e.name for e in existing_objs}
    desired_obj_names = {e.name for e in desired_objs}

    objs_to_add = {e for e in desired_objs if e.name not in existing_obj_names}
    objs_to_keep = {e for e in desired_objs if e.name in existing_obj_names}
    objs_to_delete = {e for e in existing_objs if e.name not in desired_obj_names}

    return objs_to_keep, objs_to_delete, objs_to_add


U = TypeVar("U", EntityProto, FeatureViewProto)


def tag_proto_objects_for_keep_delete_add(
    existing_objs: Iterable[U], desired_objs: Iterable[U]
) -> Tuple[Iterable[U], Iterable[U], Iterable[U]]:
    existing_obj_names = {e.spec.name for e in existing_objs}
    desired_obj_names = {e.spec.name for e in desired_objs}

    objs_to_add = [e for e in desired_objs if e.spec.name not in existing_obj_names]
    objs_to_keep = [e for e in desired_objs if e.spec.name in existing_obj_names]
    objs_to_delete = [e for e in existing_objs if e.spec.name not in desired_obj_names]

    return objs_to_keep, objs_to_delete, objs_to_add
