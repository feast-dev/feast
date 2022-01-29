from dataclasses import dataclass
from typing import Generic, Iterable, List, Tuple, TypeVar

from feast.diff.property_diff import PropertyDiff, TransitionType
from feast.infra.infra_object import (
    DATASTORE_INFRA_OBJECT_CLASS_TYPE,
    DYNAMODB_INFRA_OBJECT_CLASS_TYPE,
    SQLITE_INFRA_OBJECT_CLASS_TYPE,
    InfraObject,
)
from feast.protos.feast.core.DatastoreTable_pb2 import (
    DatastoreTable as DatastoreTableProto,
)
from feast.protos.feast.core.DynamoDBTable_pb2 import (
    DynamoDBTable as DynamoDBTableProto,
)
from feast.protos.feast.core.InfraObject_pb2 import Infra as InfraProto
from feast.protos.feast.core.SqliteTable_pb2 import SqliteTable as SqliteTableProto

InfraObjectProto = TypeVar(
    "InfraObjectProto", DatastoreTableProto, DynamoDBTableProto, SqliteTableProto
)


@dataclass
class InfraObjectDiff(Generic[InfraObjectProto]):
    name: str
    infra_object_type: str
    current_infra_object: InfraObjectProto
    new_infra_object: InfraObjectProto
    infra_object_property_diffs: List[PropertyDiff]
    transition_type: TransitionType


@dataclass
class InfraDiff:
    infra_object_diffs: List[InfraObjectDiff]

    def __init__(self):
        self.infra_object_diffs = []

    def update(self):
        """Apply the infrastructure changes specified in this object."""
        for infra_object_diff in self.infra_object_diffs:
            if infra_object_diff.transition_type in [
                TransitionType.DELETE,
                TransitionType.UPDATE,
            ]:
                infra_object = InfraObject.from_proto(
                    infra_object_diff.current_infra_object
                )
                infra_object.teardown()
            elif infra_object_diff.transition_type in [
                TransitionType.CREATE,
                TransitionType.UPDATE,
            ]:
                infra_object = InfraObject.from_proto(
                    infra_object_diff.new_infra_object
                )
                infra_object.update()

    def to_string(self):
        from colorama import Fore, Style

        log_string = ""

        message_action_map = {
            TransitionType.CREATE: ("Created", Fore.GREEN),
            TransitionType.DELETE: ("Deleted", Fore.RED),
            TransitionType.UNCHANGED: ("Unchanged", Fore.LIGHTBLUE_EX),
            TransitionType.UPDATE: ("Updated", Fore.YELLOW),
        }
        for infra_object_diff in self.infra_object_diffs:
            if infra_object_diff.transition_type == TransitionType.UNCHANGED:
                continue
            action, color = message_action_map[infra_object_diff.transition_type]
            log_string += f"{action} {infra_object_diff.infra_object_type} {Style.BRIGHT + color}{infra_object_diff.name}{Style.RESET_ALL}\n"
            if infra_object_diff.transition_type == TransitionType.UPDATE:
                for _p in infra_object_diff.infra_object_property_diffs:
                    log_string += f"\t{_p.property_name}: {Style.BRIGHT + color}{_p.val_existing}{Style.RESET_ALL} -> {Style.BRIGHT + Fore.LIGHTGREEN_EX}{_p.val_declared}{Style.RESET_ALL}\n"

        log_string = (
            f"{Style.BRIGHT + Fore.LIGHTBLUE_EX}No changes to infrastructure"
            if not log_string
            else log_string
        )

        return log_string


def tag_infra_proto_objects_for_keep_delete_add(
    existing_objs: Iterable[InfraObjectProto], desired_objs: Iterable[InfraObjectProto]
) -> Tuple[
    Iterable[InfraObjectProto], Iterable[InfraObjectProto], Iterable[InfraObjectProto]
]:
    existing_obj_names = {e.name for e in existing_objs}
    desired_obj_names = {e.name for e in desired_objs}

    objs_to_add = [e for e in desired_objs if e.name not in existing_obj_names]
    objs_to_keep = [e for e in desired_objs if e.name in existing_obj_names]
    objs_to_delete = [e for e in existing_objs if e.name not in desired_obj_names]

    return objs_to_keep, objs_to_delete, objs_to_add


def diff_infra_protos(
    current_infra_proto: InfraProto, new_infra_proto: InfraProto
) -> InfraDiff:
    infra_diff = InfraDiff()

    infra_object_class_types_to_str = {
        DATASTORE_INFRA_OBJECT_CLASS_TYPE: "datastore table",
        DYNAMODB_INFRA_OBJECT_CLASS_TYPE: "dynamodb table",
        SQLITE_INFRA_OBJECT_CLASS_TYPE: "sqlite table",
    }

    for infra_object_class_type in infra_object_class_types_to_str:
        current_infra_objects = get_infra_object_protos_by_type(
            current_infra_proto, infra_object_class_type
        )
        new_infra_objects = get_infra_object_protos_by_type(
            new_infra_proto, infra_object_class_type
        )
        (
            infra_objects_to_keep,
            infra_objects_to_delete,
            infra_objects_to_add,
        ) = tag_infra_proto_objects_for_keep_delete_add(
            current_infra_objects, new_infra_objects,
        )

        for e in infra_objects_to_add:
            infra_diff.infra_object_diffs.append(
                InfraObjectDiff(
                    e.name,
                    infra_object_class_types_to_str[infra_object_class_type],
                    None,
                    e,
                    [],
                    TransitionType.CREATE,
                )
            )
        for e in infra_objects_to_delete:
            infra_diff.infra_object_diffs.append(
                InfraObjectDiff(
                    e.name,
                    infra_object_class_types_to_str[infra_object_class_type],
                    e,
                    None,
                    [],
                    TransitionType.DELETE,
                )
            )
        for e in infra_objects_to_keep:
            current_infra_object = [
                _e for _e in current_infra_objects if _e.name == e.name
            ][0]
            infra_diff.infra_object_diffs.append(
                diff_between(
                    current_infra_object,
                    e,
                    infra_object_class_types_to_str[infra_object_class_type],
                )
            )

    return infra_diff


def get_infra_object_protos_by_type(
    infra_proto: InfraProto, infra_object_class_type: str
) -> List[InfraObjectProto]:
    return [
        InfraObject.from_infra_object_proto(infra_object).to_proto()
        for infra_object in infra_proto.infra_objects
        if infra_object.infra_object_class_type == infra_object_class_type
    ]


FIELDS_TO_IGNORE = {"project"}


def diff_between(
    current: InfraObjectProto, new: InfraObjectProto, infra_object_type: str
) -> InfraObjectDiff:
    assert current.DESCRIPTOR.full_name == new.DESCRIPTOR.full_name
    property_diffs = []
    transition: TransitionType = TransitionType.UNCHANGED
    if current != new:
        for _field in current.DESCRIPTOR.fields:
            if _field.name in FIELDS_TO_IGNORE:
                continue
            if getattr(current, _field.name) != getattr(new, _field.name):
                transition = TransitionType.UPDATE
                property_diffs.append(
                    PropertyDiff(
                        _field.name,
                        getattr(current, _field.name),
                        getattr(new, _field.name),
                    )
                )
    return InfraObjectDiff(
        new.name, infra_object_type, current, new, property_diffs, transition,
    )
