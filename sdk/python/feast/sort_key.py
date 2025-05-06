import warnings
from typing import Dict, Optional, Union

from typeguard import typechecked

from feast.protos.feast.core.SortedFeatureView_pb2 import (
    SortKey as SortKeyProto,
)
from feast.protos.feast.core.SortedFeatureView_pb2 import (
    SortOrder,
)
from feast.value_type import ValueType

warnings.simplefilter("ignore", DeprecationWarning)


@typechecked
class SortKey:
    """
    A helper class representing a sorting key for a SortedFeatureView.
    """

    name: str
    value_type: ValueType
    default_sort_order: SortOrder.Enum.ValueType
    tags: Dict[str, str]
    description: str

    def __init__(
        self,
        name: str,
        value_type: ValueType,
        default_sort_order: Union[str, SortOrder.Enum.ValueType] = SortOrder.ASC,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
    ):
        if isinstance(default_sort_order, str):
            try:
                default_sort_order = SortOrder.Enum.Value(default_sort_order.upper())
            except ValueError:
                raise ValueError("default_sort_order must be 'ASC' or 'DESC'")
        if default_sort_order not in (SortOrder.ASC, SortOrder.DESC):
            raise ValueError(
                "default_sort_order must be SortOrder.ASC or SortOrder.DESC"
            )
        self.name = name
        # TODO: Handle ValueType conversion, user should be able to pass in a dtype instead of ValueType
        self.value_type = value_type
        self.default_sort_order = default_sort_order
        self.tags = tags or {}
        self.description = description

    def __eq__(self, other):
        if not isinstance(other, SortKey):
            raise TypeError("Comparisons should only involve SortKey class objects.")
        return (
            self.name == other.name
            and self.value_type == other.value_type
            and self.default_sort_order == other.default_sort_order
            and self.tags == other.tags
            and self.description == other.description
        )

    def ensure_valid(self):
        """
        Validates that the SortKey has the required fields.
        """
        if not self.name:
            raise ValueError("SortKey must have a non-empty name.")
        if not isinstance(self.value_type, ValueType):
            raise ValueError("SortKey must have a valid value_type of type ValueType.")
        if self.default_sort_order not in (SortOrder.ASC, SortOrder.DESC):
            raise ValueError(
                "SortKey default_sort_order must be either SortOrder.ASC or SortOrder.DESC."
            )

    def to_proto(self) -> SortKeyProto:
        proto = SortKeyProto(
            name=self.name,
            value_type=self.value_type.value,
            default_sort_order=self.default_sort_order,
            description=self.description,
            tags=self.tags,
        )
        return proto

    @classmethod
    def from_proto(cls, proto: SortKeyProto) -> "SortKey":
        return cls(
            name=proto.name,
            value_type=ValueType(proto.value_type),
            default_sort_order=proto.default_sort_order,
            tags=dict(proto.tags),
            description=proto.description,
        )
