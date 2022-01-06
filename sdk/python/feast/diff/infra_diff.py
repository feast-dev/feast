from dataclasses import dataclass
from typing import Any, List

from feast.diff.property_diff import PropertyDiff, TransitionType


@dataclass
class InfraObjectDiff:
    name: str
    infra_object_type: str
    current_fco: Any
    new_fco: Any
    fco_property_diffs: List[PropertyDiff]
    transition_type: TransitionType


@dataclass
class InfraDiff:
    infra_object_diffs: List[InfraObjectDiff]

    def __init__(self):
        self.infra_object_diffs = []

    def update(self):
        pass

    def to_string(self):
        pass
