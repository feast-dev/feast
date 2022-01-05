from dataclasses import dataclass
from enum import Enum


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
