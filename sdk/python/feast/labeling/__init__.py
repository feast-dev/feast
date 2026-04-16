"""Labeling primitives for mutable annotations decoupled from immutable feature data."""

from feast.labeling.conflict_policy import ConflictPolicy
from feast.labeling.label_view import LabelView

__all__ = [
    "ConflictPolicy",
    "LabelView",
]
