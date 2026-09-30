"""Presentation-independent summaries of repository operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import click

from feast.diff.infra_diff import InfraDiff
from feast.diff.property_diff import TransitionType
from feast.diff.registry_diff import RegistryDiff


@dataclass
class OperationReport:
    projects: list[dict[str, Any]] = field(default_factory=list)
    mutation_started: bool = False

    def record(self, project: str, registry: RegistryDiff, infra: InfraDiff) -> None:
        # Values may contain credentials or serialized code. Report field names only.
        registry_changes = [
            {
                "name": diff.name,
                "type": diff.feast_object_type.value,
                "action": diff.transition_type.name.lower(),
                "changed_fields": sorted(
                    p.property_name for p in diff.feast_object_property_diffs
                ),
            }
            for diff in registry.feast_object_diffs
            if diff.transition_type != TransitionType.UNCHANGED
        ]
        infra_changes = [
            {
                "name": diff.name,
                "type": diff.infra_object_type,
                "action": diff.transition_type.name.lower(),
                "changed_fields": sorted(
                    p.property_name for p in diff.infra_object_property_diffs
                ),
            }
            for diff in infra.infra_object_diffs
            if diff.transition_type != TransitionType.UNCHANGED
        ]
        self.projects.append(
            {
                "project": project,
                "changed": bool(registry_changes or infra_changes),
                "registry_changes": sorted(
                    registry_changes, key=lambda item: (item["type"], item["name"])
                ),
                "infrastructure_changes": sorted(
                    infra_changes, key=lambda item: (item["type"], item["name"])
                ),
            }
        )


class StructuredOperationUnsupported(Exception):
    """The provider cannot offer a reliable structured operation result."""


class RepositoryConfigurationMissing(click.ClickException):
    """Missing feature_store.yaml, with a stable type for machine consumers."""
