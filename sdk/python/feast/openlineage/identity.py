# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
OpenLineage identity and lineage context for Feast.

This module is the single source of truth for:
- Namespace resolution (Feast project ↔ OpenLineage namespace)
- Stable job names (materialize, spark compute, …)
- Parent-run context passed from FeatureStore to compute engines

Compute engines that emit their own OpenLineage events (e.g. Spark) should
consume :class:`LineageParentContext` rather than ad-hoc kwargs/dicts.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Union


class FeastJobKind(str, Enum):
    """Semantic role of a Feast-emitted OpenLineage job.

    ``DEFINITION`` — registry topology (apply / feature service membership).
    ``TRANSFORM`` — runtime execution (materialize, compute engines, …).
    """

    DEFINITION = "definition"
    TRANSFORM = "transform"


def resolve_namespace(
    configured_namespace: Optional[str],
    project: str,
) -> str:
    """Resolve the OpenLineage namespace for a Feast project.

    Rules (keep in sync with docs; do not duplicate elsewhere):

    - Empty / default ``\"feast\"`` → use the project name.
    - Configured namespace equals the project (or already ends with
      ``/{project}``) → use configured as-is (aligns with
      ``spark.openlineage.namespace``).
    - Otherwise → ``{configured}/{project}``.
    """
    configured = (configured_namespace or "feast").strip()
    if configured in ("", "feast"):
        return project
    if configured == project or configured.endswith(f"/{project}"):
        return configured
    return f"{configured}/{project}"


def materialize_job_name(project: str) -> str:
    """Stable OpenLineage job name for Feast materialization."""
    return f"materialize_{project}"


def spark_compute_job_name(project: str) -> str:
    """Stable OpenLineage job name for SparkApplication compute runs.

    Kubernetes SparkApplication / ConfigMap names remain unique per run
    (operational). OpenLineage job identity must be stable so each
    SparkApplication is a *run* of one job.
    """
    return f"spark_compute_{project}"


def feature_views_job_name(project: str) -> str:
    """OpenLineage job name for apply-time feature-view wiring."""
    return f"feast_feature_views_{project}"


def feature_service_job_name(feature_service_name: str) -> str:
    """OpenLineage job name for FeatureService membership wiring."""
    return f"feature_service_{feature_service_name}"


@dataclass(frozen=True)
class LineageParentContext:
    """Parent OpenLineage run that a compute-engine run should link to.

    Maps to the OpenLineage ``parentRun`` / Spark
    ``spark.openlineage.parent*`` configuration. Engines that do not emit
    OpenLineage events ignore this context.
    """

    job_namespace: str
    job_name: str
    run_id: str

    def to_spark_openlineage_conf(self) -> Dict[str, str]:
        """Spark OpenLineage agent parent / root-parent configuration."""
        return {
            "spark.openlineage.parentJobNamespace": self.job_namespace,
            "spark.openlineage.parentJobName": self.job_name,
            "spark.openlineage.parentRunId": self.run_id,
            # Materialize is the root for Feast-driven Spark jobs today.
            "spark.openlineage.rootParentJobNamespace": self.job_namespace,
            "spark.openlineage.rootParentJobName": self.job_name,
            "spark.openlineage.rootParentRunId": self.run_id,
        }

    @classmethod
    def from_mapping(
        cls, data: Optional[Mapping[str, Any]]
    ) -> Optional["LineageParentContext"]:
        """Build from a mapping (e.g. legacy kwargs) or return None."""
        if not data:
            return None
        ns = data.get("jobNamespace") or data.get("job_namespace")
        name = data.get("jobName") or data.get("job_name")
        run_id = data.get("runId") or data.get("run_id")
        if not (ns and name and run_id):
            return None
        return cls(job_namespace=str(ns), job_name=str(name), run_id=str(run_id))


def coerce_lineage_parent(
    value: Union[None, "LineageParentContext", Mapping[str, Any]],
) -> Optional[LineageParentContext]:
    """Normalize lineage parent from typed context or legacy mapping."""
    if value is None:
        return None
    if isinstance(value, LineageParentContext):
        return value
    return LineageParentContext.from_mapping(value)
