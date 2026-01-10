"""
dbt manifest parser for Feast integration.

This module provides functionality to parse dbt manifest.json files and extract
model metadata for generating Feast FeatureViews.

Uses dbt-artifacts-parser for typed parsing of manifest versions v1-v12 (dbt 0.19 through 1.11+).
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class DbtColumn:
    """Represents a column in a dbt model."""

    name: str
    description: str = ""
    data_type: str = "STRING"
    tags: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DbtModel:
    """Represents a dbt model."""

    name: str
    unique_id: str
    database: str
    schema: str
    alias: str
    description: str = ""
    columns: List[DbtColumn] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)

    @property
    def full_table_name(self) -> str:
        """Returns fully qualified table name (database.schema.table)."""
        return f"{self.database}.{self.schema}.{self.alias}"


class DbtManifestParser:
    """
    Parser for dbt manifest.json files using dbt-artifacts-parser.

    Uses dbt-artifacts-parser for typed parsing of manifest versions v1-v12
    (dbt versions 0.19 through 1.11+).

    Examples:
        >>> parser = DbtManifestParser("target/manifest.json")
        >>> parser.parse()
        >>> models = parser.get_models(tag_filter="feast")
        >>> for model in models:
        ...     print(f"Model: {model.name}, Columns: {len(model.columns)}")

    Args:
        manifest_path: Path to manifest.json file (typically target/manifest.json)

    Raises:
        FileNotFoundError: If manifest.json doesn't exist
        ValueError: If manifest.json is invalid JSON
    """

    def __init__(self, manifest_path: str):
        """
        Initialize parser.

        Args:
            manifest_path: Path to manifest.json file
        """
        self.manifest_path = Path(manifest_path)
        self._raw_manifest: Optional[Dict[str, Any]] = None
        self._parsed_manifest: Optional[Any] = None

    def parse(self) -> None:
        """
        Load and parse the manifest.json file using dbt-artifacts-parser.

        Raises:
            FileNotFoundError: If manifest.json doesn't exist
            ValueError: If manifest.json is invalid JSON
            ImportError: If dbt-artifacts-parser is not installed
        """
        if not self.manifest_path.exists():
            raise FileNotFoundError(
                f"dbt manifest not found at {self.manifest_path}.\n"
                f"Run 'dbt compile' or 'dbt run' first.\n"
                f"Expected path: <dbt_project>/target/manifest.json"
            )

        try:
            with open(self.manifest_path, "r") as f:
                self._raw_manifest = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON in manifest: {e}\nTry: dbt clean && dbt compile"
            )

        # Parse using dbt-artifacts-parser for typed access
        try:
            from dbt_artifacts_parser.parser import parse_manifest

            self._parsed_manifest = parse_manifest(manifest=self._raw_manifest)
        except ImportError:
            raise ImportError(
                "dbt-artifacts-parser is required for dbt integration.\n"
                "Install with: pip install 'feast[dbt]' or pip install dbt-artifacts-parser"
            )

    def _extract_column_from_node(self, col_name: str, col_data: Any) -> DbtColumn:
        """Extract column info from a parsed node column."""
        return DbtColumn(
            name=col_name,
            description=getattr(col_data, "description", "") or "",
            data_type=getattr(col_data, "data_type", "STRING") or "STRING",
            tags=list(getattr(col_data, "tags", []) or []),
            meta=dict(getattr(col_data, "meta", {}) or {}),
        )

    def _extract_model_from_node(self, node_id: str, node: Any) -> Optional[DbtModel]:
        """Extract DbtModel from a parsed manifest node."""
        # Check resource type
        resource_type = getattr(node, "resource_type", None)
        if resource_type is None:
            if not node_id.startswith("model."):
                return None
        else:
            resource_type_str = (
                resource_type.value
                if hasattr(resource_type, "value")
                else str(resource_type)
            )
            if resource_type_str != "model":
                return None

        model_name = getattr(node, "name", "")
        node_tags = list(getattr(node, "tags", []) or [])
        node_columns = getattr(node, "columns", {}) or {}
        depends_on = getattr(node, "depends_on", None)

        if depends_on:
            depends_on_nodes = list(getattr(depends_on, "nodes", []) or [])
        else:
            depends_on_nodes = []

        # Extract columns
        columns = [
            self._extract_column_from_node(col_name, col_data)
            for col_name, col_data in node_columns.items()
        ]

        # Get schema - dbt-artifacts-parser uses schema_ to avoid Python keyword
        schema = getattr(node, "schema_", "") or getattr(node, "schema", "") or ""

        return DbtModel(
            name=model_name,
            unique_id=node_id,
            database=getattr(node, "database", "") or "",
            schema=schema,
            alias=getattr(node, "alias", model_name) or model_name,
            description=getattr(node, "description", "") or "",
            columns=columns,
            tags=node_tags,
            meta=dict(getattr(node, "meta", {}) or {}),
            depends_on=depends_on_nodes,
        )

    def get_models(
        self,
        model_names: Optional[List[str]] = None,
        tag_filter: Optional[str] = None,
    ) -> List[DbtModel]:
        """
        Extract dbt models from manifest.

        Args:
            model_names: Optional list of specific model names to extract
            tag_filter: Optional tag to filter models by

        Returns:
            List of DbtModel objects

        Examples:
            >>> models = parser.get_models(model_names=["driver_stats"])
            >>> models = parser.get_models(tag_filter="feast")
        """
        if self._parsed_manifest is None:
            self.parse()

        if self._parsed_manifest is None:
            return []

        models = []
        nodes = getattr(self._parsed_manifest, "nodes", {}) or {}

        for node_id, node in nodes.items():
            # Only process models (not tests, seeds, snapshots, etc.)
            if not node_id.startswith("model."):
                continue

            model = self._extract_model_from_node(node_id, node)
            if model is None:
                continue

            # Filter by model names if specified
            if model_names and model.name not in model_names:
                continue

            # Filter by tag if specified
            if tag_filter and tag_filter not in model.tags:
                continue

            models.append(model)

        return models

    def get_model_by_name(self, model_name: str) -> Optional[DbtModel]:
        """
        Get a specific model by name.

        Args:
            model_name: Name of the model to retrieve

        Returns:
            DbtModel if found, None otherwise
        """
        models = self.get_models(model_names=[model_name])
        return models[0] if models else None

    @property
    def dbt_version(self) -> Optional[str]:
        """Get dbt version from manifest metadata."""
        if self._parsed_manifest is None:
            return None
        metadata = getattr(self._parsed_manifest, "metadata", None)
        if metadata is None:
            return None
        return getattr(metadata, "dbt_version", None)

    @property
    def project_name(self) -> Optional[str]:
        """Get project name from manifest metadata."""
        if self._parsed_manifest is None:
            return None
        metadata = getattr(self._parsed_manifest, "metadata", None)
        if metadata is None:
            return None
        # project_name may not exist in all manifest versions
        return getattr(metadata, "project_name", None) or getattr(
            metadata, "project_id", None
        )
