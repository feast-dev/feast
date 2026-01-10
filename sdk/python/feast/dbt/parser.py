"""
dbt manifest parser for Feast integration.

This module provides functionality to parse dbt manifest.json files and extract
model metadata for generating Feast FeatureViews.

Uses dbt-artifacts-parser to handle manifest versions v1-v12 (dbt 0.19 through 1.11).
"""

import json
from enum import property
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field


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
    Parser for dbt manifest.json files.

    Uses dbt-artifacts-parser to handle manifest versions v1-v12.
    Supports dbt versions 0.19 through 1.11.

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
        self.manifest = None
        self._raw_manifest: Optional[Dict[str, Any]] = None

    def parse(self) -> None:
        """
        Load and parse the manifest.json file.

        Raises:
            FileNotFoundError: If manifest.json doesn't exist
            ValueError: If manifest.json is invalid JSON
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
                f"Invalid JSON in manifest: {e}\n"
                f"Try: dbt clean && dbt compile"
            )

        # Try to use dbt-artifacts-parser if available
        try:
            from dbt_artifacts_parser.parser import parse_manifest

            self.manifest = parse_manifest(manifest=self._raw_manifest)
        except ImportError:
            # Fall back to raw dict parsing if dbt-artifacts-parser not installed
            self.manifest = None

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
        if self._raw_manifest is None:
            self.parse()

        models = []
        nodes = self._raw_manifest.get("nodes", {})

        for node_id, node in nodes.items():
            # Only process models (not tests, seeds, snapshots, etc.)
            if not node_id.startswith("model."):
                continue

            # Also check resource_type if available
            resource_type = node.get("resource_type", "model")
            if resource_type != "model":
                continue

            model_name = node.get("name", "")

            # Filter by model names if specified
            if model_names and model_name not in model_names:
                continue

            # Get tags from node
            node_tags = node.get("tags", []) or []

            # Filter by tag if specified
            if tag_filter and tag_filter not in node_tags:
                continue

            # Extract columns
            columns = []
            node_columns = node.get("columns", {}) or {}
            for col_name, col_data in node_columns.items():
                if isinstance(col_data, dict):
                    columns.append(
                        DbtColumn(
                            name=col_name,
                            description=col_data.get("description", "") or "",
                            data_type=col_data.get("data_type", "STRING") or "STRING",
                            tags=col_data.get("tags", []) or [],
                            meta=col_data.get("meta", {}) or {},
                        )
                    )

            # Get depends_on nodes
            depends_on = node.get("depends_on", {}) or {}
            depends_on_nodes = depends_on.get("nodes", []) or []

            # Create DbtModel
            models.append(
                DbtModel(
                    name=model_name,
                    unique_id=node_id,
                    database=node.get("database", "") or "",
                    schema=node.get("schema", "") or "",
                    alias=node.get("alias", model_name) or model_name,
                    description=node.get("description", "") or "",
                    columns=columns,
                    tags=node_tags,
                    meta=node.get("meta", {}) or {},
                    depends_on=depends_on_nodes,
                )
            )

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
        if self._raw_manifest is None:
            return None
        metadata = self._raw_manifest.get("metadata", {})
        return metadata.get("dbt_version")

    @property
    def project_name(self) -> Optional[str]:
        """Get project name from manifest metadata."""
        if self._raw_manifest is None:
            return None
        metadata = self._raw_manifest.get("metadata", {})
        return metadata.get("project_name")
