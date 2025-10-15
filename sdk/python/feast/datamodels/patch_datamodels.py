#!/usr/bin/env python3
"""
Script to patch auto-generated Pydantic datamodels from protobuf_to_pydantic.
This applies necessary modifications to make the models work with REST responses.
"""

import re
from pathlib import Path

# Marker for patched files
PATCH_MARKER = "# PATCHED by patch_datamodels.py"


class DatamodelPatcher:
    """Applies patches to generated Pydantic datamodels."""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def patch_all(self):
        """Apply all patches to the generated datamodels."""
        print("Starting datamodel patching...")

        # Patch individual files
        self.patch_datasource()
        self.patch_entity()
        self.patch_feature_table()
        self.patch_feature_view_projection()
        self.patch_feature_view()
        self.patch_feature()
        self.patch_on_demand_feature_view()
        self.patch_saved_dataset()
        self.patch_stream_feature_view()
        self.patch_registry_server()
        self.patch_field()

        print("Patching complete!")

    def read_file(self, filepath: Path) -> str:
        """Read file content."""
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()

    def write_file(self, filepath: Path, content: str):
        """Write content to file."""
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"✓ Patched {filepath.name}")

    def is_already_patched(self, content: str) -> bool:
        """Check if file has already been patched."""
        return PATCH_MARKER in content

    def add_patch_header(self, content: str, filename: str) -> str:
        """Add patch metadata header to file content.
        
        Tracks the number of times this file has been patched for debugging.
        Version increments each time the script processes the file.
        """
        if self.is_already_patched(content):
            # Extract current version and increment it
            version_pattern = rf"{re.escape(PATCH_MARKER)} v(\d+) - {re.escape(filename)}"
            match = re.search(version_pattern, content)
            
            if match:
                current_version = int(match.group(1))
                new_version = current_version + 1
                replacement = f"{PATCH_MARKER} v{new_version} - {filename}"
                content = re.sub(version_pattern, replacement, content)
            else:
                # Old format without version, start at v2
                old_pattern = rf"{re.escape(PATCH_MARKER)}[^\n]*\n"
                replacement = f"{PATCH_MARKER} v2 - {filename}\n"
                content = re.sub(old_pattern, replacement, content)
            
            return content
        
        # Add new header after the Pydantic Version comment (starting at v1)
        # Include a blank line after the patch marker to separate from imports
        header = f"{PATCH_MARKER} v1 - {filename}\n\n"
        
        # Find the line with "# Pydantic Version:" and add our header after it
        pattern = r"(# Pydantic Version: [^\n]+\n)"
        replacement = rf"\1{header}"
        content = re.sub(pattern, replacement, content, count=1)
        
        return content

    def patch_datasource(self):
        """Patch DataSource_p2p.py"""
        filepath = self.base_path / "feast" / "core" / "DataSource_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Change import: Enum -> ValueType
        content = re.sub(
            r"^from \.\.types\.Value_p2p import Enum$",
            "from ..types.Value_p2p import ValueType",
            content,
            flags=re.MULTILINE,
        )

        # Add field_validator import after IntEnum import (idempotent)
        if "from pydantic import field_validator" not in content:
            content = re.sub(
                r"(from enum import IntEnum\n)",
                r"\1from pydantic import field_validator\n",
                content,
            )

        # Change deprecated_schema type
        content = re.sub(
            r'deprecated_schema: "typing\.Dict\[str, Enum\]"',
            'deprecated_schema: "typing.Dict[str, ValueType.Enum]"',
            content,
        )

        # Add field validator for 'type' field (idempotent check)
        # Only add if the validator method doesn't exist
        if "def validate_type(cls, v):" not in content:
            type_field_pattern = (
                r'(    type: "DataSource\.SourceType" = Field\(default=0\))\n'
            )
            validator_code = """
    @field_validator('type', mode='before')
    @classmethod
    def validate_type(cls, v):
        if isinstance(v, str):
            # Convert string enum names to integer values
            type_mapping = {
                'INVALID': 0,
                'BATCH_FILE': 1,
                'BATCH_BIGQUERY': 2,
                'STREAM_KAFKA': 3,
                'STREAM_KINESIS': 4,
                'BATCH_REDSHIFT': 5,
                'CUSTOM_SOURCE': 6,
                'REQUEST_SOURCE': 7,
                'BATCH_SNOWFLAKE': 8,
                'PUSH_SOURCE': 9,
                'BATCH_TRINO': 10,
                'BATCH_SPARK': 11,
                'BATCH_ATHENA': 12,
            }
            return type_mapping.get(v, 0)
        return v
"""

            content = re.sub(type_field_pattern, r"\1" + "\n" + validator_code, content)

        # Change batch_source to Optional
        content = re.sub(
            r'batch_source: "DataSource" = Field\(default_factory=lambda : DataSource\(\)\)',
            'batch_source: typing.Optional["DataSource"] = Field(default=None)',
            content,
        )

        self.write_file(filepath, content)

    def patch_entity(self):
        """Patch Entity_p2p.py"""
        filepath = self.base_path / "feast" / "core" / "Entity_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Change import: Enum -> ValueType
        content = re.sub(
            r"^from \.\.types\.Value_p2p import Enum$",
            "from ..types.Value_p2p import ValueType",
            content,
            flags=re.MULTILINE,
        )

        # Change value_type field
        content = re.sub(
            r"value_type: Enum = Field\(default=0\)",
            "value_type: ValueType.Enum = Field(default=0)",
            content,
        )

        self.write_file(filepath, content)
    
    def patch_field(self):
        """Patch Field_p2p.py"""
        filepath = self.base_path / "feast" / "types" / "Field_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Change import: Enum -> ValueType
        content = re.sub(
            r"^from \.Value_p2p import Enum$",
            "from .Value_p2p import ValueType",
            content,
            flags=re.MULTILINE,
        )

        # Change value_type field
        content = re.sub(
            r": Enum =",
            ": ValueType.Enum =",
            content,
        )

        self.write_file(filepath, content)


    def patch_feature_table(self):
        """Patch FeatureTable_p2p.py"""
        filepath = self.base_path / "feast" / "core" / "FeatureTable_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Change batch_source to Optional
        content = re.sub(
            r"batch_source: DataSource = Field\(default_factory=DataSource\)",
            "batch_source: typing.Optional[DataSource] = Field(default=None)",
            content,
        )

        # Change stream_source to Optional
        content = re.sub(
            r"stream_source: DataSource = Field\(default_factory=DataSource\)",
            "stream_source: typing.Optional[DataSource] = Field(default=None)",
            content,
        )

        self.write_file(filepath, content)

    def patch_feature_view_projection(self):
        """Patch FeatureViewProjection_p2p.py"""
        filepath = self.base_path / "feast" / "core" / "FeatureViewProjection_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Change batch_source to Optional
        content = re.sub(
            r"batch_source: DataSource = Field\(default_factory=DataSource\)",
            "batch_source: typing.Optional[DataSource] = Field(default=None)",
            content,
        )

        # Change stream_source to Optional
        content = re.sub(
            r"stream_source: DataSource = Field\(default_factory=DataSource\)",
            "stream_source: typing.Optional[DataSource] = Field(default=None)",
            content,
        )

        self.write_file(filepath, content)

    def patch_feature_view(self):
        """Patch FeatureView_p2p.py"""
        filepath = self.base_path / "feast" / "core" / "FeatureView_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Change batch_source to Optional
        content = re.sub(
            r"batch_source: DataSource = Field\(default_factory=DataSource\)",
            "batch_source: typing.Optional[DataSource] = Field(default=None)",
            content,
        )

        # Change stream_source to Optional
        content = re.sub(
            r"stream_source: DataSource = Field\(default_factory=DataSource\)",
            "stream_source: typing.Optional[DataSource] = Field(default=None)",
            content,
        )

        self.write_file(filepath, content)

    def patch_feature(self):
        """Patch Feature_p2p.py"""
        filepath = self.base_path / "feast" / "core" / "Feature_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Change import: Enum -> ValueType
        content = re.sub(
            r"^from \.\.types\.Value_p2p import Enum$",
            "from ..types.Value_p2p import ValueType",
            content,
            flags=re.MULTILINE,
        )

        # Change value_type field
        content = re.sub(
            r"value_type: Enum = Field\(default=0\)",
            "value_type: ValueType.Enum = Field(default=0)",
            content,
        )

        self.write_file(filepath, content)

    def patch_on_demand_feature_view(self):
        """Patch OnDemandFeatureView_p2p.py"""
        filepath = self.base_path / "feast" / "core" / "OnDemandFeatureView_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Remove Aggregation import
        content = re.sub(
            r"^from \.Aggregation_p2p import Aggregation\n",
            "",
            content,
            flags=re.MULTILINE,
        )

        # Remove aggregations field and its comment
        content = re.sub(
            r"# Aggregation definitions\n    aggregations: typing\.List\[Aggregation\] = Field\(default_factory=list\)\n",
            "",
            content,
        )

        self.write_file(filepath, content)

    def patch_saved_dataset(self):
        """Patch SavedDataset_p2p.py"""
        filepath = self.base_path / "feast" / "core" / "SavedDataset_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Replace specific option imports with DataSource import
        old_imports = [
            r"^from \.DataSource_p2p import AthenaOptions\n",
            r"^from \.DataSource_p2p import BigQueryOptions\n",
            r"^from \.DataSource_p2p import CustomSourceOptions\n",
            r"^from \.DataSource_p2p import FileOptions\n",
            r"^from \.DataSource_p2p import RedshiftOptions\n",
            r"^from \.DataSource_p2p import SnowflakeOptions\n",
            r"^from \.DataSource_p2p import SparkOptions\n",
            r"^from \.DataSource_p2p import TrinoOptions\n",
        ]

        # Remove old imports
        for pattern in old_imports:
            content = re.sub(pattern, "", content, flags=re.MULTILINE)

        # Add DataSource import at the right place (idempotent)
        if "from .DataSource_p2p import DataSource" not in content:
            # Find the last DataSource_p2p import line and add our import there
            content = re.sub(
                r"(# Pydantic Version: [^\n]+\n)",
                r"\1from .DataSource_p2p import DataSource\n",
                content,
            )

        # Change storage option fields to use DataSource.XXXOptions and Optional
        storage_options = [
            ("file_storage", "FileOptions"),
            ("bigquery_storage", "BigQueryOptions"),
            ("redshift_storage", "RedshiftOptions"),
            ("snowflake_storage", "SnowflakeOptions"),
            ("trino_storage", "TrinoOptions"),
            ("spark_storage", "SparkOptions"),
            ("custom_storage", "CustomSourceOptions"),
            ("athena_storage", "AthenaOptions"),
        ]

        for field_name, option_type in storage_options:
            content = re.sub(
                rf"{field_name}: {option_type} = Field\(default_factory={option_type}\)",
                f"{field_name}: typing.Optional[DataSource.{option_type}] = Field(default=None)",
                content,
            )

        # Change join_keys to joinKeys
        content = re.sub(
            r"join_keys: typing\.List\[str\] = Field\(default_factory=list\)",
            "joinKeys: typing.List[str] = Field(default_factory=list)",
            content,
        )

        # Change storage field to Optional
        content = re.sub(
            r"storage: SavedDatasetStorage = Field\(default_factory=SavedDatasetStorage\)",
            "storage: typing.Optional[SavedDatasetStorage] = Field(default=None)",
            content,
        )

        self.write_file(filepath, content)

    def patch_stream_feature_view(self):
        """Patch StreamFeatureView_p2p.py"""
        filepath = self.base_path / "feast" / "core" / "StreamFeatureView_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Change batch_source to Optional
        content = re.sub(
            r"batch_source: DataSource = Field\(default_factory=DataSource\)",
            "batch_source: typing.Optional[DataSource] = Field(default=None)",
            content,
        )

        # Change stream_source to Optional
        content = re.sub(
            r"stream_source: DataSource = Field\(default_factory=DataSource\)",
            "stream_source: typing.Optional[DataSource] = Field(default=None)",
            content,
        )

        self.write_file(filepath, content)

    def patch_registry_server(self):
        """Patch RegistryServer_p2p.py"""
        filepath = self.base_path / "feast" / "registry" / "RegistryServer_p2p.py"
        if not filepath.exists():
            print(f"⚠ Skipping {filepath.name} (not found)")
            return

        content = self.read_file(filepath)
        
        # Add patch header
        content = self.add_patch_header(content, filepath.name)

        # Add aliases to list response fields
        aliases = [
            ("data_sources", "dataSources"),
            ("feature_views", "featureViews"),
            ("feature_services", "featureServices"),
            ("saved_datasets", "savedDatasets"),
        ]

        for field_name, alias in aliases:
            # Pattern to match field without alias (already has alias check built-in)
            pattern = rf"({field_name}: typing\.List\[[^\]]+\] = Field\(default_factory=list)\)"
            replacement = rf'\1, alias="{alias}")'
            content = re.sub(pattern, replacement, content)

        self.write_file(filepath, content)


def main():
    """Main entry point."""
    import sys

    # Default path - adjust as needed
    if len(sys.argv) > 1:
        base_path = sys.argv[1]
    else:
        # Assume script is in feast/sdk/python/feast/datamodels/
        base_path = Path(__file__).parent

    patcher = DatamodelPatcher(base_path)
    patcher.patch_all()


if __name__ == "__main__":
    main()
