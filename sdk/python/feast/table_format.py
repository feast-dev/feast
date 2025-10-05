# Copyright 2020 The Feast Authors
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

import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Optional


class TableFormatType(Enum):
    """Enum for supported table formats"""

    DELTA = "delta"
    ICEBERG = "iceberg"
    HUDI = "hudi"


class TableFormat(ABC):
    """
    Abstract base class for table formats.

    Table formats encapsulate metadata and configuration specific to different
    table storage formats like Iceberg, Delta Lake, Hudi, etc. They provide
    a unified interface for configuring table-specific properties that are
    used when reading from or writing to these advanced table formats.

    This base class defines the contract that all table format implementations
    must follow, including serialization/deserialization capabilities and
    property management.

    Attributes:
        format_type (TableFormatType): The type of table format (iceberg, delta, hudi).
        properties (Dict[str, str]): Dictionary of format-specific properties.

    Examples:
        Table formats are typically used with data sources to specify
        advanced table metadata and reading options:

        >>> from feast.table_format import IcebergFormat
        >>> iceberg_format = IcebergFormat(
        ...     catalog="my_catalog",
        ...     namespace="my_namespace"
        ... )
        >>> iceberg_format.set_property("snapshot-id", "123456789")
    """

    def __init__(
        self, format_type: TableFormatType, properties: Optional[Dict[str, str]] = None
    ):
        self.format_type = format_type
        self.properties = properties or {}

    @abstractmethod
    def to_dict(self) -> Dict:
        """Convert table format to dictionary representation"""
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict) -> "TableFormat":
        """Create table format from dictionary representation"""
        pass

    def get_property(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get a table format property"""
        return self.properties.get(key, default)

    def set_property(self, key: str, value: str) -> None:
        """Set a table format property"""
        self.properties[key] = value


class IcebergFormat(TableFormat):
    """
    Apache Iceberg table format configuration.

    Iceberg is an open table format for huge analytic datasets. This class provides
    configuration for Iceberg-specific properties including catalog configuration,
    namespace settings, and table-level properties for reading and writing Iceberg tables.

    Args:
        catalog (Optional[str]): Name of the Iceberg catalog to use. The catalog manages
            table metadata and provides access to tables.
        namespace (Optional[str]): Namespace (schema/database) within the catalog where
            the table is located.
        catalog_properties (Optional[Dict[str, str]]): Properties for configuring the
            Iceberg catalog (e.g., warehouse location, catalog implementation).
        table_properties (Optional[Dict[str, str]]): Table-level properties for Iceberg
            operations (e.g., file format, compression, partitioning).

    Attributes:
        catalog (str): The Iceberg catalog name.
        namespace (str): The namespace within the catalog.
        catalog_properties (Dict[str, str]): Catalog configuration properties.
        table_properties (Dict[str, str]): Table-level properties.

    Examples:
        Basic Iceberg configuration:

        >>> iceberg_format = IcebergFormat(
        ...     catalog="my_catalog",
        ...     namespace="my_database"
        ... )

        Advanced configuration with catalog and table properties:

        >>> iceberg_format = IcebergFormat(
        ...     catalog="spark_catalog",
        ...     namespace="lakehouse",
        ...     catalog_properties={
        ...         "warehouse": "s3://my-bucket/warehouse",
        ...         "catalog-impl": "org.apache.iceberg.spark.SparkCatalog"
        ...     },
        ...     table_properties={
        ...         "format-version": "2",
        ...         "write.parquet.compression-codec": "snappy"
        ...     }
        ... )

        Reading from a specific snapshot:

        >>> iceberg_format = IcebergFormat(catalog="my_catalog", namespace="db")
        >>> iceberg_format.set_property("snapshot-id", "123456789")

        Time travel queries:

        >>> iceberg_format.set_property("as-of-timestamp", "1648684800000")
    """

    def __init__(
        self,
        catalog: Optional[str] = None,
        namespace: Optional[str] = None,
        catalog_properties: Optional[Dict[str, str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
    ):
        super().__init__(TableFormatType.ICEBERG)
        self.catalog = catalog
        self.namespace = namespace
        self.catalog_properties = catalog_properties or {}
        self.table_properties = table_properties or {}

        # Merge all properties
        all_properties = {}
        all_properties.update(self.catalog_properties)
        all_properties.update(self.table_properties)
        if catalog:
            all_properties["iceberg.catalog"] = catalog
        if namespace:
            all_properties["iceberg.namespace"] = namespace

        self.properties = all_properties

    def to_dict(self) -> Dict:
        return {
            "format_type": self.format_type.value,
            "catalog": self.catalog,
            "namespace": self.namespace,
            "catalog_properties": self.catalog_properties,
            "table_properties": self.table_properties,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "IcebergFormat":
        return cls(
            catalog=data.get("catalog"),
            namespace=data.get("namespace"),
            catalog_properties=data.get("catalog_properties", {}),
            table_properties=data.get("table_properties", {}),
        )


class DeltaFormat(TableFormat):
    """
    Delta Lake table format configuration.

    Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark
    and big data workloads. This class provides configuration for Delta-specific properties
    including table properties, checkpoint locations, and versioning options.

    Args:
        table_properties (Optional[Dict[str, str]]): Properties for configuring Delta table
            behavior (e.g., auto-optimize, vacuum settings, data skipping).
        checkpoint_location (Optional[str]): Location for storing Delta transaction logs
            and checkpoints. Required for streaming operations.

    Attributes:
        table_properties (Dict[str, str]): Delta table configuration properties.
        checkpoint_location (str): Path to checkpoint storage location.

    Examples:
        Basic Delta configuration:

        >>> delta_format = DeltaFormat()

        Configuration with table properties:

        >>> delta_format = DeltaFormat(
        ...     table_properties={
        ...         "delta.autoOptimize.optimizeWrite": "true",
        ...         "delta.autoOptimize.autoCompact": "true",
        ...         "delta.tuneFileSizesForRewrites": "true"
        ...     }
        ... )

        Streaming configuration with checkpoint:

        >>> delta_format = DeltaFormat(
        ...     checkpoint_location="s3://my-bucket/checkpoints/my_table"
        ... )

        Time travel - reading specific version:

        >>> delta_format = DeltaFormat()
        >>> delta_format.set_property("versionAsOf", "5")

        Time travel - reading at specific timestamp:

        >>> delta_format.set_property("timestampAsOf", "2023-01-01 00:00:00")
    """

    def __init__(
        self,
        table_properties: Optional[Dict[str, str]] = None,
        checkpoint_location: Optional[str] = None,
    ):
        super().__init__(TableFormatType.DELTA)
        self.table_properties = table_properties or {}
        self.checkpoint_location = checkpoint_location

        # Set up properties
        all_properties = self.table_properties.copy()
        if checkpoint_location:
            all_properties["delta.checkpointLocation"] = checkpoint_location

        self.properties = all_properties

    def to_dict(self) -> Dict:
        return {
            "format_type": self.format_type.value,
            "table_properties": self.table_properties,
            "checkpoint_location": self.checkpoint_location,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "DeltaFormat":
        return cls(
            table_properties=data.get("table_properties", {}),
            checkpoint_location=data.get("checkpoint_location"),
        )


class HudiFormat(TableFormat):
    """
    Apache Hudi table format configuration.

    Apache Hudi is a data management framework used to simplify incremental data processing
    and data pipeline development. This class provides configuration for Hudi-specific
    properties including table type, record keys, and write operations.

    Args:
        table_type (Optional[str]): Type of Hudi table. Options are:
            - "COPY_ON_WRITE": Stores data in columnar format (Parquet) and rewrites entire files
            - "MERGE_ON_READ": Stores data using combination of columnar and row-based formats
        record_key (Optional[str]): Field(s) that uniquely identify a record. Can be a single
            field or comma-separated list for composite keys.
        precombine_field (Optional[str]): Field used to determine the latest version of a record
            when multiple updates exist (usually a timestamp or version field).
        table_properties (Optional[Dict[str, str]]): Additional Hudi table properties for
            configuring compaction, indexing, and other Hudi features.

    Attributes:
        table_type (str): The Hudi table type (COPY_ON_WRITE or MERGE_ON_READ).
        record_key (str): The record key field(s).
        precombine_field (str): The field used for record deduplication.
        table_properties (Dict[str, str]): Additional Hudi configuration properties.

    Examples:
        Basic Hudi configuration:

        >>> hudi_format = HudiFormat(
        ...     table_type="COPY_ON_WRITE",
        ...     record_key="user_id",
        ...     precombine_field="timestamp"
        ... )

        Configuration with composite record key:

        >>> hudi_format = HudiFormat(
        ...     table_type="MERGE_ON_READ",
        ...     record_key="user_id,event_type",
        ...     precombine_field="event_timestamp"
        ... )

        Advanced configuration with table properties:

        >>> hudi_format = HudiFormat(
        ...     table_type="COPY_ON_WRITE",
        ...     record_key="id",
        ...     precombine_field="updated_at",
        ...     table_properties={
        ...         "hoodie.compaction.strategy": "org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy",
        ...         "hoodie.index.type": "BLOOM",
        ...         "hoodie.bloom.index.parallelism": "100"
        ...     }
        ... )

        Reading incremental data:

        >>> hudi_format = HudiFormat(table_type="COPY_ON_WRITE")
        >>> hudi_format.set_property("hoodie.datasource.query.type", "incremental")
        >>> hudi_format.set_property("hoodie.datasource.read.begin.instanttime", "20230101000000")
    """

    def __init__(
        self,
        table_type: Optional[str] = None,  # COPY_ON_WRITE or MERGE_ON_READ
        record_key: Optional[str] = None,
        precombine_field: Optional[str] = None,
        table_properties: Optional[Dict[str, str]] = None,
    ):
        super().__init__(TableFormatType.HUDI)
        self.table_type = table_type
        self.record_key = record_key
        self.precombine_field = precombine_field
        self.table_properties = table_properties or {}

        # Set up properties
        all_properties = self.table_properties.copy()
        if table_type:
            all_properties["hoodie.datasource.write.table.type"] = table_type
        if record_key:
            all_properties["hoodie.datasource.write.recordkey.field"] = record_key
        if precombine_field:
            all_properties["hoodie.datasource.write.precombine.field"] = (
                precombine_field
            )

        self.properties = all_properties

    def to_dict(self) -> Dict:
        return {
            "format_type": self.format_type.value,
            "table_type": self.table_type,
            "record_key": self.record_key,
            "precombine_field": self.precombine_field,
            "table_properties": self.table_properties,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "HudiFormat":
        return cls(
            table_type=data.get("table_type"),
            record_key=data.get("record_key"),
            precombine_field=data.get("precombine_field"),
            table_properties=data.get("table_properties", {}),
        )


def create_table_format(format_type: TableFormatType, **kwargs) -> TableFormat:
    """
    Factory function to create appropriate TableFormat instance based on type.

    This is a convenience function that creates the correct TableFormat subclass
    based on the provided format type, passing through any additional keyword arguments
    to the constructor.

    Args:
        format_type (TableFormatType): The type of table format to create.
        **kwargs: Additional keyword arguments passed to the format constructor.

    Returns:
        TableFormat: An instance of the appropriate TableFormat subclass.

    Raises:
        ValueError: If an unsupported format_type is provided.

    Examples:
        Create an Iceberg format:

        >>> iceberg_format = create_table_format(
        ...     TableFormatType.ICEBERG,
        ...     catalog="my_catalog",
        ...     namespace="my_db"
        ... )

        Create a Delta format:

        >>> delta_format = create_table_format(
        ...     TableFormatType.DELTA,
        ...     checkpoint_location="s3://bucket/checkpoints"
        ... )
    """
    if format_type == TableFormatType.ICEBERG:
        return IcebergFormat(**kwargs)
    elif format_type == TableFormatType.DELTA:
        return DeltaFormat(**kwargs)
    elif format_type == TableFormatType.HUDI:
        return HudiFormat(**kwargs)
    else:
        raise ValueError(f"Unknown table format type: {format_type}")


def table_format_from_dict(data: Dict) -> TableFormat:
    """
    Create TableFormat instance from dictionary representation.

    This function deserializes a dictionary (typically from JSON or protobuf)
    back into the appropriate TableFormat instance. The dictionary must contain
    a 'format_type' field that indicates which format class to instantiate.

    Args:
        data (Dict): Dictionary containing table format configuration. Must include
            'format_type' field with value 'iceberg', 'delta', or 'hudi'.

    Returns:
        TableFormat: An instance of the appropriate TableFormat subclass.

    Raises:
        ValueError: If format_type is not recognized.
        KeyError: If format_type field is missing from data.

    Examples:
        Deserialize an Iceberg format:

        >>> data = {
        ...     "format_type": "iceberg",
        ...     "catalog": "my_catalog",
        ...     "namespace": "my_db"
        ... }
        >>> iceberg_format = table_format_from_dict(data)
    """
    format_type = TableFormatType(data["format_type"])

    if format_type == TableFormatType.ICEBERG:
        return IcebergFormat.from_dict(data)
    elif format_type == TableFormatType.DELTA:
        return DeltaFormat.from_dict(data)
    elif format_type == TableFormatType.HUDI:
        return HudiFormat.from_dict(data)
    else:
        raise ValueError(f"Unknown table format type: {format_type}")


def table_format_from_json(json_str: str) -> TableFormat:
    """
    Create TableFormat instance from JSON string.

    This is a convenience function that parses a JSON string and creates
    the appropriate TableFormat instance. Useful for loading table format
    configurations from files or network requests.

    Args:
        json_str (str): JSON string containing table format configuration.

    Returns:
        TableFormat: An instance of the appropriate TableFormat subclass.

    Raises:
        json.JSONDecodeError: If the JSON string is invalid.
        ValueError: If format_type is not recognized.
        KeyError: If format_type field is missing.

    Examples:
        Load from JSON string:

        >>> json_config = '{"format_type": "delta", "checkpoint_location": "s3://bucket/checkpoints"}'
        >>> delta_format = table_format_from_json(json_config)
    """
    data = json.loads(json_str)
    return table_format_from_dict(data)
