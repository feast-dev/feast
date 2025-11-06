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
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from feast.protos.feast.core.DataFormat_pb2 import TableFormat as TableFormatProto


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
        properties (Optional[Dict[str, str]]): Properties for configuring Iceberg
            catalog and table operations (e.g., warehouse location, snapshot-id,
            as-of-timestamp, file format, compression, partitioning).

    Attributes:
        catalog (str): The Iceberg catalog name.
        namespace (str): The namespace within the catalog.
        properties (Dict[str, str]): Iceberg configuration properties.

    Examples:
        Basic Iceberg configuration:

        >>> iceberg_format = IcebergFormat(
        ...     catalog="my_catalog",
        ...     namespace="my_database"
        ... )

        Advanced configuration with properties:

        >>> iceberg_format = IcebergFormat(
        ...     catalog="spark_catalog",
        ...     namespace="lakehouse",
        ...     properties={
        ...         "warehouse": "s3://my-bucket/warehouse",
        ...         "catalog-impl": "org.apache.iceberg.spark.SparkCatalog",
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
        properties: Optional[Dict[str, str]] = None,
    ):
        super().__init__(TableFormatType.ICEBERG, properties)
        self.catalog = catalog
        self.namespace = namespace

        # Add catalog and namespace to properties if provided
        if catalog:
            self.properties["iceberg.catalog"] = catalog
        if namespace:
            self.properties["iceberg.namespace"] = namespace

    def to_dict(self) -> Dict:
        return {
            "format_type": self.format_type.value,
            "catalog": self.catalog,
            "namespace": self.namespace,
            "properties": self.properties,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "IcebergFormat":
        return cls(
            catalog=data.get("catalog"),
            namespace=data.get("namespace"),
            properties=data.get("properties", {}),
        )

    def to_proto(self) -> "TableFormatProto":
        """Convert to protobuf TableFormat message"""
        from feast.protos.feast.core.DataFormat_pb2 import (
            TableFormat as TableFormatProto,
        )

        iceberg_proto = TableFormatProto.IcebergFormat(
            catalog=self.catalog or "",
            namespace=self.namespace or "",
            properties=self.properties,
        )
        return TableFormatProto(iceberg_format=iceberg_proto)

    @classmethod
    def from_proto(cls, proto: "TableFormatProto") -> "IcebergFormat":
        """Create from protobuf TableFormat message"""
        iceberg_proto = proto.iceberg_format
        return cls(
            catalog=iceberg_proto.catalog if iceberg_proto.catalog else None,
            namespace=iceberg_proto.namespace if iceberg_proto.namespace else None,
            properties=dict(iceberg_proto.properties),
        )


class DeltaFormat(TableFormat):
    """
    Delta Lake table format configuration.

    Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark
    and big data workloads. This class provides configuration for Delta-specific properties
    including table properties, checkpoint locations, and versioning options.

    Args:
        checkpoint_location (Optional[str]): Location for storing Delta transaction logs
            and checkpoints. Required for streaming operations.
        properties (Optional[Dict[str, str]]): Properties for configuring Delta table
            behavior (e.g., auto-optimize, vacuum settings, data skipping).

    Attributes:
        checkpoint_location (str): Path to checkpoint storage location.
        properties (Dict[str, str]): Delta table configuration properties.

    Examples:
        Basic Delta configuration:

        >>> delta_format = DeltaFormat()

        Configuration with table properties:

        >>> delta_format = DeltaFormat(
        ...     properties={
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
        checkpoint_location: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ):
        super().__init__(TableFormatType.DELTA, properties)
        self.checkpoint_location = checkpoint_location

        # Add checkpoint location to properties if provided
        if checkpoint_location:
            self.properties["delta.checkpointLocation"] = checkpoint_location

    def to_dict(self) -> Dict:
        return {
            "format_type": self.format_type.value,
            "checkpoint_location": self.checkpoint_location,
            "properties": self.properties,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "DeltaFormat":
        return cls(
            checkpoint_location=data.get("checkpoint_location"),
            properties=data.get("properties", {}),
        )

    def to_proto(self) -> "TableFormatProto":
        """Convert to protobuf TableFormat message"""
        from feast.protos.feast.core.DataFormat_pb2 import (
            TableFormat as TableFormatProto,
        )

        delta_proto = TableFormatProto.DeltaFormat(
            checkpoint_location=self.checkpoint_location or "",
            properties=self.properties,
        )
        return TableFormatProto(delta_format=delta_proto)

    @classmethod
    def from_proto(cls, proto: "TableFormatProto") -> "DeltaFormat":
        """Create from protobuf TableFormat message"""
        delta_proto = proto.delta_format
        return cls(
            checkpoint_location=delta_proto.checkpoint_location
            if delta_proto.checkpoint_location
            else None,
            properties=dict(delta_proto.properties),
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
        properties (Optional[Dict[str, str]]): Additional Hudi table properties for
            configuring compaction, indexing, and other Hudi features.

    Attributes:
        table_type (str): The Hudi table type (COPY_ON_WRITE or MERGE_ON_READ).
        record_key (str): The record key field(s).
        precombine_field (str): The field used for record deduplication.
        properties (Dict[str, str]): Additional Hudi configuration properties.

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
        ...     properties={
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
        properties: Optional[Dict[str, str]] = None,
    ):
        super().__init__(TableFormatType.HUDI, properties)
        self.table_type = table_type
        self.record_key = record_key
        self.precombine_field = precombine_field

        # Add Hudi-specific properties if provided
        if table_type:
            self.properties["hoodie.datasource.write.table.type"] = table_type
        if record_key:
            self.properties["hoodie.datasource.write.recordkey.field"] = record_key
        if precombine_field:
            self.properties["hoodie.datasource.write.precombine.field"] = (
                precombine_field
            )

    def to_dict(self) -> Dict:
        return {
            "format_type": self.format_type.value,
            "table_type": self.table_type,
            "record_key": self.record_key,
            "precombine_field": self.precombine_field,
            "properties": self.properties,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "HudiFormat":
        return cls(
            table_type=data.get("table_type"),
            record_key=data.get("record_key"),
            precombine_field=data.get("precombine_field"),
            properties=data.get("properties", {}),
        )

    def to_proto(self) -> "TableFormatProto":
        """Convert to protobuf TableFormat message"""
        from feast.protos.feast.core.DataFormat_pb2 import (
            TableFormat as TableFormatProto,
        )

        hudi_proto = TableFormatProto.HudiFormat(
            table_type=self.table_type or "",
            record_key=self.record_key or "",
            precombine_field=self.precombine_field or "",
            properties=self.properties,
        )
        return TableFormatProto(hudi_format=hudi_proto)

    @classmethod
    def from_proto(cls, proto: "TableFormatProto") -> "HudiFormat":
        """Create from protobuf TableFormat message"""
        hudi_proto = proto.hudi_format
        return cls(
            table_type=hudi_proto.table_type if hudi_proto.table_type else None,
            record_key=hudi_proto.record_key if hudi_proto.record_key else None,
            precombine_field=hudi_proto.precombine_field
            if hudi_proto.precombine_field
            else None,
            properties=dict(hudi_proto.properties),
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
    if "format_type" not in data:
        raise KeyError("Missing 'format_type' field in data")
    format_type = data["format_type"]

    if format_type == TableFormatType.ICEBERG.value:
        return IcebergFormat.from_dict(data)
    elif format_type == TableFormatType.DELTA.value:
        return DeltaFormat.from_dict(data)
    elif format_type == TableFormatType.HUDI.value:
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


def table_format_from_proto(proto: "TableFormatProto") -> TableFormat:
    """
    Create TableFormat instance from protobuf TableFormat message.

    Args:
        proto: TableFormat protobuf message

    Returns:
        TableFormat: An instance of the appropriate TableFormat subclass.

    Raises:
        ValueError: If the proto doesn't contain a recognized format.
    """

    which_format = proto.WhichOneof("format")

    if which_format == "iceberg_format":
        return IcebergFormat.from_proto(proto)
    elif which_format == "delta_format":
        return DeltaFormat.from_proto(proto)
    elif which_format == "hudi_format":
        return HudiFormat.from_proto(proto)
    else:
        raise ValueError(f"Unknown table format in proto: {which_format}")
