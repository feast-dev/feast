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
    PARQUET = "parquet"
    DELTA = "delta"
    ICEBERG = "iceberg"
    AVRO = "avro"
    JSON = "json"
    CSV = "csv"
    HUDI = "hudi"


class TableFormat(ABC):
    """
    Abstract base class for table formats.
    
    Table formats encapsulate metadata and configuration specific to different
    table storage formats like Iceberg, Delta Lake, Hudi, etc.
    """
    
    def __init__(self, format_type: TableFormatType, properties: Optional[Dict[str, str]] = None):
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


class IcebergTableFormat(TableFormat):
    """
    Iceberg table format configuration.
    
    Supports configuration for Iceberg catalogs, namespaces, and other Iceberg-specific
    properties like table location, file format, etc.
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
    def from_dict(cls, data: Dict) -> "IcebergTableFormat":
        return cls(
            catalog=data.get("catalog"),
            namespace=data.get("namespace"),
            catalog_properties=data.get("catalog_properties", {}),
            table_properties=data.get("table_properties", {}),
        )


class DeltaTableFormat(TableFormat):
    """
    Delta Lake table format configuration.
    
    Supports Delta Lake specific properties like table properties,
    checkpoint location, etc.
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
    def from_dict(cls, data: Dict) -> "DeltaTableFormat":
        return cls(
            table_properties=data.get("table_properties", {}),
            checkpoint_location=data.get("checkpoint_location"),
        )


class HudiTableFormat(TableFormat):
    """
    Apache Hudi table format configuration.
    
    Supports Hudi specific properties like table type, write operation,
    record key, etc.
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
            all_properties["hoodie.datasource.write.precombine.field"] = precombine_field
        
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
    def from_dict(cls, data: Dict) -> "HudiTableFormat":
        return cls(
            table_type=data.get("table_type"),
            record_key=data.get("record_key"),
            precombine_field=data.get("precombine_field"),
            table_properties=data.get("table_properties", {}),
        )


class SimpleTableFormat(TableFormat):
    """
    Simple table format for basic file formats like Parquet, Avro, JSON, CSV.
    
    These formats typically don't require complex metadata or catalog configurations.
    """
    
    def __init__(
        self,
        format_type: TableFormatType,
        properties: Optional[Dict[str, str]] = None,
    ):
        super().__init__(format_type, properties)
    
    def to_dict(self) -> Dict:
        return {
            "format_type": self.format_type.value,
            "properties": self.properties,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "SimpleTableFormat":
        format_type = TableFormatType(data["format_type"])
        return cls(
            format_type=format_type,
            properties=data.get("properties", {}),
        )


def create_table_format(format_type: TableFormatType, **kwargs) -> TableFormat:
    """
    Factory function to create appropriate TableFormat instance based on type.
    """
    if format_type == TableFormatType.ICEBERG:
        return IcebergTableFormat(**kwargs)
    elif format_type == TableFormatType.DELTA:
        return DeltaTableFormat(**kwargs)
    elif format_type == TableFormatType.HUDI:
        return HudiTableFormat(**kwargs)
    else:
        # For simple formats (parquet, avro, json, csv)
        return SimpleTableFormat(format_type, kwargs.get("properties"))


def table_format_from_dict(data: Dict) -> TableFormat:
    """
    Create TableFormat instance from dictionary representation.
    """
    format_type = TableFormatType(data["format_type"])
    
    if format_type == TableFormatType.ICEBERG:
        return IcebergTableFormat.from_dict(data)
    elif format_type == TableFormatType.DELTA:
        return DeltaTableFormat.from_dict(data)
    elif format_type == TableFormatType.HUDI:
        return HudiTableFormat.from_dict(data)
    else:
        return SimpleTableFormat.from_dict(data)


def table_format_from_json(json_str: str) -> TableFormat:
    """
    Create TableFormat instance from JSON string.
    """
    data = json.loads(json_str)
    return table_format_from_dict(data)