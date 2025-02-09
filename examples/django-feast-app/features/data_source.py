from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto

class DjangoModelDataSource(DataSource):
    """A custom data source that uses Django models as a source for feature values."""

    def __init__(
        self,
        model,
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
    ):
        """Initialize the Django model data source.
        
        Args:
            model: Django model class to use as data source
            timestamp_field: Field containing the event timestamp
            created_timestamp_column: Field containing creation timestamp
            field_mapping: Optional mapping for field names
            name: Optional name for the data source
        """
        self._model = model
        self._timestamp_field = timestamp_field
        self._created_timestamp_column = created_timestamp_column
        self._field_mapping = field_mapping or {}
        self._name = name or model._meta.db_table

        super().__init__(
            name=self._name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
        )

    def get_table_query_string(self) -> str:
        """Get table name for the Django model."""
        return self._model._meta.db_table

    def to_proto(self) -> DataSourceProto:
        """Convert data source to protobuf representation."""
        meta = self._model._meta
        return DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self._field_mapping,
            timestamp_field=self._timestamp_field,
            table=meta.db_table,
            description=f"Django model source: {meta.app_label}.{meta.model_name}"
        )

    @staticmethod
    def from_proto(proto: DataSourceProto) -> "DjangoModelDataSource":
        """Create data source from protobuf representation."""
        raise NotImplementedError("Loading from proto not implemented yet")

    def get_table_column_names_and_types(self, config: Optional[Dict[str, str]] = None) -> List[str]:
        """Get column names and types from Django model."""
        fields = []
        for field in self._model._meta.fields:
            fields.append((field.name, field.get_internal_type()))
        return fields

    def validate(self, config: Optional[Dict[str, str]] = None):
        """Validate the data source configuration."""
        if not hasattr(self._model._meta, "db_table"):
            raise ValueError("Model must have a db_table defined")
        
        # Verify timestamp field exists
        if not any(field.name == self._timestamp_field for field in self._model._meta.fields):
            raise ValueError(f"Timestamp field {self._timestamp_field} not found in model")

    def get_dataframe(self, config: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """Get feature values as a pandas DataFrame."""
        queryset = self._model.objects.all()
        return pd.DataFrame.from_records(queryset.values())
