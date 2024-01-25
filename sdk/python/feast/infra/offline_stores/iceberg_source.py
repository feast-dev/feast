from typing import Callable, Dict, Iterable, List, Optional, Tuple

from typeguard import typechecked

from feast import type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException, DataSourceNotFoundException
from feast.feature_logging import LoggingDestination
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.FeatureService_pb2 import (
    LoggingConfig as LoggingConfigProto,
)
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import get_user_agent
from feast.value_type import ValueType

 
from typing import Optional
from feast import BigQuerySource, DataSourceProto

class IcebergSource(BigQuerySource):
    """
    IcebergSource is a variant of BigQuerySource that connects to Iceberg tables through BigQuery.
    """

    def __init__(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        date_partition_column: Optional[str] = None,
        field_mapping: Optional[dict] = None,
    ):
        super().__init__(
            table_ref=table,
            query=query,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column=date_partition_column,
            field_mapping=field_mapping,
        )

    @classmethod
    def from_proto(cls, data_source: DataSourceProto.DataSource):
        return cls(
            table=data_source.bigquery_options.table_ref,
            query=data_source.bigquery_options.query,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            field_mapping=dict(data_source.field_mapping),
        )

    def to_proto(self) -> DataSourceProto.DataSource:
        data_source_proto = DataSourceProto.DataSource(
            type=DataSourceProto.DataSource.BIGQUERY,
            bigquery_options=DataSourceProto.DataSource.BigQueryOptions(
                table_ref=self.table_ref,
                query=self.query,
            ),
            event_timestamp_column=self.event_timestamp_column,
            created_timestamp_column=self.created_timestamp_column,
            date_partition_column=self.date_partition_column,
            field_mapping=self.field_mapping,
        )

        return data_source_proto