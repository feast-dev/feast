from feast.infra.offline_stores.bigquery import BigQuerySource
from feast.infra.offline_stores.file_source import FileSource
from tests.integration.feature_repos.repo_configuration import REDIS_CONFIG
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)


class HybridDataSourceCreator(DataSourceCreator):
    """Creates data sources for hybrid offline store tests."""

    def create_data_source(
        self,
        dataset_name,
        timestamp_field="ts",
        created_timestamp_field=None,
        field_mapping=None,
        date_partition_column=None,
    ):
        """
        Create a hybrid data source for testing that uses BigQuery and File sources.

        The source type will be chosen based on the dataset name for demonstration purposes.
        In practice, the HybridOfflineStore will route to the appropriate offline store
        based on the data source type.
        """
        if "bigquery" in dataset_name:
            return BigQuerySource(
                table=f"{dataset_name}",
                timestamp_field=timestamp_field,
                created_timestamp_column=created_timestamp_field,
                field_mapping=field_mapping or {},
                date_partition_column=date_partition_column,
            )
        else:
            return FileSource(
                path=f"{dataset_name}.parquet",
                timestamp_field=timestamp_field,
                created_timestamp_column=created_timestamp_field,
                field_mapping=field_mapping or {},
                date_partition_column=date_partition_column,
            )


# Define available offline stores for testing
AVAILABLE_OFFLINE_STORES = [
    ("local", HybridDataSourceCreator),
]

# Define available online stores to pair with hybrid offline store
AVAILABLE_ONLINE_STORES = {"redis": (REDIS_CONFIG, RedisOnlineStoreCreator)}
