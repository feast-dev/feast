from typing import List

from feast.data_source import BigQuerySource, DataSource, FileSource
from feast.infra.offline_stores.bigquery import BigQueryOfflineStore
from feast.infra.offline_stores.file import FileOfflineStore
from feast.infra.offline_stores.offline_store import OfflineStore


def get_offline_store_from_sources(sources: List[DataSource]) -> OfflineStore:
    """Detect which offline store should be used for retrieving historical features"""

    source_types = [type(source) for source in sources]

    # Retrieve features from ParquetOfflineStore
    if all(source == FileSource for source in source_types):
        return FileOfflineStore()

    # Retrieve features from BigQueryOfflineStore
    if all(source == BigQuerySource for source in source_types):
        return BigQueryOfflineStore()

    # Could not map inputs to an OfflineStore implementation
    raise NotImplementedError(
        "Unsupported combination of feature view input source types. Please ensure that all source types are "
        "consistent and available in the same offline store."
    )
