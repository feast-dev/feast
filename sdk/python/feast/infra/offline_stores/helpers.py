from typing import List

from feast.data_source import DataSource
from feast.infra.offline_stores.offline_store import OfflineStore


def get_offline_store_from_sources(sources: List[DataSource]) -> OfflineStore:
    """Detect which offline store should be used for retrieving historical features"""

    source_types = [type(source) for source in sources]

    if len(set(source_types)) > 1:
        raise NotImplementedError(
            "Unsupported combination of feature view input source types. Please ensure that all source types are "
            "consistent and available in the same offline store."
        )

    # Assert statement to pass mypy and make sure we return an OfflineStore object
    offline_store = sources.pop().offline_store
    assert isinstance(offline_store, OfflineStore)

    return offline_store
