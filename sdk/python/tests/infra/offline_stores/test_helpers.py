import pytest

from feast.data_source import BigQuerySource, FileSource
from feast.infra.offline_stores.bigquery import BigQueryOfflineStore
from feast.infra.offline_stores.file import FileOfflineStore
from feast.infra.offline_stores.helpers import get_offline_store_from_sources


@pytest.fixture
def bigquery_source():
    return BigQuerySource(event_timestamp_column="events")


@pytest.fixture
def file_source():
    return FileSource(event_timestamp_column="events", path="tmp/")


def test_retrieve_bigquery_offline_store(bigquery_source):
    offline_store = get_offline_store_from_sources([bigquery_source, bigquery_source])
    assert isinstance(offline_store, BigQueryOfflineStore)


def test_retrieve_file_offline_store(file_source):
    offline_store = get_offline_store_from_sources([file_source, file_source])
    assert isinstance(offline_store, FileOfflineStore)


def test_raise_if_multiple_offline_stores(bigquery_source, file_source):
    with pytest.raises(NotImplementedError):
        get_offline_store_from_sources([bigquery_source, file_source])
