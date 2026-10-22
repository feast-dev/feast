import json

import pytest

from feast.data_source import DataSource
from feast.saved_dataset import (
    _DATA_SOURCE_TO_SAVED_DATASET_STORAGE,
    SavedDatasetStorage,
)
from feast.table_format import LanceFormat

lance_source = pytest.importorskip(
    "feast.infra.data_sources.contrib.lance.lance_source"
)
LanceSource = lance_source.LanceSource
SavedDatasetLanceStorage = lance_source.SavedDatasetLanceStorage


def test_a_pinned_version_round_trips():
    written = SavedDatasetLanceStorage(LanceFormat(version=7), uri="s3://bucket/ds")

    read_back = SavedDatasetLanceStorage.from_proto(written.to_proto())

    assert isinstance(read_back, SavedDatasetLanceStorage)
    assert read_back.table_format.version == 7
    assert read_back.uri == "s3://bucket/ds"


def test_a_namespace_addressed_dataset_round_trips():
    written = SavedDatasetLanceStorage(
        LanceFormat(tag="candidate"),
        table="embeddings",
        namespace_impl="dir",
        namespace_properties={"root": "/warehouse"},
    )

    read_back = SavedDatasetLanceStorage.from_proto(written.to_proto())

    assert read_back.lance_table == "embeddings"
    assert read_back.namespace_impl == "dir"
    assert read_back.namespace_properties == {"root": "/warehouse"}
    assert read_back.table_format.tag == "candidate"


def test_the_written_payload_records_the_storage_class():
    """Four classes share custom_storage, so the writer must be recorded."""
    proto = SavedDatasetLanceStorage(
        LanceFormat(version=1), uri="s3://bucket/ds"
    ).to_proto()

    config = json.loads(proto.custom_storage.configuration.decode("utf8"))

    assert config["saved_dataset_storage_class"].endswith("SavedDatasetLanceStorage")


def test_an_unpinned_format_is_refused():
    """A saved dataset addressing 'latest' does not identify its own rows."""
    with pytest.raises(ValueError) as error:
        SavedDatasetLanceStorage(LanceFormat(), uri="s3://bucket/ds")

    assert "pin a version" in str(error.value)


def test_a_format_of_the_wrong_type_is_refused():
    with pytest.raises(ValueError) as error:
        SavedDatasetLanceStorage("not-a-format", uri="s3://bucket/ds")

    assert "requires a LanceFormat" in str(error.value)


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"uri": "s3://bucket/ds", "table": "embeddings"},
    ],
    ids=["neither", "both"],
)
def test_uri_and_table_are_exclusive(kwargs):
    with pytest.raises(ValueError) as error:
        SavedDatasetLanceStorage(LanceFormat(version=1), **kwargs)

    assert "either uri or table" in str(error.value)


def test_a_table_without_a_namespace_impl_is_refused():
    with pytest.raises(ValueError) as error:
        SavedDatasetLanceStorage(LanceFormat(version=1), table="embeddings")

    assert "requires namespace_impl" in str(error.value)


def test_to_data_source_keeps_the_pin():
    storage = SavedDatasetLanceStorage(LanceFormat(version=7), uri="s3://bucket/ds")

    source = storage.to_data_source()

    assert isinstance(source, LanceSource)
    assert source.uri == "s3://bucket/ds"
    assert source.table_format.version == 7


def test_from_data_source_carries_the_pin_over():
    source = LanceSource(uri="s3://bucket/ds", table_format=LanceFormat(version=7))

    storage = SavedDatasetLanceStorage.from_data_source(source)

    assert storage.table_format.version == 7
    assert storage.uri == "s3://bucket/ds"


def test_from_data_source_refuses_an_unpinned_source():
    """LanceSource defaults to an unpinned LanceFormat, never None."""
    source = LanceSource(uri="s3://bucket/ds")
    assert source.table_format is not None
    assert not source.table_format.is_pinned

    with pytest.raises(ValueError) as error:
        SavedDatasetLanceStorage.from_data_source(source)

    assert "pin a version" in str(error.value)


def test_from_data_source_refuses_a_non_lance_source():
    class NotLance(DataSource):
        pass

    with pytest.raises(ValueError) as error:
        SavedDatasetLanceStorage.from_data_source(NotLance.__new__(NotLance))

    assert "requires a LanceSource" in str(error.value)


def test_lance_source_is_registered_for_dispatch():
    """So SavedDatasetStorage.from_data_source resolves a LanceSource."""
    assert "LanceSource" in _DATA_SOURCE_TO_SAVED_DATASET_STORAGE

    source = LanceSource(uri="s3://bucket/ds", table_format=LanceFormat(version=7))
    storage = SavedDatasetStorage.from_data_source(source)

    assert isinstance(storage, SavedDatasetLanceStorage)
    assert storage.table_format.version == 7


def test_dispatch_resolves_lance_among_every_custom_storage_claimant():
    """Lance is the fourth class to claim ``custom_storage``.

    Before the writing class was recorded in the payload, dispatch depended on
    which module was imported last.
    """
    for module in (
        "feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source",
        "feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source",
        "feast.infra.offline_stores.contrib.couchbase_offline_store.couchbase_source",
    ):
        pytest.importorskip(module)

    written = SavedDatasetLanceStorage(LanceFormat(version=7), uri="s3://bucket/ds")

    read_back = SavedDatasetStorage.from_proto(written.to_proto())

    assert type(read_back) is SavedDatasetLanceStorage
    assert read_back.table_format.version == 7
