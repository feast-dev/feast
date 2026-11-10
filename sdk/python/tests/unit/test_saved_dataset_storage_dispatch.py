import json
import sys
import types

import pytest

from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.saved_dataset import (
    CUSTOM_STORAGE_CLASS_KEY,
    SavedDatasetStorage,
    _StorageRegistry,
)

SYNTHETIC_MODULE = "feast_test_synthetic_storage"

# Two storage classes that share a proto field, in a real importable module so
# that the class path recorded in a payload can actually be resolved.
SYNTHETIC_SOURCE = """
import json

from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.saved_dataset import SavedDatasetStorage


def _options(**config):
    return DataSourceProto.CustomSourceOptions(
        configuration=json.dumps(config).encode()
    )


class FirstSharedStorage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    def to_proto(self):
        return self._custom_storage_proto(_options(table="first"))

    @staticmethod
    def from_proto(storage_proto):
        return FirstSharedStorage()

    def to_data_source(self):
        raise NotImplementedError


class SecondSharedStorage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    def to_proto(self):
        return self._custom_storage_proto(_options(table="second"))

    @staticmethod
    def from_proto(storage_proto):
        return SecondSharedStorage()

    def to_data_source(self):
        raise NotImplementedError


"""


@pytest.fixture
def synthetic_storage():
    """Register synthetic storage classes, then undo the registration.

    The registry is process-global, so without the teardown these classes would
    remain claimants for every test that runs afterwards.
    """
    by_name = dict(_StorageRegistry.classes_by_proto_attr_name)
    all_by_name = {
        key: list(value)
        for key, value in _StorageRegistry.all_classes_by_proto_attr_name.items()
    }

    module = types.ModuleType(SYNTHETIC_MODULE)
    sys.modules[SYNTHETIC_MODULE] = module
    exec(compile(SYNTHETIC_SOURCE, SYNTHETIC_MODULE, "exec"), module.__dict__)

    yield module

    sys.modules.pop(SYNTHETIC_MODULE, None)
    _StorageRegistry.classes_by_proto_attr_name.clear()
    _StorageRegistry.classes_by_proto_attr_name.update(by_name)
    _StorageRegistry.all_classes_by_proto_attr_name.clear()
    _StorageRegistry.all_classes_by_proto_attr_name.update(all_by_name)


def _custom_options(**config) -> DataSourceProto.CustomSourceOptions:
    return DataSourceProto.CustomSourceOptions(
        configuration=json.dumps(config).encode()
    )


def test_a_field_with_one_claimant_is_unaffected():
    storage = SavedDatasetFileStorage(path="data/driver_stats.parquet")

    read_back = SavedDatasetStorage.from_proto(storage.to_proto())

    assert isinstance(read_back, SavedDatasetFileStorage)
    assert read_back.file_options.uri == "data/driver_stats.parquet"


def test_sharing_a_field_round_trips_to_the_class_that_wrote_it(synthetic_storage):
    first = synthetic_storage.FirstSharedStorage()
    second = synthetic_storage.SecondSharedStorage()

    # SecondSharedStorage was defined last, so the flat registry points at it.
    # Both must still read back as themselves.
    assert isinstance(
        SavedDatasetStorage.from_proto(first.to_proto()),
        synthetic_storage.FirstSharedStorage,
    )
    assert isinstance(
        SavedDatasetStorage.from_proto(second.to_proto()),
        synthetic_storage.SecondSharedStorage,
    )


def test_the_written_proto_records_the_concrete_class(synthetic_storage):
    proto = synthetic_storage.FirstSharedStorage().to_proto()

    config = json.loads(proto.custom_storage.configuration.decode("utf8"))

    assert config[CUSTOM_STORAGE_CLASS_KEY] == (
        f"{SYNTHETIC_MODULE}.FirstSharedStorage"
    )
    # The payload the class itself wrote is preserved alongside the new key.
    assert config["table"] == "first"


def test_an_untagged_payload_with_several_claimants_names_them(synthetic_storage):
    # What an older version wrote: no class recorded in the payload.
    legacy = _custom_options(table="first")
    assert CUSTOM_STORAGE_CLASS_KEY not in json.loads(
        legacy.configuration.decode("utf8")
    )

    with pytest.raises(ValueError) as error:
        SavedDatasetStorage.from_proto(SavedDatasetStorageProto(custom_storage=legacy))

    message = str(error.value)
    assert CUSTOM_STORAGE_CLASS_KEY in message
    # Every claimant is named so the reader knows which to call directly.
    assert f"{SYNTHETIC_MODULE}.FirstSharedStorage" in message
    assert f"{SYNTHETIC_MODULE}.SecondSharedStorage" in message


def test_an_unresolvable_recorded_class_does_not_raise_an_import_error(
    synthetic_storage,
):
    """A recorded class whose module is not installed must fall back.

    The fallback still cannot disambiguate, so this reports the ambiguity
    rather than leaking an import failure from the recorded path.
    """
    payload = _custom_options(
        table="t", **{CUSTOM_STORAGE_CLASS_KEY: "not_a_module.NotAClass"}
    )

    with pytest.raises(ValueError) as error:
        SavedDatasetStorage.from_proto(SavedDatasetStorageProto(custom_storage=payload))

    assert CUSTOM_STORAGE_CLASS_KEY in str(error.value)


@pytest.mark.parametrize(
    "module_path, class_name, kwargs",
    [
        (
            "feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source",
            "SavedDatasetPostgreSQLStorage",
            {"table_ref": "public.saved_dataset"},
        ),
        (
            "feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source",
            "SavedDatasetClickhouseStorage",
            {"table_ref": "public.saved_dataset"},
        ),
        (
            "feast.infra.offline_stores.contrib.couchbase_offline_store.couchbase_source",
            "SavedDatasetCouchbaseColumnarStorage",
            {
                "database_ref": "db",
                "scope_ref": "scope",
                "collection_ref": "collection",
            },
        ),
    ],
)
def test_the_real_custom_storage_classes_round_trip(module_path, class_name, kwargs):
    """The three in-tree classes that share ``custom_storage``.

    Postgres and Clickhouse serialize identical payloads, so before the writing
    class was recorded these two could only be told apart by import order.
    """
    module = pytest.importorskip(module_path)
    storage_class = getattr(module, class_name)

    written = storage_class(**kwargs)
    read_back = SavedDatasetStorage.from_proto(written.to_proto())

    assert type(read_back) is storage_class
