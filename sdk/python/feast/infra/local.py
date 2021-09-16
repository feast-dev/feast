import uuid
from datetime import datetime
from pathlib import Path
from typing import Union

import pytz

from feast import FeatureTable
from feast.feature_view import FeatureView
from feast.infra.passthrough_provider import PassthroughProvider
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.registry_store import RegistryStore
from feast.repo_config import RegistryConfig


class LocalProvider(PassthroughProvider):
    """
    This class only exists for backwards compatibility.
    """

    pass


def _table_id(project: str, table: Union[FeatureTable, FeatureView]) -> str:
    return f"{project}_{table.name}"


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)


class LocalRegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        registry_path = Path(registry_config.path)
        if registry_path.is_absolute():
            self._filepath = registry_path
        else:
            self._filepath = repo_path.joinpath(registry_path)

    def get_registry_proto(self):
        registry_proto = RegistryProto()
        if self._filepath.exists():
            registry_proto.ParseFromString(self._filepath.read_bytes())
            return registry_proto
        raise FileNotFoundError(
            f'Registry not found at path "{self._filepath}". Have you run "feast apply"?'
        )

    def update_registry_proto(self, registry_proto: RegistryProto):
        self._write_registry(registry_proto)

    def teardown(self):
        try:
            self._filepath.unlink()
        except FileNotFoundError:
            # If the file deletion fails with FileNotFoundError, the file has already
            # been deleted.
            pass

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.utcnow())
        file_dir = self._filepath.parent
        file_dir.mkdir(exist_ok=True)
        self._filepath.write_bytes(registry_proto.SerializeToString())
