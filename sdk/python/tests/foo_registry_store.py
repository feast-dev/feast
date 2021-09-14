from pathlib import Path

from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.registry_store import RegistryStore
from feast.repo_config import RegistryConfig


class FooRegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path) -> None:
        super().__init__()
        self.registry_proto = RegistryProto()

    def get_registry_proto(self):
        return self.registry_proto

    def update_registry_proto(self, registry_proto: RegistryProto):
        self.registry_proto = registry_proto

    def teardown(self):
        pass
