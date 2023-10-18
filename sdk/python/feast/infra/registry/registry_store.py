from abc import ABC, abstractmethod

from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto


class RegistryStore(ABC):
    """
    A registry store is a storage backend for the Feast registry.
    """

    @abstractmethod
    def get_registry_proto(self) -> RegistryProto:
        """
        Retrieves the registry proto from the registry path. If there is no file at that path,
        raises a FileNotFoundError.

        Returns:
            Returns either the registry proto stored at the registry path, or an empty registry proto.
        """
        pass

    @abstractmethod
    def update_registry_proto(self, registry_proto: RegistryProto):
        """
        Overwrites the current registry proto with the proto passed in. This method
        writes to the registry path.

        Args:
            registry_proto: the new RegistryProto
        """
        pass

    @abstractmethod
    def teardown(self):
        """
        Tear down the registry.
        """
        pass


class NoopRegistryStore(RegistryStore):
    def get_registry_proto(self) -> RegistryProto:
        pass

    def update_registry_proto(self, registry_proto: RegistryProto):
        pass

    def teardown(self):
        pass
