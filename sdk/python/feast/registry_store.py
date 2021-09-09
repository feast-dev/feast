from abc import ABC, abstractmethod

from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto


class RegistryStore(ABC):
    """
    RegistryStore: abstract base class implemented by specific backends (local file system, GCS)
    containing lower level methods used by the Registry class that are backend-specific.
    """

    @abstractmethod
    def get_registry_proto(self):
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
        Tear down all resources.
        """
        pass
