from typing import List

from feast.infra.infra_object import Infra, InfraObject
from feast.infra.passthrough_provider import PassthroughProvider
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RepoConfig


class LocalProvider(PassthroughProvider):
    """
    This class only exists for backwards compatibility.
    """

    def plan_infra(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> Infra:
        infra = Infra()
        if self.online_store:
            infra_objects: List[InfraObject] = self.online_store.plan(
                config, desired_registry_proto
            )
            infra.infra_objects += infra_objects
        return infra
