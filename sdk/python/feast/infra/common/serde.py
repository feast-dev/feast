from dataclasses import dataclass

import dill

from feast import FeatureView
from feast.infra.passthrough_provider import PassthroughProvider
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto


@dataclass
class SerializedArtifacts:
    """Class to assist with serializing unpicklable artifacts to be passed to the compute engine."""

    feature_view_proto: str
    repo_config_byte: str

    @classmethod
    def serialize(cls, feature_view, repo_config):
        # serialize to proto
        feature_view_proto = feature_view.to_proto().SerializeToString()

        # serialize repo_config to disk. Will be used to instantiate the online store
        repo_config_byte = dill.dumps(repo_config)

        return SerializedArtifacts(
            feature_view_proto=feature_view_proto, repo_config_byte=repo_config_byte
        )

    def unserialize(self):
        # unserialize
        proto = FeatureViewProto()
        proto.ParseFromString(self.feature_view_proto)
        feature_view = FeatureView.from_proto(proto)

        # load
        repo_config = dill.loads(self.repo_config_byte)

        provider = PassthroughProvider(repo_config)
        online_store = provider.online_store
        offline_store = provider.offline_store
        return feature_view, online_store, offline_store, repo_config
