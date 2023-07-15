from feast.protos.feast.serving.GrpcServer_pb2 import GrpcIngestFeatureResponse
from feast.protos.feast.serving.GrpcServer_pb2_grpc import GrpcIngestFeatureServiceServicer, \
    add_GrpcIngestFeatureServiceServicer_to_server
from concurrent import futures
import grpc
import pandas as pd
from feast.feature_store import FeatureStore
from feast.data_source import PushMode


class GrpcIngestFeatureService(GrpcIngestFeatureServiceServicer):
    fs: FeatureStore
    sfv: str
    to: PushMode

    def __init__(self, fs, sfv, to):
        self.fs = fs
        self.sfv = sfv
        self.to = to
        super().__init__()

    def GrpcIngestFeature(self, request, context):
        features = {}
        for i in request.test.keys():
            features[i] = [request.test.get(i)]
        rows = pd.DataFrame.from_dict(features)
        if self.to == PushMode.ONLINE or self.to == PushMode.ONLINE_AND_OFFLINE:
            self.fs.write_to_online_store(self.sfv, rows)
        if self.to == PushMode.OFFLINE or self.to == PushMode.ONLINE_AND_OFFLINE:
            self.fs.write_to_offline_store(self.sfv, rows)
        return GrpcIngestFeatureResponse(status=True)


class GrpcIngestFeatureServer:
    def __init__(self, address, fs: FeatureStore, sfv_name, to):
        self.address = address
        self.fs = fs
        self.sfv = sfv_name
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_GrpcIngestFeatureServiceServicer_to_server(GrpcIngestFeatureService(self.fs, self.sfv, to), self.server)
        self.server.add_insecure_port(self.address)

    def start(self):
        self.server.start()
        self.server.wait_for_termination()


def GetGrpcServer(fs: FeatureStore, address: str, sfv: str, to: PushMode):
    return GrpcIngestFeatureServer(address, fs, sfv, to)
