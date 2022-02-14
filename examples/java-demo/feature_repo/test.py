import grpc
from feast.protos.feast.serving.ServingService_pb2 import (
    FeatureList,
    GetOnlineFeaturesRequest,
)
from feast.protos.feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.protos.feast.types.Value_pb2 import RepeatedValue, Value


# Sample logic to fetch from a local gRPC java server deployed at 6566
def fetch_java():
    channel = grpc.insecure_channel("localhost:6566")
    stub = ServingServiceStub(channel)
    feature_refs = FeatureList(val=["driver_hourly_stats:conv_rate"])
    entity_rows = {
        "driver_id": RepeatedValue(
            val=[Value(int64_val=driver_id) for driver_id in range(1001, 1003)]
        )
    }

    print(
        stub.GetOnlineFeatures(
            GetOnlineFeaturesRequest(features=feature_refs, entities=entity_rows,)
        )
    )

if __name__ == "__main__":
    fetch_java()
