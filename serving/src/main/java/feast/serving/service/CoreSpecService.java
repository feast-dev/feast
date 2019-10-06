package feast.serving.service;

import feast.core.CoreServiceGrpc;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsResponse;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

// TODO: Health check and recovery for this CoreSpecService, i.e.
//       if client fails to connect to Feast Core GRPC service, what to do or report?
//       By default, managed channel should do auto-retry etc, but just to double check.

@Slf4j
public class CoreSpecService {
  private final CoreServiceGrpc.CoreServiceBlockingStub blockingStub;

  public CoreSpecService(String feastCoreHost, int feastCorePort) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(feastCoreHost, feastCorePort).usePlaintext().build();
    blockingStub = CoreServiceGrpc.newBlockingStub(channel);
  }

  public GetFeatureSetsResponse getFeatureSets(GetFeatureSetsRequest getFeatureSetsRequest) {
    return blockingStub.getFeatureSets(getFeatureSetsRequest);
  }

  public UpdateStoreResponse updateStore(UpdateStoreRequest updateStoreRequest) {
    return blockingStub.updateStore(updateStoreRequest);
  }
}
