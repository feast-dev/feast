package feast.serving.service;

import feast.core.CoreServiceGrpc;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsResponse;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

// TODO: Health check and recovery for this CoreSpecService, i.e.
//       if client fails to connect to Feast Core GRPC service, what to do or report?
//       By default, managed channel should do auto-retry etc, but just to double check.

@Slf4j
public class CoreSpecService implements SpecService {
  private final CoreServiceGrpc.CoreServiceBlockingStub blockingStub;

  public CoreSpecService(String feastCoreHost, int feastCorePort) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(feastCoreHost, feastCorePort).usePlaintext().build();
    blockingStub = CoreServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public GetStoresResponse getStores(GetStoresRequest getStoresRequest) {
    return blockingStub.getStores(getStoresRequest);
  }

  @Override
  public GetFeatureSetsResponse getFeatureSets(GetFeatureSetsRequest getFeatureSetsRequest) {
    return blockingStub.getFeatureSets(getFeatureSetsRequest);
  }
}
