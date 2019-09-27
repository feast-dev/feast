package feast.serving.controller;

import feast.core.CoreServiceProto.GetStoresRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.service.ServingService;
import feast.serving.service.SpecService;
import io.grpc.health.v1.HealthGrpc.HealthImplBase;
import io.grpc.health.v1.HealthProto.HealthCheckRequest;
import io.grpc.health.v1.HealthProto.HealthCheckResponse;
import io.grpc.health.v1.HealthProto.HealthCheckResponse.ServingStatus;
import io.grpc.stub.StreamObserver;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;

// Reference: https://github.com/grpc/grpc/blob/master/doc/health-checking.md

@GRpcService
public class HealthServiceController extends HealthImplBase {
  private SpecService specService;
  private ServingService servingService;

  @Autowired
  public HealthServiceController(SpecService specService, ServingService servingService) {
    this.specService = specService;
    this.servingService = servingService;
  }

  @Override
  public void check(
      HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
    // TODO: Implement proper logic to determine if ServingService is healthy e.g.
    //       if it's online service check that it the service can retrieve dummy/random feature set.
    //       Implement similary for batch service.

    try {
      specService.getStores(GetStoresRequest.getDefaultInstance());
      servingService.getFeastServingType(GetFeastServingTypeRequest.getDefaultInstance());
      responseObserver.onNext(
          HealthCheckResponse.newBuilder().setStatus(ServingStatus.SERVING).build());
    } catch (Exception e) {
      responseObserver.onNext(
          HealthCheckResponse.newBuilder().setStatus(ServingStatus.NOT_SERVING).build());
    }
    responseObserver.onCompleted();
  }
}
