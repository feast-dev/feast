package feast.serving.controller;

import feast.serving.FeastProperties;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeastServingVersionRequest;
import feast.serving.ServingAPIProto.GetFeastServingVersionResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.ReloadJobRequest;
import feast.serving.ServingAPIProto.ReloadJobResponse;
import feast.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.serving.service.ServingService;
import feast.serving.util.RequestHelper;
import io.grpc.stub.StreamObserver;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@GRpcService
public class ServingServiceGRpcController extends ServingServiceImplBase {
  private final ServingService servingService;
  private final String version;
  private final Tracer tracer;

  @Autowired
  public ServingServiceGRpcController(
      ServingService servingService, FeastProperties feastProperties, Tracer tracer) {
    this.servingService = servingService;
    this.version = feastProperties.getVersion();
    this.tracer = tracer;
  }

  @Override
  public void getFeastServingVersion(
      GetFeastServingVersionRequest request,
      StreamObserver<GetFeastServingVersionResponse> responseObserver) {
    responseObserver.onNext(
        GetFeastServingVersionResponse.newBuilder().setVersion(version).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getFeastServingType(
      GetFeastServingTypeRequest request,
      StreamObserver<GetFeastServingTypeResponse> responseObserver) {
    responseObserver.onNext(servingService.getFeastServingType(request));
    responseObserver.onCompleted();
  }

  @Override
  public void getOnlineFeatures(
      GetFeaturesRequest request, StreamObserver<GetOnlineFeaturesResponse> responseObserver) {
    Span span = tracer.buildSpan("getOnlineFeatures").start();
    try (Scope scope = tracer.scopeManager().activate(span, false)) {
      RequestHelper.validateRequest(request);
      GetOnlineFeaturesResponse onlineFeatures = servingService.getOnlineFeatures(request);
      responseObserver.onNext(onlineFeatures);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
    span.finish();
  }

  @Override
  public void getBatchFeatures(
      GetFeaturesRequest request, StreamObserver<GetBatchFeaturesResponse> responseObserver) {
    try {
      GetBatchFeaturesResponse batchFeatures = servingService.getBatchFeatures(request);
      responseObserver.onNext(batchFeatures);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }
  
  @Override
  public void reloadJob(
      ReloadJobRequest request, StreamObserver<ReloadJobResponse> responseObserver) {
    try {
      ReloadJobResponse reloadJobStatusResponse = servingService.reloadJob(request);
      responseObserver.onNext(reloadJobStatusResponse);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }
}
