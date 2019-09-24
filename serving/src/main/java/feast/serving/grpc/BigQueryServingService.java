package feast.serving.grpc;

import com.timgroup.statsd.StatsDClient;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeastServingVersionRequest;
import feast.serving.ServingAPIProto.GetFeastServingVersionResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.Job;
import feast.serving.ServingServiceGrpc.ServingServiceImplBase;
import io.grpc.stub.StreamObserver;
import io.opentracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;

@Slf4j
@GRpcService
public class BigQueryServingService extends ServingServiceImplBase {
  private static final String VERSION = "v0";

  @Override
  public void getFeastServingVersion(
      GetFeastServingVersionRequest request,
      StreamObserver<GetFeastServingVersionResponse> responseObserver) {
    GetFeastServingVersionResponse response =
        GetFeastServingVersionResponse.newBuilder().setVersion(VERSION).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getFeastServingType(GetFeastServingTypeRequest request,
      StreamObserver<GetFeastServingTypeResponse> responseObserver) {
    GetFeastServingTypeResponse response = GetFeastServingTypeResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_BATCH).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getBatchFeatures(GetFeaturesRequest request,
      StreamObserver<GetBatchFeaturesResponse> responseObserver) {
    GetBatchFeaturesResponse response = GetBatchFeaturesResponse.newBuilder()
        .setJob(Job.newBuilder().setId(request.getFeatureSets(0).getName())).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
