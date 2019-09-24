package feast.serving.grpc;

import com.timgroup.statsd.StatsDClient;
import feast.serving.ServingAPIProto.GetFeastServingVersionRequest;
import feast.serving.ServingAPIProto.GetFeastServingVersionResponse;
import feast.serving.ServingServiceGrpc.ServingServiceImplBase;
import io.grpc.stub.StreamObserver;
import io.opentracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;

@Slf4j
@GRpcService
public class RedisServingService extends ServingServiceImplBase {
  private static final String VERSION = "v0";
  private final Tracer tracer;
  private final StatsDClient statsDClient;

  public RedisServingService(Tracer tracer, StatsDClient statsDClient) {
    this.tracer = tracer;
    this.statsDClient = statsDClient;
  }

  @Override
  public void getFeastServingVersion(
      GetFeastServingVersionRequest request,
      StreamObserver<GetFeastServingVersionResponse> responseObserver) {
    GetFeastServingVersionResponse response =
        GetFeastServingVersionResponse.newBuilder().setVersion(VERSION).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
