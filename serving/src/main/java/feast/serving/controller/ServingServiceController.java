package feast.serving.controller;

import com.timgroup.statsd.StatsDClient;
import feast.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.serving.service.ServingService;
import io.opentracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@GRpcService
public class ServingServiceController extends ServingServiceImplBase {
  private final ServingService servingService;
  private final Tracer tracer;
  private final StatsDClient statsDClient;

  @Autowired
  public ServingServiceController(
      ServingService servingService,
      Tracer tracer,
      StatsDClient statsDClient) {
    this.servingService = servingService;
    this.tracer = tracer;
    this.statsDClient = statsDClient;
  }
}
