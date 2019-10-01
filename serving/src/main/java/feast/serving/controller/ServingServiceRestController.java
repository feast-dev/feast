package feast.serving.controller;

import feast.serving.FeastProperties;
import feast.serving.ServingAPIProto.GetFeastServingVersionResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.service.ServingService;
import feast.serving.util.RequestHelper;
import io.opentracing.Tracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ServingServiceRestController {

  private final ServingService servingService;
  private final String version;
  private final Tracer tracer;

  @Autowired
  public ServingServiceRestController(
      ServingService servingService, FeastProperties feastProperties, Tracer tracer) {
    this.servingService = servingService;
    this.version = feastProperties.getVersion();
    this.tracer = tracer;
  }

  @RequestMapping(
      value = "/api/v1/version",
      produces = "application/json"
  )
  public GetFeastServingVersionResponse getVersion() {
    return GetFeastServingVersionResponse.newBuilder().setVersion(version).build();
  }

  @RequestMapping(
      value = "/api/v1/features/online",
      produces = "application/json",
      consumes = "application/json")
  public GetOnlineFeaturesResponse getOnlineFeatures(@RequestBody GetFeaturesRequest request) {
    RequestHelper.validateRequest(request);
    GetOnlineFeaturesResponse onlineFeatures = servingService.getOnlineFeatures(request);
    return onlineFeatures;
  }
}
