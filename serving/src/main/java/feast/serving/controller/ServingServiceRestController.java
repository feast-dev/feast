package feast.serving.controller;

import static feast.serving.util.mappers.ResponseJSONMapper.mapGetOnlineFeaturesResponse;

import feast.serving.FeastProperties;
import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.service.ServingService;
import feast.serving.util.RequestHelper;
import io.opentracing.Tracer;
import java.util.List;
import java.util.Map;
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

  @RequestMapping(value = "/api/v1/info", produces = "application/json")
  public GetFeastServingInfoResponse getInfo() {
    GetFeastServingInfoResponse feastServingInfo =
        servingService.getFeastServingInfo(GetFeastServingInfoRequest.getDefaultInstance());
    return feastServingInfo.toBuilder().setVersion(version).build();
  }

  @RequestMapping(
      value = "/api/v1/features/online",
      produces = "application/json",
      consumes = "application/json")
  public List<Map<String, Object>> getOnlineFeatures(
      @RequestBody GetOnlineFeaturesRequest request) {
    RequestHelper.validateOnlineRequest(request);
    GetOnlineFeaturesResponse onlineFeatures = servingService.getOnlineFeatures(request);
    return mapGetOnlineFeaturesResponse(onlineFeatures);
  }
}
