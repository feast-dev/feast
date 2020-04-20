/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.serving.controller;

import static feast.serving.util.mappers.ResponseJSONMapper.mapGetOnlineFeaturesResponse;

import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.config.FeastProperties;
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
