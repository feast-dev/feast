/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.serving.http;

//import static feast.serving.util.RequestHelper.validateRequest;

//import feast.serving.ServingAPIProto.QueryFeaturesRequest;
//import feast.serving.ServingAPIProto.QueryFeaturesResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP endpoint for Serving API.
 */
@RestController
public class ServingHttpService {
//
//  private final FeastServing feastServing;
//
//  @Autowired
//  public ServingHttpService(FeastServing feastServing) {
//    this.feastServing = feastServing;
//  }
//
//  @RequestMapping(
//      value = "/api/v1/features/request",
//      produces = "application/json",
//      consumes = "application/json")
//  public GetOnlineFeaturesResponse queryFeature(@RequestBody GetFeaturesRequest request) {
//    validateRequest(request);
//    return redisFeastServing.getOnlineFeatures(request);
//  }
}
