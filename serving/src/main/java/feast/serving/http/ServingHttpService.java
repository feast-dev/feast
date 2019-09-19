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

import static feast.serving.util.RequestHelper.validateRequest;

import feast.serving.ServingAPIProto.QueryFeaturesRequest;
import feast.serving.ServingAPIProto.QueryFeaturesResponse;
import feast.serving.service.FeastServing;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.OpenTracingContextKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** HTTP endpoint for Serving API. */
@RestController
public class ServingHttpService {
  private final FeastServing feastServing;
  private final Tracer tracer;

  @Autowired
  public ServingHttpService(FeastServing feastServing, Tracer tracer) {
    this.feastServing = feastServing;
    this.tracer = tracer;
  }

  @RequestMapping(
      value = "/api/v1/features/request",
      produces = "application/json",
      consumes = "application/json")
  public QueryFeaturesResponse queryFeature(@RequestBody QueryFeaturesRequest request) {
    Span span =tracer
        .buildSpan("ServingHttpService.queryFeatures")
        .asChildOf(OpenTracingContextKey.activeSpan())
        .start();
    try (Scope scope = tracer.scopeManager().activate(span)) {
      validateRequest(request);
      return feastServing.queryFeatures(request);
    }
  }
}
