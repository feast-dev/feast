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

package feast.serving.grpc;

import static feast.serving.util.RequestHelper.validateRequest;
import static feast.serving.util.StatsUtil.makeStatsdTags;
import static io.grpc.Status.Code.INTERNAL;

import com.timgroup.statsd.StatsDClient;
import feast.serving.ServingAPIGrpc.ServingAPIImplBase;
import feast.serving.ServingAPIProto.QueryFeaturesRequest;
import feast.serving.ServingAPIProto.QueryFeaturesResponse;
import feast.serving.service.FeastServing;
import feast.serving.util.TimeUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.OpenTracingContextKey;
import io.opentracing.contrib.grpc.ServerTracingInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

/** Grpc service implementation for Serving API. */
@Slf4j
@GRpcService(interceptors = {ServerTracingInterceptor.class})
public class ServingGrpcService extends ServingAPIImplBase {

  private final FeastServing feast;
  private final Tracer tracer;
  private final StatsDClient statsDClient;

  @Autowired
  public ServingGrpcService(FeastServing feast, Tracer tracer, StatsDClient statsDClient) {
    this.feast = feast;
    this.tracer = tracer;
    this.statsDClient = statsDClient;
  }

  /** Query feature values from Feast storage. */
  @Override
  public void queryFeatures(QueryFeaturesRequest request, StreamObserver<QueryFeaturesResponse> responseObserver) {
    long currentMicro = TimeUtil.microTime();
    Span span =tracer
        .buildSpan("ServingGrpcService.queryFeatures")
        .asChildOf(OpenTracingContextKey.activeSpan())
        .start();
    String[] tags = makeStatsdTags(request);
    statsDClient.increment("query_features_count", tags);
    statsDClient.gauge("query_features_entity_count", request.getEntityIdCount(), tags);
    statsDClient.gauge("query_features_feature_count", request.getFeatureIdCount(), tags);
    try (Scope scope = tracer.scopeManager().activate(span)) {
      validateRequest(request);
      QueryFeaturesResponse response = feast.queryFeatures(request);

      responseObserver.onNext(response);
      responseObserver.onCompleted();
      statsDClient.increment("query_feature_success", tags);
    } catch (Exception e) {
      statsDClient.increment("query_feature_failed", tags);
      log.error("Error: {}", e.getMessage());
      responseObserver.onError(
          new StatusRuntimeException(
              Status.fromCode(INTERNAL).withDescription(e.getMessage()).withCause(e)));
      span.log("error");
    } finally {
      statsDClient.gauge("query_features_latency_us", TimeUtil.microTime() - currentMicro, tags);
      span.finish();
    }
  }
}
