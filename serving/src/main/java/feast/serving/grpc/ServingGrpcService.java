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

import static feast.serving.util.RequestHelper.checkTimestampRange;
import static feast.serving.util.RequestHelper.validateRequest;
import static feast.serving.util.StatsUtil.makeStatsdTags;
import static io.grpc.Status.Code.INTERNAL;
import static io.grpc.Status.Code.NOT_FOUND;

import com.timgroup.statsd.StatsDClient;
import feast.serving.ServingAPIGrpc.ServingAPIImplBase;
import feast.serving.ServingAPIProto.QueryFeatures.Request;
import feast.serving.ServingAPIProto.QueryFeatures.Response;
import feast.serving.service.FeastServing;
import feast.serving.util.TimeUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Grpc service implementation for Serving API.
 */
@Slf4j
@GRpcService
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

  /**
   * Query feature values from Feast storage.
   */
  @Override
  public void queryFeatures(Request request, StreamObserver<Response> responseObserver) {
    long currentMicro = TimeUtil.microTime();
    Span span =
        tracer
            .buildSpan("ServingGrpcService-queryFeatures")
            .start();
    String[] tags = makeStatsdTags(request);
    statsDClient.increment("query_features_count", tags);
    statsDClient.gauge("query_features_entity_count", request.getEntityIdCount(), tags);
    statsDClient.gauge("query_features_feature_count", request.getRequestDetailsCount(), tags);
    try (Scope scope = tracer.scopeManager().activate(span, false)) {
      Span innerSpan = scope.span();
      validateRequest(request);
      Request validRequest = checkTimestampRange(request);
      Response response = feast.queryFeatures(validRequest);

      if (response.getEntitiesCount() < 1) {
        responseObserver.onError(
            new StatusRuntimeException(
                Status.fromCode(NOT_FOUND)
                    .withDescription("No value is found for the requested feature")));
        return;
      }

      innerSpan.log("calling onNext");
      responseObserver.onNext(response);
      innerSpan.log("calling onCompleted");
      responseObserver.onCompleted();
      innerSpan.log("all done");
      statsDClient.increment("query_feature_success", tags);
    } catch (Exception e) {
      statsDClient.increment("query_feature_failed", tags);
      log.error("Error: {}", e.getMessage());
      responseObserver.onError(
          new StatusRuntimeException(
              Status.fromCode(INTERNAL).withDescription(e.getMessage()).withCause(e)));
    } finally {
      statsDClient
          .gauge("query_features_latency_us", TimeUtil.microTime() - currentMicro, tags);
      span.finish();
    }
  }
}
