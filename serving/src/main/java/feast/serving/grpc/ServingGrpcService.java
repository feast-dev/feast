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

import com.timgroup.statsd.StatsDClient;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetStatusRequest;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetStatusResponse;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetUploadUrlRequest;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetUploadUrlResponse;
import feast.serving.ServingAPIProto.BatchFeaturesJob.SetUploadCompleteRequest;
import feast.serving.ServingAPIProto.BatchFeaturesJob.SetUploadCompleteResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeastServingVersionRequest;
import feast.serving.ServingAPIProto.GetFeastServingVersionResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingServiceGrpc;
import feast.serving.service.FeastServing;
import feast.serving.util.RequestHelper;
import io.grpc.Status;
import io.grpc.Status.Code;
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
public class ServingGrpcService extends ServingServiceGrpc.ServingServiceImplBase {

  private final FeastServing feastServing;
  private final Tracer tracer;
  private final StatsDClient statsDClient;

  @Autowired
  public ServingGrpcService(FeastServing feastServing, Tracer tracer, StatsDClient statsDClient) {
    this.feastServing = feastServing;
    this.tracer = tracer;
    this.statsDClient = statsDClient;
  }

  @Override
  public void getFeastServingVersion(GetFeastServingVersionRequest request,
      StreamObserver<GetFeastServingVersionResponse> responseObserver) {
//    long currentMicro = TimeUtil.microTime();
    Span span = tracer.buildSpan("ServingGrpcService-getServingVersion").start();

    try (Scope scope = tracer.scopeManager().activate(span, false)) {
      Span innerSpan = scope.span();
      GetFeastServingVersionResponse response = feastServing.getFeastServingVersion();
      innerSpan.log("calling onNext");
      responseObserver.onNext(response);
      innerSpan.log("calling onCompleted");
      responseObserver.onCompleted();
      innerSpan.log("all done");
    } catch (Exception e) {
      log.error("Error: {}", e.getMessage());
      responseObserver.onError(new StatusRuntimeException(
          Status.fromCode(Code.INTERNAL).withDescription(e.getMessage()).withCause(e)));
    } finally {
      span.finish();
    }
  }

  @Override
  public void getFeastServingType(GetFeastServingTypeRequest request,
      StreamObserver<GetFeastServingTypeResponse> responseObserver) {
    super.getFeastServingType(request, responseObserver);
  }

  @Override
  public void getOnlineFeatures(GetFeaturesRequest request,
      StreamObserver<GetOnlineFeaturesResponse> responseObserver) {
    Span span = tracer.buildSpan("ServingGrpcService-getOnlineFeatures").start();

    try (Scope scope = tracer.scopeManager().activate(span, false)) {
      Span innerSpan = scope.span();
      RequestHelper.validateRequest(request);
      GetOnlineFeaturesResponse response = feastServing.getOnlineFeatures(request);
      innerSpan.log("calling onNext");
      responseObserver.onNext(response);
      innerSpan.log("calling onCompleted");
      responseObserver.onCompleted();
      innerSpan.log("all done");
    } catch (Exception e) {
      log.error("Error: {}", e.getMessage());
      responseObserver.onError(new StatusRuntimeException(
          Status.fromCode(Code.INTERNAL).withDescription(e.getMessage()).withCause(e)));
    }
  }

  @Override
  public void getBatchFeatures(GetFeaturesRequest request,
      StreamObserver<GetBatchFeaturesResponse> responseObserver) {
    super.getBatchFeatures(request, responseObserver);
  }

  @Override
  public void getBatchFeaturesJobStatus(GetStatusRequest request,
      StreamObserver<GetStatusResponse> responseObserver) {
    super.getBatchFeaturesJobStatus(request, responseObserver);
  }

  @Override
  public void getBatchFeaturesJobUploadUrl(GetUploadUrlRequest request,
      StreamObserver<GetUploadUrlResponse> responseObserver) {
    super.getBatchFeaturesJobUploadUrl(request, responseObserver);
  }

  @Override
  public void setBatchFeaturesJobUploadComplete(SetUploadCompleteRequest request,
      StreamObserver<SetUploadCompleteResponse> responseObserver) {
    super.setBatchFeaturesJobUploadComplete(request, responseObserver);
  }

  //  /** Query feature values from Feast storage. */
//  @Override
//  public void queryFeatures(QueryFeaturesRequest request, StreamObserver<QueryFeaturesResponse> responseObserver) {
//    long currentMicro = TimeUtil.microTime();
//    Span span = tracer.buildSpan("ServingGrpcService-queryFeatures").start();
//    String[] tags = makeStatsdTags(request);
//    statsDClient.increment("query_features_count", tags);
//    statsDClient.gauge("query_features_entity_count", request.getEntityIdCount(), tags);
//    statsDClient.gauge("query_features_feature_count", request.getFeatureIdCount(), tags);
//    try (Scope scope = tracer.scopeManager().activate(span, false)) {
//      Span innerSpan = scope.span();
//      validateRequest(request);
//      QueryFeaturesResponse response = feast.queryFeatures(request);
//
//      innerSpan.log("calling onNext");
//      responseObserver.onNext(response);
//      innerSpan.log("calling onCompleted");
//      responseObserver.onCompleted();
//      innerSpan.log("all done");
//      statsDClient.increment("query_feature_success", tags);
//    } catch (Exception e) {
//      statsDClient.increment("query_feature_failed", tags);
//      log.error("Error: {}", e.getMessage());
//      responseObserver.onError(
//          new StatusRuntimeException(
//              Status.fromCode(INTERNAL).withDescription(e.getMessage()).withCause(e)));
//    } finally {
//      statsDClient.gauge("query_features_latency_us", TimeUtil.microTime() - currentMicro, tags);
//      span.finish();
//    }
//  }
}
