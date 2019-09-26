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
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeastServingVersionRequest;
import feast.serving.ServingAPIProto.GetFeastServingVersionResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetStagingLocationRequest;
import feast.serving.ServingAPIProto.GetStagingLocationResponse;
import feast.serving.ServingAPIProto.LoadBatchFeaturesRequest;
import feast.serving.ServingAPIProto.LoadBatchFeaturesResponse;
import feast.serving.ServingAPIProto.ReloadJobStatusRequest;
import feast.serving.ServingAPIProto.ReloadJobStatusResponse;
import feast.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.serving.service.serving.ServingService;
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

/** Grpc service implementation for Serving API. */
@Slf4j
@GRpcService
public class GrpcServingService extends ServingServiceImplBase {

  private final ServingService servingService;
  private final Tracer tracer;
  private final StatsDClient statsDClient;

  @Autowired
  public GrpcServingService(ServingService servingService, Tracer tracer, StatsDClient statsDClient) {
    this.servingService = servingService;
    this.tracer = tracer;
    this.statsDClient = statsDClient;
  }

  @Override
  public void getFeastServingVersion(
      GetFeastServingVersionRequest request,
      StreamObserver<GetFeastServingVersionResponse> responseObserver) {
    String version = this.getClass().getPackage().getImplementationVersion();
    responseObserver.onNext(
        GetFeastServingVersionResponse.newBuilder().setVersion(version).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getFeastServingType(
      GetFeastServingTypeRequest request,
      StreamObserver<GetFeastServingTypeResponse> responseObserver) {
    super.getFeastServingType(request, responseObserver);
  }

  @Override
  public void getOnlineFeatures(
      GetFeaturesRequest request, StreamObserver<GetOnlineFeaturesResponse> responseObserver) {
    Span span = tracer.buildSpan("ServingGrpcService-getOnlineFeatures").start();

    try (Scope scope = tracer.scopeManager().activate(span, false)) {
      Span innerSpan = scope.span();
      RequestHelper.validateRequest(request);
      GetOnlineFeaturesResponse response = servingService.getOnlineFeatures(request);
      innerSpan.log("calling onNext");
      responseObserver.onNext(response);
      innerSpan.log("calling onCompleted");
      responseObserver.onCompleted();
      innerSpan.log("all done");
    } catch (Exception e) {
      log.error("Error: {}", e.getMessage());
      responseObserver.onError(
          new StatusRuntimeException(
              Status.fromCode(Code.INTERNAL).withDescription(e.getMessage()).withCause(e)));
    }
  }

  @Override
  public void getBatchFeatures(
      GetFeaturesRequest request, StreamObserver<GetBatchFeaturesResponse> responseObserver) {
    super.getBatchFeatures(request, responseObserver);
  }

  @Override
  public void getBatchFeaturesFromCompletedJob(GetBatchFeaturesFromCompletedJobRequest request,
      StreamObserver<GetBatchFeaturesFromCompletedJobResponse> responseObserver) {
    super.getBatchFeaturesFromCompletedJob(request, responseObserver);
  }

  @Override
  public void getStagingLocation(GetStagingLocationRequest request,
      StreamObserver<GetStagingLocationResponse> responseObserver) {
    super.getStagingLocation(request, responseObserver);
  }

  @Override
  public void loadBatchFeatures(LoadBatchFeaturesRequest request,
      StreamObserver<LoadBatchFeaturesResponse> responseObserver) {
    super.loadBatchFeatures(request, responseObserver);
  }

  @Override
  public void reloadJobStatus(ReloadJobStatusRequest request,
      StreamObserver<ReloadJobStatusResponse> responseObserver) {
    super.reloadJobStatus(request, responseObserver);
  }

}
