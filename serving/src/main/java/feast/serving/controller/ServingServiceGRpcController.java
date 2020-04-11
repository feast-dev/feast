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

import feast.serving.ServingAPIProto.GetBatchFeaturesRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetJobRequest;
import feast.serving.ServingAPIProto.GetJobResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.serving.config.FeastProperties;
import feast.serving.interceptors.GrpcMonitoringInterceptor;
import feast.serving.service.ServingService;
import feast.serving.util.RequestHelper;
import io.grpc.stub.StreamObserver;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.lognet.springboot.grpc.GRpcService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

@GRpcService(interceptors = {GrpcMonitoringInterceptor.class})
public class ServingServiceGRpcController extends ServingServiceImplBase {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(ServingServiceGRpcController.class);
  private final ServingService servingService;
  private final String version;
  private final Tracer tracer;

  @Autowired
  public ServingServiceGRpcController(
      ServingService servingService, FeastProperties feastProperties, Tracer tracer) {
    this.servingService = servingService;
    this.version = feastProperties.getVersion();
    this.tracer = tracer;
  }

  @Override
  public void getFeastServingInfo(
      GetFeastServingInfoRequest request,
      StreamObserver<GetFeastServingInfoResponse> responseObserver) {
    GetFeastServingInfoResponse feastServingInfo = servingService.getFeastServingInfo(request);
    feastServingInfo = feastServingInfo.toBuilder().setVersion(version).build();
    responseObserver.onNext(feastServingInfo);
    responseObserver.onCompleted();
  }

  @Override
  public void getOnlineFeatures(
      GetOnlineFeaturesRequest request,
      StreamObserver<GetOnlineFeaturesResponse> responseObserver) {
    Span span = tracer.buildSpan("getOnlineFeatures").start();
    try (Scope scope = tracer.scopeManager().activate(span, false)) {
      RequestHelper.validateOnlineRequest(request);
      GetOnlineFeaturesResponse onlineFeatures = servingService.getOnlineFeatures(request);
      responseObserver.onNext(onlineFeatures);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn("Failed to get Online Features", e);
      responseObserver.onError(e);
    }
    span.finish();
  }

  @Override
  public void getBatchFeatures(
      GetBatchFeaturesRequest request, StreamObserver<GetBatchFeaturesResponse> responseObserver) {
    try {
      RequestHelper.validateBatchRequest(request);
      GetBatchFeaturesResponse batchFeatures = servingService.getBatchFeatures(request);
      responseObserver.onNext(batchFeatures);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn("Failed to get Batch Features", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getJob(GetJobRequest request, StreamObserver<GetJobResponse> responseObserver) {
    try {
      GetJobResponse response = servingService.getJob(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn("Failed to get Job", e);
      responseObserver.onError(e);
    }
  }
}
