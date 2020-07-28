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

import feast.auth.service.AuthorizationService;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.ServingAPIProto.GetBatchFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.ServingAPIProto.GetJobRequest;
import feast.proto.serving.ServingAPIProto.GetJobResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.serving.config.FeastProperties;
import feast.serving.exception.SpecRetrievalException;
import feast.serving.interceptors.GrpcMonitoringInterceptor;
import feast.serving.service.ServingService;
import feast.serving.util.RequestHelper;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;

@GrpcService(interceptors = {GrpcMonitoringInterceptor.class})
public class ServingServiceGRpcController extends ServingServiceImplBase {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(ServingServiceGRpcController.class);
  private final ServingService servingService;
  private final String version;
  private final Tracer tracer;
  private final AuthorizationService authorizationService;

  @Autowired
  public ServingServiceGRpcController(
      AuthorizationService authorizationService,
      ServingService servingService,
      FeastProperties feastProperties,
      Tracer tracer) {
    this.authorizationService = authorizationService;
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
      // authorize for the project in request object.
      if (request.getProject() != null && !request.getProject().isEmpty()) {
        // project set at root level overrides the project set at feature set level
        this.authorizationService.authorizeRequest(
            SecurityContextHolder.getContext(), request.getProject());
      } else {
        // authorize for projects set in feature list, backward compatibility for
        // <=v0.5.X
        this.checkProjectAccess(request.getFeaturesList());
      }
      RequestHelper.validateOnlineRequest(request);
      GetOnlineFeaturesResponse onlineFeatures = servingService.getOnlineFeatures(request);
      responseObserver.onNext(onlineFeatures);
      responseObserver.onCompleted();
    } catch (SpecRetrievalException e) {
      log.error("Failed to retrieve specs in SpecService", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
    } catch (AccessDeniedException e) {
      log.info(String.format("User prevented from accessing one of the projects in request"));
      responseObserver.onError(
          Status.PERMISSION_DENIED
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
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
      this.checkProjectAccess(request.getFeaturesList());
      GetBatchFeaturesResponse batchFeatures = servingService.getBatchFeatures(request);
      responseObserver.onNext(batchFeatures);
      responseObserver.onCompleted();
    } catch (SpecRetrievalException e) {
      log.error("Failed to retrieve specs in SpecService", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
    } catch (AccessDeniedException e) {
      log.info(String.format("User prevented from accessing one of the projects in request"));
      responseObserver.onError(
          Status.PERMISSION_DENIED
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
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

  private void checkProjectAccess(List<FeatureReference> featureList) {
    Set<String> projectList =
        featureList.stream().map(FeatureReference::getProject).collect(Collectors.toSet());
    if (projectList.isEmpty()) {
      authorizationService.authorizeRequest(SecurityContextHolder.getContext(), "default");
    } else {
      projectList.stream()
          .forEach(
              project -> {
                this.authorizationService.authorizeRequest(
                    SecurityContextHolder.getContext(), project);
              });
    }
  }
}
