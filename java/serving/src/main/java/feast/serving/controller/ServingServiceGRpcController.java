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

import feast.common.auth.service.AuthorizationService;
import feast.common.logging.interceptors.GrpcMessageInterceptor;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.serving.config.FeastProperties;
import feast.serving.exception.SpecRetrievalException;
import feast.serving.interceptors.GrpcMonitoringContext;
import feast.serving.interceptors.GrpcMonitoringInterceptor;
import feast.serving.service.ServingServiceV2;
import feast.serving.util.RequestHelper;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.TracingServerInterceptor;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;

@GrpcService(
    interceptors = {
      TracingServerInterceptor.class,
      GrpcMessageInterceptor.class,
      GrpcMonitoringInterceptor.class
    })
public class ServingServiceGRpcController extends ServingServiceImplBase {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(ServingServiceGRpcController.class);
  private final ServingServiceV2 servingServiceV2;
  private final String version;
  private final Tracer tracer;
  private final AuthorizationService authorizationService;

  @Autowired
  public ServingServiceGRpcController(
      AuthorizationService authorizationService,
      ServingServiceV2 servingServiceV2,
      FeastProperties feastProperties,
      Tracer tracer) {
    this.authorizationService = authorizationService;
    this.servingServiceV2 = servingServiceV2;
    this.version = feastProperties.getVersion();
    this.tracer = tracer;
  }

  @Override
  public void getFeastServingInfo(
      GetFeastServingInfoRequest request,
      StreamObserver<GetFeastServingInfoResponse> responseObserver) {
    GetFeastServingInfoResponse feastServingInfo = servingServiceV2.getFeastServingInfo(request);
    feastServingInfo = feastServingInfo.toBuilder().setVersion(version).build();
    responseObserver.onNext(feastServingInfo);
    responseObserver.onCompleted();
  }

  @Override
  public void getOnlineFeaturesV2(
      ServingAPIProto.GetOnlineFeaturesRequestV2 request,
      StreamObserver<GetOnlineFeaturesResponse> responseObserver) {
    try {
      // authorize for the project in request object.
      if (request.getProject() != null && !request.getProject().isEmpty()) {
        // project set at root level overrides the project set at feature table level
        this.authorizationService.authorizeRequest(
            SecurityContextHolder.getContext(), request.getProject());

        // update monitoring context
        GrpcMonitoringContext.getInstance().setProject(request.getProject());
      }
      RequestHelper.validateOnlineRequest(request);
      Span span = tracer.buildSpan("getOnlineFeaturesV2").start();
      GetOnlineFeaturesResponse onlineFeatures = servingServiceV2.getOnlineFeatures(request);
      if (span != null) {
        span.finish();
      }
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
  }
}
