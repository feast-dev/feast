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

import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.serving.config.ApplicationProperties;
import feast.serving.exception.SpecRetrievalException;
import feast.serving.service.ServingServiceV2;
import feast.serving.util.RequestHelper;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.slf4j.Logger;

public class ServingServiceGRpcController extends ServingServiceImplBase {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(ServingServiceGRpcController.class);
  private final ServingServiceV2 servingServiceV2;
  private final String version;
  private final Tracer tracer;

  public ServingServiceGRpcController(
      ServingServiceV2 servingServiceV2,
      ApplicationProperties applicationProperties,
      Tracer tracer) {
    this.servingServiceV2 = servingServiceV2;
    this.version = applicationProperties.getFeast().getVersion();
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
  public void getOnlineFeatures(
      ServingAPIProto.GetOnlineFeaturesRequest request,
      StreamObserver<ServingAPIProto.GetOnlineFeaturesResponse> responseObserver) {
    try {
      // authorize for the project in request object.
      RequestHelper.validateOnlineRequest(request);
      Span span = tracer.buildSpan("getOnlineFeaturesV2").start();
      ServingAPIProto.GetOnlineFeaturesResponse onlineFeatures =
          servingServiceV2.getOnlineFeatures(request);
      if (span != null) {
        span.finish();
      }

      responseObserver.onNext(onlineFeatures);
      responseObserver.onCompleted();
    } catch (SpecRetrievalException e) {
      log.error("Failed to retrieve specs from Registry", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
    } catch (Exception e) {
      log.warn("Failed to get Online Features", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }
}
