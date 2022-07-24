/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.serving.grpc;

import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingServiceGrpc;
import feast.serving.service.ServingServiceV2;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import org.slf4j.Logger;

public class OnlineServingGrpcServiceV2 extends ServingServiceGrpc.ServingServiceImplBase {
  private final ServingServiceV2 servingServiceV2;
  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(OnlineServingGrpcServiceV2.class);

  @Inject
  OnlineServingGrpcServiceV2(ServingServiceV2 servingServiceV2) {
    this.servingServiceV2 = servingServiceV2;
  }

  @Override
  public void getFeastServingInfo(
      ServingAPIProto.GetFeastServingInfoRequest request,
      StreamObserver<ServingAPIProto.GetFeastServingInfoResponse> responseObserver) {
    try {
      responseObserver.onNext(this.servingServiceV2.getFeastServingInfo(request));
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.warn("Failed to get Serving Info", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void getOnlineFeatures(
      ServingAPIProto.GetOnlineFeaturesRequest request,
      StreamObserver<ServingAPIProto.GetOnlineFeaturesResponse> responseObserver) {
    try {
      responseObserver.onNext(this.servingServiceV2.getOnlineFeatures(request));
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.warn("Failed to get Online Features", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }
}
