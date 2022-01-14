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
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;

public class OnlineServingGrpcServiceV2 extends ServingServiceGrpc.ServingServiceImplBase {
  private final ServingServiceV2 servingServiceV2;

  @Inject
  OnlineServingGrpcServiceV2(ServingServiceV2 servingServiceV2) {
    this.servingServiceV2 = servingServiceV2;
  }

  @Override
  public void getFeastServingInfo(
      ServingAPIProto.GetFeastServingInfoRequest request,
      StreamObserver<ServingAPIProto.GetFeastServingInfoResponse> responseObserver) {
    responseObserver.onNext(this.servingServiceV2.getFeastServingInfo(request));
    responseObserver.onCompleted();
  }

  @Override
  public void getOnlineFeatures(
      ServingAPIProto.GetOnlineFeaturesRequest request,
      StreamObserver<ServingAPIProto.GetOnlineFeaturesResponse> responseObserver) {
    responseObserver.onNext(this.servingServiceV2.getOnlineFeatures(request));
    responseObserver.onCompleted();
  }
}
