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

import com.google.inject.Inject;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.service.ServingServiceV2;
import io.grpc.health.v1.HealthGrpc.HealthImplBase;
import io.grpc.health.v1.HealthProto.HealthCheckRequest;
import io.grpc.health.v1.HealthProto.HealthCheckResponse;
import io.grpc.health.v1.HealthProto.ServingStatus;
import io.grpc.stub.StreamObserver;

// Reference: https://github.com/grpc/grpc/blob/master/doc/health-checking.md
public class HealthServiceController extends HealthImplBase {
  private final ServingServiceV2 servingService;

  @Inject
  public HealthServiceController(final ServingServiceV2 servingService) {
    this.servingService = servingService;
  }

  @Override
  public void check(
      HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
    // TODO: Implement proper logic to determine if ServingServiceV2 is healthy e.g.
    // if it's online service check that it the service can retrieve dummy/random
    // feature table.
    // Implement similarly for batch service.

    try {
      servingService.getFeastServingInfo(GetFeastServingInfoRequest.getDefaultInstance());
      responseObserver.onNext(
          HealthCheckResponse.newBuilder().setStatus(ServingStatus.SERVING).build());
    } catch (Exception e) {
      responseObserver.onNext(
          HealthCheckResponse.newBuilder().setStatus(ServingStatus.NOT_SERVING).build());
    }
    responseObserver.onCompleted();
  }
}
